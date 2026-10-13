"""
Consistent Online Backup, Verification, and Disaster Recovery for RnR Linux IoT Hub.
File: storage/backup_manager.py

Implements Customization 11 specification:
- Consistent online SQLite backup using the official SQLite Online Backup API.
- Never copies a live database file without coordination (prevents WAL split-brain/corruption).
- Generates SHA-256 integrity checksums and metadata manifests.
- Safe restore procedure with pre-flight integrity verification.
- Supports CLI commands:
    iothubctl backup create
    iothubctl backup verify
    iothubctl backup restore
"""

from __future__ import annotations
import hashlib
import json
import logging
import os
import shutil
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from storage.database import DatabaseManager

logger = logging.getLogger("iothub.backup")

DEFAULT_BACKUP_DIR = "/var/lib/iothub/backups"


class BackupManager:
    """Manages consistent SQLite online backups, integrity verification, and safe restoration."""

    def __init__(self, db: DatabaseManager, backup_dir: Optional[str] = None):
        self.db = db
        self.backup_dir = Path(backup_dir or os.environ.get("IOTHUB_BACKUP_DIR") or DEFAULT_BACKUP_DIR)

    def _ensure_backup_dir(self) -> Path:
        """Ensures backup destination directory exists with restricted permissions."""
        try:
            self.backup_dir.mkdir(parents=True, exist_ok=True)
            try:
                self.backup_dir.chmod(0o700)
            except OSError:
                pass
            return self.backup_dir
        except OSError:
            # Fallback to local project backups directory
            local_backup = Path(__file__).resolve().parent.parent / "backups"
            local_backup.mkdir(parents=True, exist_ok=True)
            return local_backup

    async def create_backup(self, custom_dest_dir: Optional[str] = None) -> Dict[str, Any]:
        """
        Creates a consistent online snapshot of the live SQLite database.
        Returns:
            Dictionary containing backup metadata, filepath, and SHA-256 digest.
        """
        dest_dir = Path(custom_dest_dir) if custom_dest_dir else self._ensure_backup_dir()
        dest_dir.mkdir(parents=True, exist_ok=True)

        now_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        backup_filename = f"iothub_backup_{now_str}.db"
        backup_path = dest_dir / backup_filename
        manifest_path = dest_dir / f"iothub_backup_{now_str}.manifest.json"

        # 1. Execute online backup using sqlite3.Connection.backup API
        source_db_path = self.db.db_path
        if not os.path.exists(source_db_path):
            raise FileNotFoundError(f"Source database '{source_db_path}' does not exist.")

        # Source connection in read-only mode to prevent any locking conflicts
        src_conn = sqlite3.connect(f"file:{source_db_path}?mode=ro", uri=True)
        dest_conn = sqlite3.connect(str(backup_path))

        try:
            src_conn.backup(dest_conn)
        finally:
            dest_conn.close()
            src_conn.close()

        # 2. Compute SHA-256 checksum
        hasher = hashlib.sha256()
        with open(backup_path, "rb") as f:
            while chunk := f.read(65536):
                hasher.update(chunk)
        digest = hasher.hexdigest()

        # 3. Read table statistics for manifest
        verify_conn = sqlite3.connect(str(backup_path))
        table_counts = {}
        try:
            tables = ["schedules", "scheduled_actions", "config_settings", "device_bindings", "device_profiles", "executions", "event_outbox"]
            for tbl in tables:
                try:
                    cur = verify_conn.execute(f"SELECT COUNT(*) FROM {tbl}")
                    table_counts[tbl] = cur.fetchone()[0]
                except sqlite3.OperationalError:
                    table_counts[tbl] = 0
        finally:
            verify_conn.close()

        metadata = {
            "backup_file": str(backup_path),
            "manifest_file": str(manifest_path),
            "created_at": datetime.now(timezone.utc).isoformat(),
            "sha256": digest,
            "size_bytes": backup_path.stat().st_size,
            "source_db": str(source_db_path),
            "table_counts": table_counts,
        }

        # Save manifest
        manifest_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
        logger.info("Created consistent online backup at %s (SHA-256: %s)", backup_path, digest[:12])

        return metadata

    @staticmethod
    def verify_backup(backup_path_str: str) -> Tuple[bool, Dict[str, Any]]:
        """
        Verifies backup database integrity via PRAGMA integrity_check and checksum manifest.
        Returns:
            Tuple[is_valid: bool, verification_details: Dict[str, Any]]
        """
        backup_path = Path(backup_path_str)
        if not backup_path.exists():
            return False, {"error": f"Backup file '{backup_path}' not found."}

        # Check PRAGMA integrity_check
        try:
            conn = sqlite3.connect(str(backup_path))
            cur = conn.execute("PRAGMA integrity_check;")
            row = cur.fetchone()
            conn.close()
            if not row or row[0] != "ok":
                return False, {"error": f"Integrity check failed: {row[0] if row else 'empty'}"}
        except Exception as exc:
            return False, {"error": f"Database corruption detected: {exc}"}

        # Check SHA-256 match if manifest exists
        manifest_path = backup_path.parent / (backup_path.name.replace(".db", ".manifest.json"))
        manifest_verified = False
        manifest_data = {}
        if manifest_path.exists():
            try:
                manifest_data = json.loads(manifest_path.read_text(encoding="utf-8"))
                hasher = hashlib.sha256()
                with open(backup_path, "rb") as f:
                    while chunk := f.read(65536):
                        hasher.update(chunk)
                current_digest = hasher.hexdigest()
                if current_digest == manifest_data.get("sha256"):
                    manifest_verified = True
                else:
                    return False, {"error": "SHA-256 digest does not match manifest!"}
            except Exception as e:
                manifest_data["manifest_error"] = str(e)

        return True, {
            "status": "VALID",
            "integrity_check": "ok",
            "manifest_verified": manifest_verified,
            "size_bytes": backup_path.stat().st_size,
            "manifest": manifest_data,
        }

    async def restore_backup(self, backup_path_str: str, target_db_str: Optional[str] = None) -> bool:
        """
        Safely restores backup file into the target database location.
        Creates a temporary safety rollback copy of the current target before overwriting.
        """
        is_valid, details = self.verify_backup(backup_path_str)
        if not is_valid:
            raise ValueError(f"Cannot restore invalid backup: {details.get('error')}")

        target_path = Path(target_db_str or self.db.db_path)
        backup_path = Path(backup_path_str)

        # 1. Create safety copy of existing database if present
        safety_path = target_path.with_suffix(".pre_restore.bak")
        if target_path.exists():
            shutil.copy2(target_path, safety_path)

        try:
            # 2. Perform online restore from backup file to target
            src_conn = sqlite3.connect(f"file:{backup_path}?mode=ro", uri=True)
            dst_conn = sqlite3.connect(str(target_path))
            try:
                src_conn.backup(dst_conn)
            finally:
                dst_conn.close()
                src_conn.close()

            # Clean any stale WAL/SHM files
            for ext in ("-wal", "-shm"):
                w = Path(str(target_path) + ext)
                if w.exists():
                    try:
                        w.unlink()
                    except OSError:
                        pass

            logger.info("Successfully restored database from backup '%s'", backup_path)
            return True
        except Exception as exc:
            # Rollback to safety copy
            if safety_path.exists():
                shutil.copy2(safety_path, target_path)
            raise RuntimeError(f"Restore failed; rolled back to pre-restore state: {exc}")

    def list_backups(self) -> List[Dict[str, Any]]:
        """Lists available backup files and their manifests."""
        dir_to_search = self.backup_dir if self.backup_dir.exists() else Path(__file__).resolve().parent.parent / "backups"
        if not dir_to_search.exists():
            return []

        backups = []
        for p in sorted(dir_to_search.glob("*.db"), reverse=True):
            manifest_file = p.parent / (p.name.replace(".db", ".manifest.json"))
            manifest = {}
            if manifest_file.exists():
                try:
                    manifest = json.loads(manifest_file.read_text(encoding="utf-8"))
                except Exception:
                    pass

            backups.append({
                "path": str(p),
                "filename": p.name,
                "size_bytes": p.stat().st_size,
                "modified_at": datetime.fromtimestamp(p.stat().st_mtime, timezone.utc).isoformat(),
                "sha256": manifest.get("sha256", ""),
                "table_counts": manifest.get("table_counts", {}),
            })
        return backups
