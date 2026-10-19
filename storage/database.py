"""
SQLite Persistent Storage Management for RnR Linux IoT Hub.
File: storage/database.py

Configures SQLite connection with WAL (Write-Ahead Logging) mode,
foreign key enforcement, busy timeout, and transactional writes.
"""

from __future__ import annotations
import asyncio
from contextlib import asynccontextmanager
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, AsyncGenerator, Dict, List, Optional
import aiosqlite

from config import DATABASE_PATH

logger = logging.getLogger("iothub.storage")

DEFAULT_DB_PATH = DATABASE_PATH


class DatabaseManager:
    """Manages SQLite connection lifecycle, WAL mode, schema migrations, and transactions."""

    def __init__(self, db_path: str = DEFAULT_DB_PATH):
        self.db_path = db_path
        self._conn: Optional[aiosqlite.Connection] = None

    @property
    def connection(self) -> aiosqlite.Connection:
        if self._conn is None:
            raise RuntimeError("Database connection not opened. Call 'await connect()' first.")
        return self._conn

    async def connect(self) -> None:
        """Opens database connection, activates WAL mode, and verifies table schemas."""
        db_dir = os.path.dirname(self.db_path)
        if db_dir:
            try:
                os.makedirs(db_dir, exist_ok=True)
            except OSError as err:
                logger.warning(
                    "Could not create database directory '%s': %s. Falling back to local 'iothub.db'",
                    db_dir,
                    err,
                )
                self.db_path = "iothub.db"

        self._conn = await aiosqlite.connect(self.db_path)
        self._conn.row_factory = aiosqlite.Row

        # Enable SQLite WAL mode, foreign keys, and busy timeouts
        await self._configure_pragmas()

        # Initialize recommended schema (skipped gracefully if opened read-only)
        try:
            await self._init_schema()
        except Exception as exc:
            logger.debug("Schema initialization skipped (read-only database): %s", exc)

        logger.info("SQLite database connected with WAL mode at '%s'", self.db_path)

    async def close(self) -> None:
        """Gracefully closes the database connection."""
        if self._conn:
            await self._conn.close()
            self._conn = None
            logger.info("SQLite database connection closed.")

    async def _configure_pragmas(self) -> None:
        """Configures performance and consistency PRAGMAs."""
        assert self._conn is not None
        try:
            async with self._conn.execute("PRAGMA journal_mode = WAL;") as cur:
                row = await cur.fetchone()
                journal_mode = row[0] if row else "unknown"
                logger.debug("SQLite journal_mode set to: %s", journal_mode)
        except Exception as exc:
            logger.debug("PRAGMA journal_mode=WAL skipped (read-only database): %s", exc)

        try:
            await self._conn.execute("PRAGMA synchronous = NORMAL;")
            await self._conn.execute("PRAGMA foreign_keys = ON;")
            await self._conn.execute("PRAGMA busy_timeout = 5000;")
        except Exception as exc:
            logger.debug("PRAGMA configuration skipped (read-only database): %s", exc)

    @asynccontextmanager
    async def transaction(self) -> AsyncGenerator[aiosqlite.Connection, None]:
        """
        Transactional context manager.
        Begins an IMMEDIATE transaction, commits on success, and rolls back on any error.
        Guarantees atomicity when writing schedules and actions.
        """
        conn = self.connection
        await conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
            await conn.commit()
        except Exception:
            await conn.rollback()
            raise

    async def get_due_actions(self, now_iso: str) -> List[Dict[str, Any]]:
        """Finds and atomically claims due actions up to now_iso."""
        from storage.schedule_repository import ScheduleRepository
        repo = ScheduleRepository(self)
        return await repo.claim_due_actions(now_iso)

    async def claim_due_actions(self, now_iso: str) -> List[Dict[str, Any]]:
        """Atomically claims due actions up to now_iso."""
        from storage.schedule_repository import ScheduleRepository
        repo = ScheduleRepository(self)
        return await repo.claim_due_actions(now_iso)

    async def _init_schema(self) -> None:
        """Initializes tables according to the recommended specification."""
        assert self._conn is not None

        # 1. Schedules table
        await self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS schedules (
                schedule_id TEXT PRIMARY KEY,
                date TEXT NOT NULL,
                revision INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'ACTIVE',
                raw_json_hash TEXT NOT NULL,
                raw_json TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        try:
            await self._conn.execute("ALTER TABLE schedules ADD COLUMN raw_json TEXT;")
        except Exception:
            pass

        # 2. Scheduled Actions table
        await self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS scheduled_actions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                schedule_id TEXT NOT NULL,
                action_id TEXT NOT NULL,
                production_order_id TEXT,
                resource_id TEXT,
                sub_resource_id TEXT,
                iot_node_id TEXT NOT NULL,
                stage_id TEXT,
                scheduled_at TEXT NOT NULL,
                action TEXT NOT NULL,
                channel_key TEXT NOT NULL,
                device_id TEXT,
                device_type TEXT,
                device_number INTEGER,
                device_name TEXT,
                params TEXT,
                status TEXT NOT NULL DEFAULT 'PENDING',
                created_at TEXT NOT NULL,
                executed_at TEXT,
                FOREIGN KEY(schedule_id) REFERENCES schedules(schedule_id) ON DELETE CASCADE,
                UNIQUE(schedule_id, action_id)
            )
            """
        )

        # Migration: ensure device assignment columns and params exist
        async with self._conn.execute("PRAGMA table_info(scheduled_actions)") as cur:
            existing_cols = {row[1] for row in await cur.fetchall()}
        for col, ddl in (
            ("device_id", "TEXT"),
            ("device_type", "TEXT"),
            ("device_number", "INTEGER"),
            ("device_name", "TEXT"),
            ("params", "TEXT"),
        ):
            if col not in existing_cols:
                await self._conn.execute(f"ALTER TABLE scheduled_actions ADD COLUMN {col} {ddl};")
        if "params_json" in existing_cols:
            try:
                await self._conn.execute(
                    """
                    UPDATE scheduled_actions SET
                        device_id = COALESCE(device_id, json_extract(params_json, '$.device'), iot_node_id),
                        device_type = COALESCE(device_type, json_extract(params_json, '$.device_type')),
                        device_number = COALESCE(device_number, json_extract(params_json, '$.device_number'))
                    WHERE device_id IS NULL
                    """
                )
                await self._conn.execute("ALTER TABLE scheduled_actions DROP COLUMN params_json;")
            except Exception as exc:
                logger.debug("Legacy params_json migration skipped: %s", exc)
        await self._conn.commit()
        await self._conn.execute("CREATE INDEX IF NOT EXISTS idx_actions_schedule ON scheduled_actions(schedule_id);")
        await self._conn.execute("CREATE INDEX IF NOT EXISTS idx_actions_due ON scheduled_actions(scheduled_at, status);")

        # 3. Devices table
        await self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS devices (
                device_id TEXT PRIMARY KEY,
                name TEXT,
                capabilities_json TEXT NOT NULL DEFAULT '{}',
                connection_state TEXT NOT NULL DEFAULT 'UNKNOWN',
                last_seen TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )

        # 4. Executions table
        await self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS executions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                action_id TEXT,
                schedule_id TEXT,
                device_id TEXT NOT NULL,
                command_action TEXT NOT NULL,
                attempt INTEGER NOT NULL DEFAULT 1,
                status TEXT NOT NULL,
                result_json TEXT,
                error_message TEXT,
                executed_at TEXT NOT NULL
            )
            """
        )
        await self._conn.execute("CREATE INDEX IF NOT EXISTS idx_executions_action ON executions(schedule_id, action_id);")

        # 5. Event Outbox table
        await self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS event_outbox (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT,
                event_type TEXT NOT NULL,
                routing_key TEXT,
                payload_json TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'PENDING',
                retry_count INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                processed_at TEXT
            )
            """
        )
        try:
            await self._conn.execute("ALTER TABLE event_outbox ADD COLUMN event_id TEXT;")
        except Exception:
            pass
        await self._conn.execute("CREATE INDEX IF NOT EXISTS idx_outbox_pending ON event_outbox(status, id);")
        await self._conn.execute("CREATE INDEX IF NOT EXISTS idx_outbox_event_id ON event_outbox(event_id);")

        # 6. Alerts table
        await self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                level TEXT NOT NULL,
                source TEXT NOT NULL,
                message TEXT NOT NULL,
                details_json TEXT,
                created_at TEXT NOT NULL
            )
            """
        )

        # 7. Config Settings table (Customization 2 & 3: Encrypted Secrets & Configuration)
        await self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS config_settings (
                config_key TEXT PRIMARY KEY,
                storage_type TEXT NOT NULL,
                value_text TEXT,
                encrypted_value BLOB,
                nonce BLOB,
                key_version INTEGER DEFAULT 1,
                updated_at TEXT NOT NULL
            )
            """
        )

        # 8. Config Audit table
        await self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS config_audit (
                audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
                config_key TEXT NOT NULL,
                operation TEXT NOT NULL,
                actor TEXT NOT NULL,
                changed_at TEXT NOT NULL
            )
            """
        )

        # 9. Device Bindings table (Customization 4: Interactive USB Port Enrollment)
        await self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS device_bindings (
                device_mac TEXT PRIMARY KEY,
                device_name TEXT NOT NULL,
                port_hint TEXT,
                usb_vid TEXT,
                usb_pid TEXT,
                usb_serial TEXT,
                connection_mode TEXT DEFAULT 'auto',
                approved INTEGER DEFAULT 1,
                profile_id TEXT DEFAULT 'plant_controller_v1',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )

        # 10. Device Profiles table (Customization 5: Channel and Command Definitions)
        await self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS device_profiles (
                profile_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                profile_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )

        # 11. Software Versions table (Customization 10: Version and Migration Tracking)
        await self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS software_versions (
                version TEXT PRIMARY KEY,
                applied_at TEXT NOT NULL
            )
            """
        )

        # 12. Action Status table (Stores latest status JSON per device_name primary key)
        await self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS action_status (
                device_name TEXT PRIMARY KEY,
                action TEXT NOT NULL,
                status TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        await self._conn.execute("CREATE INDEX IF NOT EXISTS idx_action_status_updated ON action_status(updated_at);")
        await self._conn.commit()

    # Device repository methods
    async def save_device(
        self,
        device_id: str,
        name: Optional[str] = None,
        capabilities: Optional[Dict[str, Any]] = None,
        connection_state: str = "ONLINE",
    ) -> None:
        """Persists physical device identity, capabilities, and connection state."""
        assert self._conn is not None
        now = datetime.now(timezone.utc).isoformat()
        caps_json = json.dumps(capabilities or {})
        await self._conn.execute(
            """
            INSERT INTO devices (device_id, name, capabilities_json, connection_state, last_seen, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(device_id) DO UPDATE SET
                name = COALESCE(excluded.name, devices.name),
                capabilities_json = excluded.capabilities_json,
                connection_state = excluded.connection_state,
                last_seen = excluded.last_seen,
                updated_at = excluded.updated_at
            """,
            (device_id, name, caps_json, connection_state, now, now),
        )
        await self._conn.commit()

    # Alias for compatibility
    register_device = save_device

    async def update_device_connection(self, device_id: str, state: str) -> None:
        """Updates or registers device connection state (e.g., ONLINE, OFFLINE, QUARANTINED)."""
        assert self._conn is not None
        now = datetime.now(timezone.utc).isoformat()
        await self._conn.execute(
            """
            INSERT INTO devices (device_id, name, capabilities_json, connection_state, last_seen, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(device_id) DO UPDATE SET
                connection_state = excluded.connection_state,
                updated_at = excluded.updated_at
            """,
            (device_id, device_id, json.dumps({}), state, now, now),
        )
        await self._conn.commit()

    async def get_device(self, device_id: str) -> Optional[Dict[str, Any]]:
        """Retrieves registered device state."""
        assert self._conn is not None
        async with self._conn.execute(
            "SELECT device_id, name, capabilities_json, connection_state, last_seen, updated_at FROM devices WHERE device_id = ?",
            (device_id,),
        ) as cur:
            row = await cur.fetchone()
            if row:
                return {
                    "device_id": row["device_id"],
                    "name": row["name"],
                    "capabilities": json.loads(row["capabilities_json"]),
                    "connection_state": row["connection_state"],
                    "last_seen": row["last_seen"],
                    "updated_at": row["updated_at"],
                }
            return None

    async def list_devices(self) -> List[Dict[str, Any]]:
        """Retrieves all registered devices from database."""
        assert self._conn is not None
        async with self._conn.execute(
            "SELECT device_id, name, capabilities_json, connection_state, last_seen, updated_at FROM devices ORDER BY device_id ASC"
        ) as cur:
            rows = await cur.fetchall()
            return [
                {
                    "device_id": row["device_id"],
                    "name": row["name"],
                    "capabilities": json.loads(row["capabilities_json"]) if row["capabilities_json"] else {},
                    "connection_state": row["connection_state"],
                    "last_seen": row["last_seen"],
                    "updated_at": row["updated_at"],
                }
                for row in rows
            ]

    # Alert repository methods
    async def record_alert(
        self,
        level: str,
        source: str,
        message: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> int:
        """Persists operational failures and supervisor alert notifications."""
        assert self._conn is not None
        now = datetime.now(timezone.utc).isoformat()
        cur = await self._conn.execute(
            "INSERT INTO alerts (level, source, message, details_json, created_at) VALUES (?, ?, ?, ?, ?)",
            (level, source, message, json.dumps(details or {}), now),
        )
        await self._conn.commit()
        return cur.lastrowid or 0

    async def get_alerts(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Retrieves recent alerts."""
        assert self._conn is not None
        async with self._conn.execute(
            "SELECT id, level, source, message, details_json, created_at FROM alerts ORDER BY id DESC LIMIT ?",
            (limit,),
        ) as cur:
            rows = await cur.fetchall()
            return [
                {
                    "id": row["id"],
                    "level": row["level"],
                    "source": row["source"],
                    "message": row["message"],
                    "details": json.loads(row["details_json"]) if row["details_json"] else {},
                    "created_at": row["created_at"],
                }
                for row in rows
            ]
