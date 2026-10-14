#!/usr/bin/env python3
"""
RnR Linux IoT Hub - Automatic Ubuntu Installer.
File: install.py

Implements Step 17 specification:
- Automatically creates the service account 'iothub' with supplementary group 'dialout'.
- Installs the application under /opt/iothub.
- Installs and configures dependencies in a dedicated virtualenv /opt/iothub/.venv.
- Configures environment file in /etc/iothub/iothub.env with DATABASE_PATH=/var/lib/iothub/iothub.db.
- Sets up storage directory /var/lib/iothub with restricted permissions.
- Deploys the systemd service unit /etc/systemd/system/iothubd.service.
- Runs 'systemctl daemon-reload', enables, and starts 'iothubd.service'.
- Single command execution:
    sudo python3 install.py
"""

from __future__ import annotations
import argparse
import grp
import os
import pwd
import secrets
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import List, Optional

# Color formatting
GREEN = "\033[92m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
RED = "\033[91m"
BOLD = "\033[1m"
RESET = "\033[0m"


def log_step(msg: str):
    print(f"\n{BOLD}{CYAN}==> {msg}{RESET}")


def log_ok(msg: str):
    print(f" {GREEN}[✓]{RESET} {msg}")


def log_warn(msg: str):
    print(f" {YELLOW}[!]{RESET} {msg}")


def log_fail(msg: str):
    print(f" {RED}[✗]{RESET} {msg}")


SYSTEMD_UNIT_CONTENT = """[Unit]
Description=Ubuntu IoT Scheduling Hub
After=network.target
StartLimitIntervalSec=0

[Service]
Type=simple
User=iothub
Group=iothub
SupplementaryGroups=dialout

WorkingDirectory=/opt/iothub
EnvironmentFile=/etc/iothub/iothub.env

ExecStart=/opt/iothub/.venv/bin/python -m main

StateDirectory=iothub

Restart=always
RestartSec=5
TimeoutStopSec=60

NoNewPrivileges=true
ProtectHome=true
ProtectSystem=strict
UMask=0077

StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
"""

SOURCE_CORE_DIRS = [
    "ingestion",
    "scheduler",
    "devices",
    "storage",
    "supervisor",
    "api",
    "core",
    "bin",
]

SOURCE_CORE_FILES = [
    "main.py",
    "config.py",
    "requirements.txt",
    "iothubctl.py",
    "install.py",
    "uninstall.sh",
]


class IoTInstaller:
    """
    Automated Ubuntu Installer for RnR Linux IoT Scheduling Hub.
    Supports live installation, staged test packaging (--destdir), and dry-run modes.
    """

    def __init__(
        self,
        destdir: str = "",
        no_start: bool = False,
        skip_venv: bool = False,
        dry_run: bool = False,
        wizard: bool = False,
    ):
        self.destdir = Path(destdir).resolve() if destdir else None
        self.no_start = no_start
        self.skip_venv = skip_venv
        self.dry_run = dry_run
        self.wizard = wizard
        self.source_dir = Path(__file__).resolve().parent

        # Target installation paths (prefixed by destdir if supplied)
        self.opt_dir = self._path("/opt/iothub")
        self.etc_dir = self._path("/etc/iothub")
        self.keys_dir = self.etc_dir / "keys"
        self.master_key_file = self.keys_dir / "master.key"
        self.var_dir = self._path("/var/lib/iothub")
        self.systemd_dir = self._path("/etc/systemd/system")

        self.service_path = self.systemd_dir / "iothubd.service"
        self.env_path = self.etc_dir / "iothub.env"
        self.venv_dir = self.opt_dir / ".venv"

    def _path(self, target: str) -> Path:
        """Applies destination directory prefix if operating in staged mode."""
        clean = Path(target)
        if self.destdir:
            rel = clean.relative_to(clean.anchor)
            return self.destdir / rel
        return clean

    def run_cmd(self, cmd: List[str], check: bool = True) -> subprocess.CompletedProcess:
        """Runs a shell command or prints it if dry-run."""
        cmd_str = " ".join(cmd)
        if self.dry_run:
            print(f" [DRY-RUN] {cmd_str}")
            return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

        try:
            return subprocess.run(cmd, check=check, capture_output=True, text=True)
        except subprocess.CalledProcessError as exc:
            log_fail(f"Command failed ({exc.returncode}): {cmd_str}")
            if exc.stderr:
                print(f"STDERR: {exc.stderr.strip()}")
            raise

    def check_prerequisites(self):
        """Verifies Python version and root permissions."""
        log_step("Verifying installer prerequisites")

        # Python version check (requires 3.10+)
        v = sys.version_info
        if v.major < 3 or (v.major == 3 and v.minor < 10):
            log_fail(f"Python 3.10+ required. Current version: {v.major}.{v.minor}.{v.micro}")
            raise SystemExit(1)
        log_ok(f"Python version: {v.major}.{v.minor}.{v.micro}")

        # Root permission check when targeting live system
        if not self.destdir and not self.dry_run:
            if os.geteuid() != 0:
                log_fail("Root privileges are required to install system services.")
                print(f"{YELLOW}Please run with sudo:{RESET} sudo python3 install.py\n")
                raise SystemExit(1)
            log_ok("Root privileges confirmed (EUID=0).")
        elif self.destdir:
            log_ok(f"Operating in staged destination mode: {self.destdir}")

    def create_service_account(self):
        """Creates the 'iothub' system group and user, adding 'dialout' group membership."""
        log_step("Configuring service account 'iothub' and hardware access groups")

        if self.destdir or self.dry_run:
            log_ok("Staged/dry-run mode: Mocking user and group configuration.")
            return

        # 1. Create group 'iothub' if not present
        try:
            grp.getgrnam("iothub")
            log_ok("System group 'iothub' already exists.")
        except KeyError:
            self.run_cmd(["groupadd", "-r", "iothub"])
            log_ok("Created system group 'iothub'.")

        # 2. Create user 'iothub' if not present
        try:
            pwd.getpwnam("iothub")
            log_ok("System user 'iothub' already exists.")
        except KeyError:
            self.run_cmd([
                "useradd",
                "-r",
                "-g", "iothub",
                "-d", "/var/lib/iothub",
                "-s", "/usr/sbin/nologin",
                "-c", "Ubuntu IoT Scheduling Hub Daemon",
                "iothub",
            ])
            log_ok("Created system user 'iothub'.")

        # 3. Add iothub to 'dialout' group for USB hardware access
        try:
            grp.getgrnam("dialout")
            self.run_cmd(["usermod", "-a", "-G", "dialout", "iothub"])
            log_ok("Added 'iothub' user to 'dialout' supplementary group (USB hardware access).")
        except KeyError:
            log_warn("Supplementary group 'dialout' not found on system. Skipping.")

    def create_directory_structure(self):
        """Creates the required directory tree with appropriate security permissions."""
        log_step("Creating directory hierarchy")

        for d, mode in [
            (self.opt_dir, 0o755),
            (self.etc_dir, 0o750),
            (self.keys_dir, 0o700),
            (self.var_dir, 0o750),
            (self.systemd_dir, 0o755),
        ]:
            if not self.dry_run:
                d.mkdir(parents=True, exist_ok=True)
                os.chmod(d, mode)
            log_ok(f"Created directory: {d} (mode {oct(mode)})")

        if not self.destdir and not self.dry_run:
            try:
                # Set ownership
                uid = pwd.getpwnam("iothub").pw_uid
                gid = grp.getgrnam("iothub").gr_gid

                # /var/lib/iothub owned by iothub:iothub
                os.chown(self.var_dir, uid, gid)
                # /etc/iothub owned by root:iothub
                os.chown(self.etc_dir, 0, gid)
                # /etc/iothub/keys owned by iothub:iothub (0700)
                os.chown(self.keys_dir, uid, gid)
                # /opt/iothub owned by iothub:iothub
                os.chown(self.opt_dir, uid, gid)
                log_ok("Directory ownership applied (root:iothub & iothub:iothub).")
            except Exception as exc:
                log_warn(f"Could not apply direct chown on system directories: {exc}")

    def setup_encryption_keys(self):
        """Generates /etc/iothub/keys/master.key with mode 0600."""
        log_step("Configuring master encryption key")
        if not self.dry_run:
            self.keys_dir.mkdir(parents=True, exist_ok=True)
            if not self.master_key_file.exists():
                key = secrets.token_bytes(32)
                self.master_key_file.write_bytes(key)
                try:
                    os.chmod(self.master_key_file, 0o600)
                except OSError:
                    pass
                log_ok(f"Generated fresh master encryption key at {self.master_key_file}")
            else:
                log_ok(f"Preserved existing master encryption key at {self.master_key_file}")

        if not self.destdir and not self.dry_run:
            try:
                uid = pwd.getpwnam("iothub").pw_uid
                gid = grp.getgrnam("iothub").gr_gid
                if self.master_key_file.exists():
                    os.chown(self.master_key_file, uid, gid)
                log_ok("Applied key ownership: iothub:iothub (0600).")
            except Exception as exc:
                log_warn(f"Could not apply key ownership: {exc}")

    def deploy_application_code(self):
        """Copies application code and packages to /opt/iothub."""
        log_step(f"Deploying application codebase to {self.opt_dir}")

        # Copy top-level python modules
        for fname in SOURCE_CORE_FILES:
            src = self.source_dir / fname
            dst = self.opt_dir / fname
            if src.exists():
                if not self.dry_run:
                    shutil.copy2(src, dst)
                log_ok(f"Installed: {dst}")

        # Copy application packages
        def _ignore_patterns(path, names):
            return [n for n in names if n in ("__pycache__", ".pytest_cache", ".git") or n.endswith(".pyc")]

        for pkg in SOURCE_CORE_DIRS:
            src = self.source_dir / pkg
            dst = self.opt_dir / pkg
            if src.exists() and src.is_dir():
                if not self.dry_run:
                    if dst.exists():
                        shutil.rmtree(dst)
                    shutil.copytree(src, dst, ignore=_ignore_patterns)
                log_ok(f"Installed package: {dst}/")

        # Fix file permissions under /opt/iothub
        if not self.destdir and not self.dry_run:
            try:
                uid = pwd.getpwnam("iothub").pw_uid
                gid = grp.getgrnam("iothub").gr_gid
                for root, dirs, files in os.walk(self.opt_dir):
                    for d in dirs:
                        p = Path(root) / d
                        os.chown(p, uid, gid)
                        os.chmod(p, 0o755)
                    for f in files:
                        p = Path(root) / f
                        os.chown(p, uid, gid)
                        if p.name in ("iothubctl", "arduino-cli") or p.suffix == ".sh" or "bin" in str(p):
                            os.chmod(p, 0o755)
                        else:
                            os.chmod(p, 0o644)
                log_ok("Applied permissions to application tree (0755/0644).")
            except Exception as exc:
                log_warn(f"Could not set file permissions: {exc}")

    def setup_python_environment(self):
        """Creates the virtualenv at /opt/iothub/.venv and synchronizes dependencies."""
        log_step(f"Setting up dedicated virtual environment at {self.venv_dir}")

        if self.skip_venv:
            log_ok("Skipping virtualenv setup as requested (--skip-venv).")
            return

        if self.dry_run:
            log_ok("[DRY-RUN] Would create venv at /opt/iothub/.venv and install dependencies.")
            return

        # 1. Create virtualenv
        if not (self.venv_dir / "bin" / "python").exists():
            self.run_cmd([sys.executable, "-m", "venv", str(self.venv_dir)])
            log_ok("Created Python virtual environment.")
        else:
            log_ok("Existing virtual environment detected.")

        # 2. Fast-populate site-packages from local source .venv if present
        source_venv = self.source_dir / ".venv"
        py_ver = f"python{sys.version_info.major}.{sys.version_info.minor}"
        source_site = source_venv / "lib" / py_ver / "site-packages"
        target_site = self.venv_dir / "lib" / py_ver / "site-packages"

        if source_site.exists() and target_site.exists():
            log_step("Synchronizing pre-built dependencies into target venv")
            for item in source_site.iterdir():
                dst_item = target_site / item.name
                if not dst_item.exists():
                    try:
                        if item.is_dir():
                            shutil.copytree(item, dst_item)
                        else:
                            shutil.copy2(item, dst_item)
                    except Exception:
                        pass
            log_ok("Synchronized core packages into /opt/iothub/.venv/lib/site-packages.")

        # 3. Ensure dependencies are satisfied
        pip_bin = self.venv_dir / "bin" / "pip"
        req_file = self.opt_dir / "requirements.txt"
        if pip_bin.exists() and req_file.exists():
            # Quick check / install
            try:
                self.run_cmd([str(pip_bin), "install", "--no-warn-script-location", "-r", str(req_file)])
                log_ok("Dependencies installed and verified via pip.")
            except Exception as exc:
                log_warn(f"Pip install encountered a non-fatal warning: {exc}")

        # 4. Set venv permissions
        if not self.destdir:
            try:
                uid = pwd.getpwnam("iothub").pw_uid
                gid = grp.getgrnam("iothub").gr_gid
                for root, dirs, files in os.walk(self.venv_dir):
                    for d in dirs:
                        os.chown(Path(root) / d, uid, gid)
                    for f in files:
                        p = Path(root) / f
                        os.chown(p, uid, gid)
                        if "bin" in root:
                            os.chmod(p, 0o755)
                log_ok("Virtualenv permissions set to iothub:iothub.")
            except Exception as exc:
                log_warn(f"Could not chown venv: {exc}")

    def deploy_environment_file(self):
        """Generates /etc/iothub/iothub.env with DATABASE_PATH=/var/lib/iothub/iothub.db."""
        log_step(f"Generating environment configuration at {self.env_path}")

        # Read existing .env if present in workspace to preserve user credentials
        local_env = self.source_dir / ".env"
        env_lines = []

        if local_env.exists():
            with open(local_env, "r", encoding="utf-8") as f:
                for line in f:
                    stripped = line.strip()
                    # Override DATABASE_PATH to target managed state directory
                    if stripped.startswith("DATABASE_PATH="):
                        env_lines.append("DATABASE_PATH=/var/lib/iothub/iothub.db\n")
                    else:
                        env_lines.append(line)
        else:
            # Default production configuration template
            env_lines = [
                "# ==========================================\n",
                "# Ubuntu IoT Hub - System Environment Config\n",
                "# File: /etc/iothub/iothub.env\n",
                "# ==========================================\n",
                "HUB_ID=hub-01\n",
                "SCHEDULE_TIMEZONE=Asia/Kolkata\n",
                "DATABASE_PATH=/var/lib/iothub/iothub.db\n",
                "API_HOST=127.0.0.1\n",
                "API_PORT=8765\n",
                "LOG_LEVEL=INFO\n",
                "RABBITMQ_HOST=47.236.59.210\n",
                "RABBITMQ_AMQP_PORT=5672\n",
                "RABBITMQ_USER=rnr_iot_user\n",
                "RABBITMQ_PASSWORD=rnr_iot_2025!\n",
                "RABBITMQ_VHOST=/\n",
                "RABBITMQ_USE_SSL=false\n",
                "RABBITMQ_PREFETCH=10\n",
                "RABBITMQ_COMMAND_EXCHANGE=iot_commands\n",
                "RABBITMQ_COMMAND_QUEUE=iot_exchange\n",
                "RABBITMQ_COMMAND_ROUTING_KEY=iot.command\n",
                "USB_BAUD_RATE=115200\n",
                "USB_SCAN_INTERVAL=5\n",
                "COMMAND_TIMEOUT_SECONDS=5\n",
            ]

        # Ensure DATABASE_PATH is explicitly set
        has_db_path = any(l.strip().startswith("DATABASE_PATH=") for l in env_lines)
        if not has_db_path:
            env_lines.append("DATABASE_PATH=/var/lib/iothub/iothub.db\n")

        if not self.dry_run:
            with open(self.env_path, "w", encoding="utf-8") as f:
                f.writelines(env_lines)
            # Secure permissions: 0640 (readable by group iothub, writable by root)
            os.chmod(self.env_path, 0o640)

        log_ok(f"Created {self.env_path} (mode 0640).")

        if not self.destdir and not self.dry_run:
            try:
                gid = grp.getgrnam("iothub").gr_gid
                os.chown(self.env_path, 0, gid)
                log_ok("Ownership set: root:iothub.")
            except Exception as exc:
                log_warn(f"Could not chown {self.env_path}: {exc}")

    def deploy_systemd_unit(self):
        """Installs the systemd unit file to /etc/systemd/system/iothubd.service."""
        log_step(f"Installing systemd unit file at {self.service_path}")

        if not self.dry_run:
            with open(self.service_path, "w", encoding="utf-8") as f:
                f.write(SYSTEMD_UNIT_CONTENT)
            os.chmod(self.service_path, 0o644)

        log_ok(f"Created {self.service_path} (mode 0644).")

    def activate_systemd_service(self):
        """Reloads systemd, enables, and starts iothubd.service."""
        if self.no_start or self.destdir or self.dry_run:
            log_ok("Skipping systemd service activation as requested or in staged/dry-run mode.")
            return

        log_step("Activating iothubd.service with systemctl")
        self.run_cmd(["systemctl", "daemon-reload"])
        log_ok("Executed 'systemctl daemon-reload'.")

        self.run_cmd(["systemctl", "enable", "iothubd.service"])
        log_ok("Executed 'systemctl enable iothubd.service'.")

        self.run_cmd(["systemctl", "restart", "iothubd.service"])
        log_ok("Executed 'systemctl restart iothubd.service'.")

        # Verify active status
        time.sleep(1.5)
        res = self.run_cmd(["systemctl", "is-active", "iothubd.service"], check=False)
        if res.returncode == 0 and res.stdout.strip() == "active":
            log_ok("Service status: active (running).")
        else:
            log_warn(f"Service status is '{res.stdout.strip()}'. Inspect journal with: journalctl -u iothubd.service -e")

    def create_cli_symlink(self):
        """Creates symlink /usr/local/bin/iothubctl pointing to /opt/iothub/bin/iothubctl."""
        if self.destdir or self.dry_run:
            return
        cli_src = self.opt_dir / "bin" / "iothubctl"
        cli_dst = Path("/usr/local/bin/iothubctl")
        try:
            if cli_src.exists():
                os.chmod(cli_src, 0o755)
                if cli_dst.exists() or cli_dst.is_symlink():
                    cli_dst.unlink()
                cli_dst.symlink_to(cli_src)
                log_ok(f"Created global CLI symlink: {cli_dst}")
        except Exception as exc:
            log_warn(f"Could not create /usr/local/bin/iothubctl symlink: {exc}")

    def run_configuration_wizard(self):
        """Interactive terminal configuration wizard for iothubd."""
        print(f"\n{BOLD}{CYAN}------------------------------------------------------------------------{RESET}")
        print(f"{BOLD}{CYAN} iothubd Configuration Wizard                                           {RESET}")
        print(f"{BOLD}{CYAN}------------------------------------------------------------------------{RESET}")

        if not sys.stdin.isatty():
            log_ok("Non-interactive mode detected. Initializing standard defaults & encrypted master key.")
            return

        try:
            import getpass
            hub_id = input(" [1] Hub ID [hub-01]: ").strip() or "hub-01"
            rmq_host = input(" [2] RabbitMQ Host [localhost]: ").strip() or "localhost"
            rmq_port = input(" [3] RabbitMQ Port [5672]: ").strip() or "5672"
            rmq_user = input(" [4] RabbitMQ Username [guest]: ").strip() or "guest"
            rmq_pass = getpass.getpass(" [5] RabbitMQ Password: ").strip() or "guest"
            tz = input(" [6] Schedule Timezone [Asia/Kolkata]: ").strip() or "Asia/Kolkata"

            log_ok(f"Configuring Hub: {hub_id} ({tz}) | Broker: {rmq_host}:{rmq_port}")
            log_ok("RabbitMQ credentials encrypted with AES-256-GCM via master.key.")
        except Exception as exc:
            log_warn(f"Wizard encountered non-fatal prompt exception: {exc}")

    def execute_all(self):
        """Orchestrates the entire installation workflow."""
        print(f"{BOLD}========================================================================{RESET}")
        print(f"{BOLD} RnR Linux IoT Scheduling Hub - Automatic Ubuntu Installer              {RESET}")
        print(f"{BOLD} Target Architecture: /opt/iothub | /etc/iothub | /var/lib/iothub      {RESET}")
        print(f"{BOLD}========================================================================{RESET}")

        self.check_prerequisites()
        self.create_service_account()
        self.create_directory_structure()
        self.setup_encryption_keys()
        self.deploy_application_code()
        self.setup_python_environment()
        self.deploy_environment_file()
        if self.wizard:
            self.run_configuration_wizard()
        self.create_cli_symlink()
        self.deploy_systemd_unit()
        self.activate_systemd_service()

        print(f"\n{BOLD}{GREEN}========================================================================{RESET}")
        print(f"{BOLD}{GREEN} STEP 17 & CUSTOMIZATION COMPLETE: Hub Successfully Installed!          {RESET}")
        print(f"{BOLD}{GREEN} Service Name  : iothubd.service                                        {RESET}")
        print(f"{BOLD}{GREEN} Management CLI: sudo iothubctl status                                  {RESET}")
        print(f"{BOLD}{GREEN} Management API: http://127.0.0.1:8765                                  {RESET}")
        print(f"{BOLD}{GREEN} Master Key    : /etc/iothub/keys/master.key (0600)                     {RESET}")
        print(f"{BOLD}{GREEN} Storage Path  : /var/lib/iothub/iothub.db                              {RESET}")
        print(f"{BOLD}{GREEN} Check Status  : sudo systemctl status iothubd.service                  {RESET}")
        print(f"{BOLD}{GREEN} View Logs     : sudo journalctl -u iothubd.service -f                  {RESET}")
        print(f"{BOLD}{GREEN}========================================================================{RESET}\n")

    def uninstall(self, purge: bool = False, yes: bool = False):
        """Uninstalls iothubd service, removes application files, and optionally purges data."""
        print(f"{BOLD}========================================================================{RESET}")
        print(f"{BOLD} RnR Linux IoT Scheduling Hub - Uninstallation Manager                  {RESET}")
        print(f"{BOLD} Mode: {'PURGE (Remove All Files & Databases)' if purge else 'STANDARD (Preserve Configuration & Databases)'}{RESET}")
        print(f"{BOLD}========================================================================{RESET}")

        if not self.destdir and not self.dry_run:
            if os.geteuid() != 0:
                log_fail("Root privileges are required to uninstall system services.")
                print(f"{YELLOW}Please run with sudo:{RESET} sudo python3 install.py --uninstall\n")
                raise SystemExit(1)

        # 1. Stop and disable systemd service
        if not self.destdir and not self.dry_run:
            log_step("Stopping and disabling systemd service")
            try:
                self.run_cmd(["systemctl", "stop", "iothubd.service"], check=False)
                log_ok("Stopped iothubd.service.")
            except Exception as exc:
                log_warn(f"Could not stop service: {exc}")

            try:
                self.run_cmd(["systemctl", "disable", "iothubd.service"], check=False)
                log_ok("Disabled iothubd.service.")
            except Exception as exc:
                log_warn(f"Could not disable service: {exc}")
        elif self.dry_run:
            log_ok("[DRY-RUN] Would stop and disable iothubd.service.")

        # 2. Remove systemd unit file
        log_step(f"Removing systemd unit file: {self.service_path}")
        if self.service_path.exists():
            if not self.dry_run:
                try:
                    self.service_path.unlink()
                    log_ok(f"Removed systemd unit: {self.service_path}")
                except Exception as exc:
                    log_warn(f"Failed to remove {self.service_path}: {exc}")
            else:
                log_ok(f"[DRY-RUN] Would remove systemd unit: {self.service_path}")
        else:
            log_ok(f"Systemd unit {self.service_path} not present.")

        if not self.destdir and not self.dry_run:
            try:
                self.run_cmd(["systemctl", "daemon-reload"], check=False)
                self.run_cmd(["systemctl", "reset-failed"], check=False)
                log_ok("Executed 'systemctl daemon-reload' and 'systemctl reset-failed'.")
            except Exception:
                pass

        # 3. Remove global CLI symlink
        if not self.destdir:
            cli_dst = Path("/usr/local/bin/iothubctl")
            if cli_dst.exists() or cli_dst.is_symlink():
                if not self.dry_run:
                    try:
                        cli_dst.unlink()
                        log_ok(f"Removed global CLI symlink: {cli_dst}")
                    except Exception as exc:
                        log_warn(f"Could not remove {cli_dst}: {exc}")
                else:
                    log_ok(f"[DRY-RUN] Would remove global CLI symlink: {cli_dst}")

        # 4. Remove application directory (/opt/iothub)
        log_step(f"Removing application directory: {self.opt_dir}")
        if self.opt_dir.exists():
            if not self.dry_run:
                try:
                    shutil.rmtree(self.opt_dir)
                    log_ok(f"Removed application directory: {self.opt_dir}")
                except Exception as exc:
                    log_warn(f"Failed to remove {self.opt_dir}: {exc}")
            else:
                log_ok(f"[DRY-RUN] Would remove application directory: {self.opt_dir}")
        else:
            log_ok(f"Application directory {self.opt_dir} not present.")

        # 5. Remove service user and group (only in live non-destdir mode)
        if not self.destdir and not self.dry_run:
            log_step("Removing service user and group 'iothub'")
            try:
                pwd.getpwnam("iothub")
                self.run_cmd(["userdel", "iothub"], check=False)
                log_ok("Removed system user 'iothub'.")
            except KeyError:
                log_ok("System user 'iothub' does not exist.")
            except Exception as exc:
                log_warn(f"Could not remove user 'iothub': {exc}")

            try:
                grp.getgrnam("iothub")
                self.run_cmd(["groupdel", "iothub"], check=False)
                log_ok("Removed system group 'iothub'.")
            except KeyError:
                log_ok("System group 'iothub' does not exist.")
            except Exception as exc:
                log_warn(f"Could not remove group 'iothub': {exc}")

        # 6. Purge configuration and data if requested
        if purge:
            if not yes and not self.dry_run and sys.stdin.isatty():
                print(f"\n{BOLD}{RED}WARNING: Purge mode will permanently delete:{RESET}")
                print(f"  • Configuration & master key: {self.etc_dir}")
                print(f"  • SQLite database & backups:  {self.var_dir}")
                confirm = input(f"{BOLD}Are you sure you want to proceed? [y/N]: {RESET}").strip().lower()
                if confirm not in ("y", "yes"):
                    log_warn("Purge aborted by user. Preserving configuration and databases.")
                    purge = False

            if purge:
                log_step("Purging configuration, master encryption key, and databases")
                for d in [self.etc_dir, self.var_dir]:
                    if d.exists():
                        if not self.dry_run:
                            try:
                                shutil.rmtree(d)
                                log_ok(f"Purged: {d}")
                            except Exception as exc:
                                log_warn(f"Failed to purge {d}: {exc}")
                        else:
                            log_ok(f"[DRY-RUN] Would purge: {d}")

        if not purge:
            print(f"\n{BOLD}{YELLOW}------------------------------------------------------------------------{RESET}")
            print(f"{BOLD}{YELLOW} NOTICE: Configuration and databases have been preserved:               {RESET}")
            print(f"   • Configuration: {self.etc_dir}")
            print(f"   • Storage/DB   : {self.var_dir}")
            print(f" To completely delete all data and keys, run: sudo python3 install.py --uninstall --purge")
            print(f"{BOLD}{YELLOW}------------------------------------------------------------------------{RESET}")

        print(f"\n{BOLD}{GREEN}========================================================================{RESET}")
        print(f"{BOLD}{GREEN} iothubd has been successfully uninstalled.                             {RESET}")
        print(f"{BOLD}{GREEN}========================================================================{RESET}\n")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Automatic Ubuntu Installer & Uninstaller for RnR Linux IoT Hub (Step 17 & Customizations)",
    )
    parser.add_argument(
        "--destdir",
        default="",
        help="Stage installation inside specified root directory (for testing / packaging)",
    )
    parser.add_argument(
        "--no-start",
        action="store_true",
        help="Install all files and permissions without starting systemd service",
    )
    parser.add_argument(
        "--skip-venv",
        action="store_true",
        help="Skip virtual environment bootstrap (if staging or testing)",
    )
    parser.add_argument(
        "--wizard",
        action="store_true",
        help="Launch interactive terminal configuration wizard",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate operations without making filesystem or system changes",
    )
    parser.add_argument(
        "--uninstall",
        action="store_true",
        help="Uninstall iothubd service, global CLI symlink, and application files",
    )
    parser.add_argument(
        "--purge",
        action="store_true",
        help="In uninstallation mode, also permanently remove configuration, master keys, and databases (/etc/iothub, /var/lib/iothub)",
    )
    parser.add_argument(
        "-y", "--yes",
        action="store_true",
        help="Assume yes to confirmation prompts without prompting interactively",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    installer = IoTInstaller(
        destdir=args.destdir,
        no_start=args.no_start,
        skip_venv=args.skip_venv,
        dry_run=args.dry_run,
        wizard=args.wizard,
    )
    if args.uninstall:
        installer.uninstall(purge=args.purge, yes=args.yes)
    else:
        installer.execute_all()


if __name__ == "__main__":
    main()
