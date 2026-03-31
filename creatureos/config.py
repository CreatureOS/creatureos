from __future__ import annotations

import hashlib
import os
import sys
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path


_DATA_DIR_OVERRIDE: ContextVar[Path | None] = ContextVar("creatureos_data_dir_override", default=None)
_DB_PATH_OVERRIDE: ContextVar[Path | None] = ContextVar("creatureos_db_path_override", default=None)
_WORKSPACE_ROOT_OVERRIDE: ContextVar[Path | None] = ContextVar("creatureos_workspace_root_override", default=None)
_SERVER_RUNTIME_ROOT_FILES = (
    "cli.py",
    "web.py",
    "service.py",
    "storage.py",
    "config.py",
    "codex_cli.py",
)
# Templates and static assets are read directly on each refresh, so they do not
# belong in the supervisor restart set.
_SERVER_RUNTIME_DIRS = (
)


def _env(*names: str) -> str:
    for name in names:
        value = os.getenv(name, "").strip()
        if value:
            return value
    return ""


@contextmanager
def override_runtime_paths(
    *,
    workspace_root: str | Path | None = None,
    data_dir: str | Path | None = None,
    db_path: str | Path | None = None,
):
    workspace_token = _WORKSPACE_ROOT_OVERRIDE.set(Path(workspace_root).expanduser() if workspace_root else None)
    data_token = _DATA_DIR_OVERRIDE.set(Path(data_dir).expanduser() if data_dir else None)
    db_token = _DB_PATH_OVERRIDE.set(Path(db_path).expanduser() if db_path else None)
    try:
        yield
    finally:
        _WORKSPACE_ROOT_OVERRIDE.reset(workspace_token)
        _DATA_DIR_OVERRIDE.reset(data_token)
        _DB_PATH_OVERRIDE.reset(db_token)


def package_root() -> Path:
    return Path(__file__).resolve().parent


def project_root() -> Path:
    return package_root()


def workspace_root() -> Path:
    override = _WORKSPACE_ROOT_OVERRIDE.get()
    if override is not None:
        return override
    value = _env("CREATURE_OS_WORKSPACE_ROOT")
    if value:
        return Path(value).expanduser()
    return Path.cwd()


def workspace_root_source() -> str:
    if _WORKSPACE_ROOT_OVERRIDE.get() is not None:
        return "flag"
    if _env("CREATURE_OS_WORKSPACE_ROOT"):
        return "env"
    return "cwd"


def template_dir() -> Path:
    return package_root() / "templates"


def static_dir() -> Path:
    return package_root() / "static"


def _default_data_dir() -> Path:
    if os.name == "nt":
        base = Path(_env("LOCALAPPDATA") or (Path.home() / "AppData" / "Local"))
        return base / "CreatureOS"
    if sys.platform == "darwin":
        return Path.home() / "Library" / "Application Support" / "CreatureOS"
    xdg_state_home = _env("XDG_STATE_HOME")
    if xdg_state_home:
        return Path(xdg_state_home).expanduser() / "creatureos"
    return Path.home() / ".local" / "state" / "creatureos"

def data_dir() -> Path:
    override = _DATA_DIR_OVERRIDE.get()
    if override is not None:
        override.mkdir(parents=True, exist_ok=True)
        return override
    value = _env("CREATURE_OS_DATA_DIR")
    path = Path(value).expanduser() if value else _default_data_dir()
    path.mkdir(parents=True, exist_ok=True)
    return path


def data_dir_source() -> str:
    if _DATA_DIR_OVERRIDE.get() is not None:
        return "flag"
    if _env("CREATURE_OS_DATA_DIR"):
        return "env"
    return "default"


def db_path() -> Path:
    override = _DB_PATH_OVERRIDE.get()
    if override is not None:
        override.parent.mkdir(parents=True, exist_ok=True)
        return override
    value = _env("CREATURE_OS_DB_PATH")
    if value:
        path = Path(value).expanduser()
        path.parent.mkdir(parents=True, exist_ok=True)
        return path
    return data_dir() / "creature_os.sqlite3"


def log_dir() -> Path:
    path = data_dir() / "logs"
    path.mkdir(parents=True, exist_ok=True)
    return path


def server_lock_path() -> Path:
    return data_dir() / "server.lock"


def server_pid_path() -> Path:
    return data_dir() / "server.pid"


def server_ready_path() -> Path:
    return data_dir() / "server.ready.json"


def server_runtime_paths() -> list[Path]:
    root = package_root()
    paths: list[Path] = []
    seen: set[Path] = set()
    for name in _SERVER_RUNTIME_ROOT_FILES:
        path = root / name
        if path.is_file() and path not in seen:
            seen.add(path)
            paths.append(path)
    for name in _SERVER_RUNTIME_DIRS:
        directory = root / name
        if not directory.is_dir():
            continue
        for path in sorted(directory.rglob("*")):
            if path.is_file() and path not in seen:
                seen.add(path)
                paths.append(path)
    pyproject_path = root.parent / "pyproject.toml"
    if pyproject_path.is_file() and pyproject_path not in seen:
        seen.add(pyproject_path)
        paths.append(pyproject_path)
    return paths


def server_source_revision() -> str:
    root = package_root().parent
    digest = hashlib.blake2s(digest_size=16)
    for path in server_runtime_paths():
        stat = path.stat()
        relative = path.relative_to(root).as_posix()
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        digest.update(str(stat.st_mtime_ns).encode("ascii"))
        digest.update(b":")
        digest.update(str(stat.st_size).encode("ascii"))
        digest.update(b"\n")
    return digest.hexdigest()


def static_version(source_revision: str | None = None) -> str:
    revision = str(source_revision or server_source_revision()).strip() or "dev"
    return f"creatureos-{revision[:12]}"


def host() -> str:
    return _env("CREATURE_OS_HOST") or "127.0.0.1"


def public_host() -> str:
    return _env("CREATURE_OS_PUBLIC_HOST") or "localhost"


def port() -> int:
    return int(_env("CREATURE_OS_PORT") or "404")


def codex_bin() -> str:
    override = _env("CREATURE_OS_CODEX_BIN", "CODEX_BIN")
    if override:
        return override
    if os.name == "nt":
        appdata = _env("APPDATA")
        candidates = []
        if appdata:
            candidates.append(Path(appdata) / "npm" / "codex.cmd")
        candidates.append(Path.home() / "AppData" / "Roaming" / "npm" / "codex.cmd")
        for candidate in candidates:
            if candidate.is_file():
                return str(candidate)
    return "codex"


def creature_model() -> str:
    return _env("CREATURE_OS_MODEL") or "gpt-5.4"


def creature_reasoning_effort() -> str:
    return _env("CREATURE_OS_REASONING_EFFORT") or "xhigh"


def _optional_timeout_env(name: str) -> int | None:
    raw = _env(name)
    if not raw:
        return None
    value = int(raw)
    return value if value > 0 else None


def creature_timeout_seconds(sandbox_mode: str = "read-only") -> int | None:
    if sandbox_mode == "workspace-write":
        return _optional_timeout_env("CREATURE_OS_WRITE_TIMEOUT_SECONDS")
    return _optional_timeout_env("CREATURE_OS_TIMEOUT_SECONDS")


def python_bin() -> str:
    override = _env("CREATURE_OS_PYTHON_BIN", "PYTHON_BIN")
    if override:
        return override
    if sys.executable:
        return sys.executable
    return "python" if os.name == "nt" else "python3"


def app_url() -> str:
    return f"http://{public_host()}:{port()}/"
