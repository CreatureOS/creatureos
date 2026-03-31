from pathlib import Path
import signal
import subprocess

from creatureos import cli
from creatureos import config


def test_prepare_server_runtime_initializes_and_refreshes(monkeypatch):
    calls: list[tuple[str, str | None]] = []

    monkeypatch.setattr(cli.storage, "init_db", lambda: calls.append(("init_db", None)))
    monkeypatch.setattr(
        cli.storage,
        "delete_meta",
        lambda key: calls.append(("delete_meta", str(key))),
    )
    monkeypatch.setattr(
        cli.service,
        "ensure_runtime_ready",
        lambda: calls.append(("ensure_runtime_ready", None)),
    )

    cli._prepare_server_runtime(force_scan=True)

    assert calls == [
        ("init_db", None),
        ("delete_meta", cli.service.ONBOARDING_ENVIRONMENT_KEY),
        ("delete_meta", cli.service.ONBOARDING_BRIEFING_KEY),
        ("ensure_runtime_ready", None),
    ]


def test_prepare_server_runtime_without_force_scan(monkeypatch):
    calls: list[str] = []

    monkeypatch.setattr(cli.storage, "init_db", lambda: calls.append("init_db"))
    monkeypatch.setattr(
        cli.storage,
        "delete_meta",
        lambda key: calls.append(f"delete_meta:{key}"),
    )
    monkeypatch.setattr(
        cli.service,
        "ensure_runtime_ready",
        lambda: calls.append("ensure_runtime_ready"),
    )

    cli._prepare_server_runtime(force_scan=False)

    assert calls == ["init_db", "ensure_runtime_ready"]


def test_acquire_supervisor_lock_uses_fcntl_when_available(monkeypatch):
    calls: list[tuple[int, int]] = []

    class FakeFcntl:
        LOCK_EX = 1
        LOCK_NB = 2
        LOCK_UN = 4

        @staticmethod
        def flock(fd: int, mode: int) -> None:
            calls.append((fd, mode))

    class FakeFile:
        def fileno(self) -> int:
            return 17

    monkeypatch.setattr(cli, "fcntl", FakeFcntl)
    monkeypatch.setattr(cli, "msvcrt", None)

    cli._acquire_supervisor_lock(FakeFile())
    cli._release_supervisor_lock(FakeFile())

    assert calls == [(17, 3), (17, 4)]


def test_acquire_supervisor_lock_uses_msvcrt_when_fcntl_missing(monkeypatch):
    calls: list[tuple[int, int, int]] = []

    class FakeMsvcrt:
        LK_NBLCK = 7
        LK_UNLCK = 8

        @staticmethod
        def locking(fd: int, mode: int, size: int) -> None:
            calls.append((fd, mode, size))

    class FakeFile:
        def __init__(self):
            self.contents = ""
            self.pointer = 0

        def fileno(self) -> int:
            return 21

        def seek(self, offset: int) -> None:
            self.pointer = offset

        def read(self, size: int = -1) -> str:
            if size < 0:
                return self.contents[self.pointer :]
            return self.contents[self.pointer : self.pointer + size]

        def write(self, value: str) -> int:
            self.contents = value
            self.pointer = len(value)
            return len(value)

        def flush(self) -> None:
            return None

    fake_file = FakeFile()

    monkeypatch.setattr(cli, "fcntl", None)
    monkeypatch.setattr(cli, "msvcrt", FakeMsvcrt)

    cli._acquire_supervisor_lock(fake_file)
    cli._release_supervisor_lock(fake_file)

    assert fake_file.contents == "1"
    assert calls == [(21, 7, 1), (21, 8, 1)]


def test_worker_command_args_include_runtime_paths(tmp_path: Path):
    workspace_root = tmp_path / "workspace"
    data_dir = tmp_path / "data"
    db_path = data_dir / "creature.sqlite3"
    workspace_root.mkdir()

    with config.override_runtime_paths(workspace_root=workspace_root, data_dir=data_dir, db_path=db_path):
        args = cli._worker_command_args(bind_mode=cli.SERVE_BIND_MODE_DEFAULT)

    assert args == [
        config.python_bin(),
        "-m",
        "creatureos.cli",
        "--workspace",
        str(workspace_root),
        "--data-dir",
        str(data_dir),
        "--db-path",
        str(db_path),
        "serve-worker",
    ]


def test_supervisor_command_args_include_runtime_paths_and_force_scan(tmp_path: Path):
    workspace_root = tmp_path / "workspace"
    data_dir = tmp_path / "data"
    db_path = data_dir / "creature.sqlite3"
    workspace_root.mkdir()

    with config.override_runtime_paths(workspace_root=workspace_root, data_dir=data_dir, db_path=db_path):
        args = cli._supervisor_command_args(
            bind_mode=cli.SERVE_BIND_MODE_TAILSCALE,
            include_force_scan=True,
        )

    assert args == [
        config.python_bin(),
        "-m",
        "creatureos.cli",
        "--workspace",
        str(workspace_root),
        "--data-dir",
        str(data_dir),
        "--db-path",
        str(db_path),
        "serve",
        "--tailscale",
        "--force-scan",
    ]


def test_wait_for_worker_ready_uses_pid_reported_by_ready_file(monkeypatch):
    class FakeWorker:
        pid = 111

        @staticmethod
        def poll():
            return None

    monotonic_values = iter([0.0, 0.0])
    monkeypatch.setattr(cli.time, "monotonic", lambda: next(monotonic_values))
    monkeypatch.setattr(cli.time, "sleep", lambda seconds: None)
    monkeypatch.setattr(
        cli,
        "_load_server_ready_payload",
        lambda: {
            "status": "ready",
            "source_revision": "rev-1",
            "booted_at": "boot-1",
            "worker_pid": 222,
        },
    )

    ready_pid = cli._wait_for_worker_ready(
        FakeWorker(),
        source_revision="rev-1",
        booted_at="boot-1",
        timeout_seconds=1.0,
    )

    assert ready_pid == 222


def test_terminate_worker_also_terminates_runtime_pid(monkeypatch):
    calls: list[tuple[str, int | float | None]] = []

    class FakeWorker:
        pid = 111

        @staticmethod
        def poll():
            return None

        @staticmethod
        def terminate() -> None:
            calls.append(("terminate", None))

        @staticmethod
        def wait(timeout: float | None = None) -> None:
            calls.append(("wait", timeout))

    monkeypatch.setattr(cli.os, "kill", lambda pid, sig: calls.append((f"kill:{pid}", sig)))

    cli._terminate_worker(FakeWorker(), runtime_pid=222)

    assert calls == [
        ("terminate", None),
        ("wait", cli.SERVER_SHUTDOWN_GRACE_SECONDS),
        ("kill:222", signal.SIGTERM),
    ]
