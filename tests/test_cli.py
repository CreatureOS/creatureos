import errno
from itertools import islice
from pathlib import Path
import signal
import subprocess

import pytest

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


def test_candidate_ports_jump_from_404_to_4040():
    assert list(islice(cli._candidate_ports(404), 4)) == [404, 4040, 4041, 4042]


def test_candidate_ports_increment_from_custom_port():
    assert list(islice(cli._candidate_ports(5050), 3)) == [5050, 5051, 5052]


def test_select_runtime_port_skips_unavailable_ports(monkeypatch):
    attempts: list[tuple[str, int]] = []
    closed_ports: list[int] = []

    class FakeSocket:
        def __init__(self, port: int):
            self.port = port

        def close(self) -> None:
            closed_ports.append(self.port)

    def fake_create_listening_socket(host: str, port: int):
        attempts.append((host, port))
        if port in {404, 4040}:
            raise OSError(errno.EADDRINUSE, f"{port} unavailable")
        return FakeSocket(port)

    monkeypatch.setattr(cli, "_create_listening_socket", fake_create_listening_socket)
    monkeypatch.setattr(cli, "_bind_hosts", lambda *, bind_mode: ["127.0.0.1"])

    selected_port = cli._select_runtime_port(
        bind_mode=cli.SERVE_BIND_MODE_DEFAULT,
        requested_port=404,
    )

    assert selected_port == 4041
    assert attempts == [
        ("127.0.0.1", 404),
        ("127.0.0.1", 4040),
        ("127.0.0.1", 4041),
    ]
    assert closed_ports == [4041]


def test_select_runtime_port_raises_non_conflict_bind_errors(monkeypatch):
    attempts: list[tuple[str, int]] = []

    def fake_create_listening_socket(host: str, port: int):
        attempts.append((host, port))
        raise OSError(errno.EADDRNOTAVAIL, "bad bind host")

    monkeypatch.setattr(cli, "_create_listening_socket", fake_create_listening_socket)
    monkeypatch.setattr(cli, "_bind_hosts", lambda *, bind_mode: ["203.0.113.7"])

    with pytest.raises(OSError, match="bad bind host"):
        cli._select_runtime_port(
            bind_mode=cli.SERVE_BIND_MODE_DEFAULT,
            requested_port=404,
        )

    assert attempts == [("203.0.113.7", 404)]


def test_select_runtime_port_falls_back_when_privileged_port_is_denied(monkeypatch):
    attempts: list[tuple[str, int]] = []
    closed_ports: list[int] = []

    class FakeSocket:
        def __init__(self, port: int):
            self.port = port

        def close(self) -> None:
            closed_ports.append(self.port)

    def fake_create_listening_socket(host: str, port: int):
        attempts.append((host, port))
        if port == 404:
            raise OSError(errno.EACCES, "permission denied")
        return FakeSocket(port)

    monkeypatch.setattr(cli, "_create_listening_socket", fake_create_listening_socket)
    monkeypatch.setattr(cli, "_bind_hosts", lambda *, bind_mode: ["127.0.0.1"])

    selected_port = cli._select_runtime_port(
        bind_mode=cli.SERVE_BIND_MODE_DEFAULT,
        requested_port=404,
    )

    assert selected_port == 4040
    assert attempts == [
        ("127.0.0.1", 404),
        ("127.0.0.1", 4040),
    ]
    assert closed_ports == [4040]


def test_reconfigure_runtime_port_after_restart_reports_port_changes(monkeypatch, capsys):
    monkeypatch.setattr(
        cli,
        "_configure_runtime_port",
        lambda *, bind_mode: (404, 4041),
    )

    requested_port, selected_port = cli._reconfigure_runtime_port_after_restart(
        bind_mode=cli.SERVE_BIND_MODE_DEFAULT,
        previous_port=404,
    )

    captured = capsys.readouterr()
    assert (requested_port, selected_port) == (404, 4041)
    assert "Port 404 unavailable; using 4041 instead." in captured.out


def test_reconfigure_runtime_port_after_restart_reports_return_to_requested_port(monkeypatch, capsys):
    monkeypatch.setattr(
        cli,
        "_configure_runtime_port",
        lambda *, bind_mode: (404, 404),
    )

    requested_port, selected_port = cli._reconfigure_runtime_port_after_restart(
        bind_mode=cli.SERVE_BIND_MODE_DEFAULT,
        previous_port=4041,
    )

    captured = capsys.readouterr()
    assert (requested_port, selected_port) == (404, 404)
    assert "Port 4041 cleared; returning to 404." in captured.out


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
