from __future__ import annotations

import argparse
import asyncio
import ipaddress
import json
import os
import signal
import socket
import subprocess
import time
from datetime import datetime, timezone
from urllib import request as urlrequest

import fcntl

import uvicorn

from . import config
from . import service
from . import storage

SERVER_WATCH_INTERVAL_SECONDS = 1.0
SERVER_RESTART_BACKOFF_SECONDS = 1.0
SERVER_SHUTDOWN_GRACE_SECONDS = 5.0
SERVER_READY_TIMEOUT_SECONDS = 45.0
SERVER_READY_POLL_INTERVAL_SECONDS = 0.5
SERVER_HEALTH_CHECK_INTERVAL_SECONDS = 5.0
SERVER_HEALTH_FAILURE_LIMIT = 3
SERVER_HEALTH_REQUEST_TIMEOUT_SECONDS = 5.0
SERVE_BIND_MODE_ENV = "CREATURE_OS_SERVE_BIND_MODE"
SERVE_BIND_MODE_DEFAULT = "default"
SERVE_BIND_MODE_TAILSCALE = "tailscale"
_TAILSCALE_IPV4_NETWORK = ipaddress.ip_network("100.64.0.0/10")


class _HiddenSubparsersAction(argparse._SubParsersAction):
    def add_parser(self, name, **kwargs):
        hidden = bool(kwargs.pop("hidden", False))
        parser = super().add_parser(name, **kwargs)
        if hidden and self._choices_actions:
            self._choices_actions.pop()
        return parser


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="CreatureOS runtime")
    parser.register("action", "parsers", _HiddenSubparsersAction)
    parser.add_argument(
        "--workspace",
        default="",
        help="Workspace directory CreatureOS should inspect. Defaults to the current directory.",
    )
    parser.add_argument(
        "--data-dir",
        default="",
        help="Runtime state directory. Defaults to a user-local CreatureOS state directory.",
    )
    parser.add_argument(
        "--db-path",
        default="",
        help="SQLite path override. Defaults to <data-dir>/creature_os.sqlite3.",
    )
    subparsers = parser.add_subparsers(
        dest="command",
        required=True,
        metavar="{init-db,run-creature,run-due,create-creature,send-message,create-conversation,spawn-conversation,delete-creature,serve}",
    )

    subparsers.add_parser("init-db")
    run_creature = subparsers.add_parser("run-creature")
    run_creature.add_argument("slug")
    run_creature.add_argument("--trigger", default="manual")
    run_creature.add_argument("--force-message", action="store_true")
    run_creature.add_argument("--conversation-id", type=int)
    run_creature.add_argument("--allow-code-changes", action="store_true")

    run_due = subparsers.add_parser("run-due")
    run_due.add_argument("--force-message", action="store_true")

    create_creature = subparsers.add_parser("create-creature")
    create_creature.add_argument("display_name")
    create_creature.add_argument("concern")
    create_creature.add_argument("--public-prompt", default="")
    create_creature.add_argument("--slug", default="")

    send_message = subparsers.add_parser("send-message")
    send_message.add_argument("slug")
    send_message.add_argument("conversation_id", type=int)
    send_message.add_argument("body")

    create_conversation = subparsers.add_parser("create-conversation")
    create_conversation.add_argument("slug")
    create_conversation.add_argument("--title", default="")

    spawn_conversation = subparsers.add_parser("spawn-conversation")
    spawn_conversation.add_argument("slug")
    spawn_conversation.add_argument("run_id", type=int)

    delete_creature = subparsers.add_parser("delete-creature")
    delete_creature.add_argument("slug")

    serve = subparsers.add_parser("serve")
    serve.add_argument("--force-scan", action="store_true", help="Clear the cached onboarding environment scan before starting the server")
    serve.add_argument(
        "--tailscale",
        action="store_true",
        help="Serve on localhost plus the detected Tailscale IPv4. Falls back to localhost if Tailscale is unavailable.",
    )
    serve_worker = subparsers.add_parser("serve-worker", help=argparse.SUPPRESS, hidden=True)
    serve_worker.add_argument("--force-scan", action="store_true", help=argparse.SUPPRESS)
    serve_worker.add_argument("--tailscale", action="store_true", help=argparse.SUPPRESS)
    return parser


def _normalize_bind_mode(value: str | None) -> str:
    cleaned = str(value or "").strip().lower()
    return SERVE_BIND_MODE_TAILSCALE if cleaned == SERVE_BIND_MODE_TAILSCALE else SERVE_BIND_MODE_DEFAULT


def _detect_tailscale_ipv4() -> str:
    override = os.getenv("CREATURE_OS_TAILSCALE_IP", "").strip()
    candidates: list[str] = [override] if override else []
    if not candidates:
        try:
            result = subprocess.run(
                ["tailscale", "ip", "-4"],
                check=False,
                capture_output=True,
                text=True,
                timeout=2,
            )
            candidates = [line.strip() for line in result.stdout.splitlines() if line.strip()]
        except Exception:
            candidates = []
    for candidate in candidates:
        try:
            ip = ipaddress.ip_address(candidate)
        except ValueError:
            continue
        if ip.version == 4 and ip in _TAILSCALE_IPV4_NETWORK:
            return str(ip)
    return ""


def _server_display_urls(*, bind_mode: str) -> list[str]:
    normalized_mode = _normalize_bind_mode(bind_mode)
    urls = [f"http://127.0.0.1:{config.port()}/"]
    if normalized_mode == SERVE_BIND_MODE_TAILSCALE:
        tailscale_ip = _detect_tailscale_ipv4()
        if tailscale_ip:
            urls.append(f"http://{tailscale_ip}:{config.port()}/")
    return urls


def _write_server_pid(
    *,
    supervisor_pid: int,
    source_revision: str,
    started_at: str,
    worker_pid: int | None = None,
    bind_mode: str = SERVE_BIND_MODE_DEFAULT,
) -> None:
    urls = _server_display_urls(bind_mode=bind_mode)
    config.server_pid_path().write_text(
        json.dumps(
            {
                "pid": supervisor_pid,
                "worker_pid": int(worker_pid or 0),
                "url": urls[0] if urls else config.app_url(),
                "urls": urls,
                "bind_mode": _normalize_bind_mode(bind_mode),
                "started_at": started_at,
                "source_revision": source_revision,
                "static_version": config.static_version(source_revision),
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )


def _remove_server_pid_if_owned(owner_pid: int) -> None:
    pid_path = config.server_pid_path()
    if not pid_path.exists():
        return
    try:
        payload = json.loads(pid_path.read_text(encoding="utf-8"))
    except Exception:
        payload = {}
    if int(payload.get("pid") or 0) == owner_pid:
        pid_path.unlink(missing_ok=True)


def _terminate_worker(worker: subprocess.Popen[bytes] | None) -> None:
    if worker is None or worker.poll() is not None:
        return
    try:
        worker.terminate()
        worker.wait(timeout=SERVER_SHUTDOWN_GRACE_SECONDS)
        return
    except subprocess.TimeoutExpired:
        pass
    except ProcessLookupError:
        return
    try:
        worker.kill()
        worker.wait(timeout=1)
    except Exception:
        return


def _launch_server_worker(source_revision: str, *, bind_mode: str) -> tuple[subprocess.Popen[bytes], str]:
    booted_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    env = os.environ.copy()
    env["CREATURE_OS_SOURCE_REVISION"] = source_revision
    env["CREATURE_OS_STATIC_VERSION"] = config.static_version(source_revision)
    env["CREATURE_OS_SERVER_BOOTED_AT"] = booted_at
    env["CREATURE_OS_SUPERVISOR_PID"] = str(os.getpid())
    env[SERVE_BIND_MODE_ENV] = _normalize_bind_mode(bind_mode)
    worker_args = [config.python_bin(), "-m", "creatureos.cli", "serve-worker"]
    if _normalize_bind_mode(bind_mode) == SERVE_BIND_MODE_TAILSCALE:
        worker_args.append("--tailscale")
    worker = subprocess.Popen(
        worker_args,
        cwd=str(config.package_root().parent),
        env=env,
    )
    return worker, booted_at


def _create_listening_socket(host: str) -> socket.socket:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((host, config.port()))
    sock.listen(socket.SOMAXCONN)
    sock.set_inheritable(True)
    return sock


def _server_health_url() -> str:
    return f"http://127.0.0.1:{config.port()}/healthz"


def _fetch_server_health(*, timeout_seconds: float = SERVER_HEALTH_REQUEST_TIMEOUT_SECONDS) -> dict[str, object] | None:
    try:
        with urlrequest.urlopen(_server_health_url(), timeout=timeout_seconds) as response:
            if int(getattr(response, "status", 200) or 200) != 200:
                return None
            payload = json.loads(response.read().decode("utf-8"))
    except Exception:
        return None
    if str(payload.get("status") or "").strip().lower() != "ok":
        return None
    return payload


def _remove_server_ready_file() -> None:
    config.server_ready_path().unlink(missing_ok=True)


def _load_server_ready_payload() -> dict[str, object] | None:
    path = config.server_ready_path()
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _server_ready_matches(
    payload: dict[str, object] | None,
    *,
    source_revision: str,
    worker_pid: int | None = None,
    booted_at: str = "",
) -> bool:
    if not isinstance(payload, dict):
        return False
    if str(payload.get("status") or "").strip().lower() != "ready":
        return False
    if str(payload.get("source_revision") or "").strip() != str(source_revision).strip():
        return False
    if worker_pid is not None and int(payload.get("worker_pid") or 0) != int(worker_pid):
        return False
    if booted_at and str(payload.get("booted_at") or "").strip() != str(booted_at).strip():
        return False
    return True


def _server_health_matches(
    payload: dict[str, object] | None,
    *,
    source_revision: str,
    worker_pid: int | None = None,
    booted_at: str = "",
) -> bool:
    if not isinstance(payload, dict):
        return False
    if str(payload.get("source_revision") or "").strip() != str(source_revision).strip():
        return False
    if worker_pid is not None and int(payload.get("worker_pid") or 0) != int(worker_pid):
        return False
    if booted_at and str(payload.get("booted_at") or "").strip() != str(booted_at).strip():
        return False
    return True


def _wait_for_worker_ready(
    worker: subprocess.Popen[bytes],
    *,
    source_revision: str,
    booted_at: str,
    timeout_seconds: float = SERVER_READY_TIMEOUT_SECONDS,
) -> bool:
    deadline = time.monotonic() + max(1.0, timeout_seconds)
    while time.monotonic() < deadline:
        if worker.poll() is not None:
            return False
        payload = _load_server_ready_payload()
        if _server_ready_matches(payload, source_revision=source_revision, worker_pid=worker.pid, booted_at=booted_at):
            return True
        time.sleep(SERVER_READY_POLL_INTERVAL_SECONDS)
    return False


def _run_server_worker(*, force_scan: bool = False, bind_mode: str = SERVE_BIND_MODE_DEFAULT) -> int:
    if force_scan:
        storage.delete_meta(service.ONBOARDING_ENVIRONMENT_KEY)
        storage.delete_meta(service.ONBOARDING_BRIEFING_KEY)
    normalized_mode = _normalize_bind_mode(bind_mode)
    if normalized_mode == SERVE_BIND_MODE_TAILSCALE:
        tailscale_ip = _detect_tailscale_ipv4()
        listen_hosts = ["127.0.0.1"]
        if tailscale_ip and tailscale_ip not in listen_hosts:
            listen_hosts.append(tailscale_ip)
        sockets: list[socket.socket] = []
        try:
            for host in listen_hosts:
                sockets.append(_create_listening_socket(host))
            print("CreatureOS dual-bind mode:", flush=True)
            print(f"  Local: http://127.0.0.1:{config.port()}", flush=True)
            if tailscale_ip:
                print(f"  Tailscale: http://{tailscale_ip}:{config.port()}", flush=True)
            else:
                print("  Tailscale: not detected, staying local-only", flush=True)
            uvicorn_config = uvicorn.Config(
                "creatureos.web:app",
                host="127.0.0.1",
                port=config.port(),
                reload=False,
                access_log=False,
                log_config=None,
            )
            server = uvicorn.Server(uvicorn_config)
            return 0 if asyncio.run(server.serve(sockets=sockets)) is not False else 1
        finally:
            for sock in sockets:
                try:
                    sock.close()
                except Exception:
                    pass
    uvicorn.run(
        "creatureos.web:app",
        host=config.host(),
        port=config.port(),
        reload=False,
        access_log=False,
        log_config=None,
    )
    return 0


def _run_server_supervisor(*, force_scan: bool = False, bind_mode: str = SERVE_BIND_MODE_DEFAULT) -> int:
    lock_path = config.server_lock_path()
    pid_path = config.server_pid_path()
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_file = lock_path.open("a+", encoding="utf-8")
    try:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        existing_pid = ""
        if pid_path.exists():
            try:
                existing_pid = str(json.loads(pid_path.read_text(encoding="utf-8")).get("pid") or "").strip()
            except Exception:
                existing_pid = ""
        detail = f" (pid {existing_pid})" if existing_pid else ""
        print(f"CreatureOS server is already running for this habitat{detail}.", flush=True)
        try:
            lock_file.close()
        except Exception:
            pass
        return 1

    worker: subprocess.Popen[bytes] | None = None
    normalized_bind_mode = _normalize_bind_mode(bind_mode)
    os.environ[SERVE_BIND_MODE_ENV] = normalized_bind_mode
    current_revision = config.server_source_revision()
    worker_started_at = ""
    state = {"shutdown": False, "reload": False}
    launch_force_scan = bool(force_scan)
    last_health_probe_at = 0.0
    consecutive_health_failures = 0
    launch_count = 0

    def _supervisor_args(*, include_force_scan: bool = False) -> list[str]:
        args = [config.python_bin(), "-m", "creatureos.cli", "serve"]
        if normalized_bind_mode == SERVE_BIND_MODE_TAILSCALE:
            args.append("--tailscale")
        if include_force_scan:
            args.append("--force-scan")
        return args

    def _handle_shutdown(signum: int, frame) -> None:  # type: ignore[no-untyped-def]
        state["shutdown"] = True
        _terminate_worker(worker)

    def _handle_reload(signum: int, frame) -> None:  # type: ignore[no-untyped-def]
        state["reload"] = True

    def _reexec_supervisor(*, include_force_scan: bool = False) -> None:
        os.execvpe(
            config.python_bin(),
            _supervisor_args(include_force_scan=include_force_scan),
            os.environ.copy(),
        )

    signal.signal(signal.SIGTERM, _handle_shutdown)
    signal.signal(signal.SIGINT, _handle_shutdown)
    if hasattr(signal, "SIGHUP"):
        signal.signal(signal.SIGHUP, _handle_reload)

    try:
        print("404: Humans not found.", flush=True)
        print(f"Creatures awakening at {config.app_url().rstrip('/')}", flush=True)
        print(f"Working root: {config.workspace_root()} ({config.workspace_root_source()})", flush=True)
        print(f"Data dir: {config.data_dir()} ({config.data_dir_source()})", flush=True)
        print(f"Database: {config.db_path()}", flush=True)
        if config.workspace_root_source() == "cwd":
            print(
                "Tip: pass --workspace PATH or set CREATURE_OS_WORKSPACE_ROOT to keep creature file work anchored to a predictable directory.",
                flush=True,
            )
        print("Onboarding scans look across likely work directories on this machine.", flush=True)
        if normalized_bind_mode == SERVE_BIND_MODE_TAILSCALE:
            for url in _server_display_urls(bind_mode=normalized_bind_mode):
                print(f"Listening on {url.rstrip('/')}", flush=True)
        _remove_server_ready_file()
        if launch_force_scan:
            storage.delete_meta(service.ONBOARDING_ENVIRONMENT_KEY)
            storage.delete_meta(service.ONBOARDING_BRIEFING_KEY)
        while not state["shutdown"]:
            if state["reload"]:
                _terminate_worker(worker)
                print("Reload requested; restarting supervisor.", flush=True)
                _reexec_supervisor(include_force_scan=launch_force_scan)

            latest_revision = config.server_source_revision()
            if latest_revision != current_revision:
                _terminate_worker(worker)
                print("Source revision changed; restarting supervisor.", flush=True)
                _reexec_supervisor(include_force_scan=launch_force_scan)

            if worker is None or worker.poll() is not None:
                if state["shutdown"]:
                    break
                if worker is not None:
                    print(
                        f"Worker exited with code {worker.returncode}; restarting in {SERVER_RESTART_BACKOFF_SECONDS:.1f}s.",
                        flush=True,
                    )
                    time.sleep(SERVER_RESTART_BACKOFF_SECONDS)
                current_revision = config.server_source_revision()
                _remove_server_ready_file()
                worker, worker_started_at = _launch_server_worker(current_revision, bind_mode=normalized_bind_mode)
                launch_count += 1
                print(
                    f"Launching worker #{launch_count} ({normalized_bind_mode}) for revision {current_revision[:12]}.",
                    flush=True,
                )
                if not _wait_for_worker_ready(worker, source_revision=current_revision, booted_at=worker_started_at):
                    print(
                        f"Worker #{launch_count} failed to become ready within {SERVER_READY_TIMEOUT_SECONDS:.0f}s.",
                        flush=True,
                    )
                    _terminate_worker(worker)
                    worker = None
                    continue
                launch_force_scan = False
                consecutive_health_failures = 0
                last_health_probe_at = time.monotonic()
                _write_server_pid(
                    supervisor_pid=os.getpid(),
                    worker_pid=worker.pid,
                    source_revision=current_revision,
                    started_at=worker_started_at,
                    bind_mode=normalized_bind_mode,
                )
                continue

            now = time.monotonic()
            if now - last_health_probe_at >= SERVER_HEALTH_CHECK_INTERVAL_SECONDS:
                last_health_probe_at = now
                payload = _fetch_server_health()
                if _server_health_matches(payload, source_revision=current_revision, worker_pid=worker.pid, booted_at=worker_started_at):
                    consecutive_health_failures = 0
                else:
                    consecutive_health_failures += 1
                    print(
                        f"Health probe failed ({consecutive_health_failures}/{SERVER_HEALTH_FAILURE_LIMIT}); waiting for recovery.",
                        flush=True,
                    )
                    if consecutive_health_failures >= SERVER_HEALTH_FAILURE_LIMIT:
                        print("Worker stayed unhealthy; restarting it.", flush=True)
                        _terminate_worker(worker)
                        worker = None
                        consecutive_health_failures = 0
                        continue

            time.sleep(SERVER_WATCH_INTERVAL_SECONDS)
        return 0
    finally:
        _terminate_worker(worker)
        _remove_server_ready_file()
        try:
            _remove_server_pid_if_owned(os.getpid())
        except Exception:
            pass
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
        except Exception:
            pass
        try:
            lock_file.close()
        except Exception:
            pass


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    with config.override_runtime_paths(
        workspace_root=args.workspace or None,
        data_dir=args.data_dir or None,
        db_path=args.db_path or None,
    ):
        if args.command == "init-db":
            storage.init_db()
            print(config.db_path())
            return 0
        if args.command == "run-creature":
            result = service.run_creature(
                args.slug,
                trigger_type=args.trigger,
                force_message=bool(args.force_message),
                conversation_id=args.conversation_id,
                allow_code_changes=bool(args.allow_code_changes),
            )
            print(json.dumps(result, indent=2))
            return 0
        if args.command == "run-due":
            result = service.run_due_creatures(force_message=bool(args.force_message))
            print(json.dumps(result, indent=2))
            return 0
        if args.command == "create-creature":
            result = service.create_creature(
                display_name=args.display_name,
                concern=args.concern,
                public_prompt=args.public_prompt,
                slug=args.slug or None,
            )
            print(json.dumps(result, indent=2))
            return 0
        if args.command == "send-message":
            result = service.send_user_message(args.slug, args.conversation_id, args.body)
            print(json.dumps(result, indent=2))
            return 0
        if args.command == "create-conversation":
            result = service.create_conversation(args.slug, title=args.title or None)
            print(json.dumps(result, indent=2))
            return 0
        if args.command == "spawn-conversation":
            result = service.spawn_conversation_from_run(args.slug, args.run_id)
            print(json.dumps(result, indent=2))
            return 0
        if args.command == "delete-creature":
            service.delete_creature(args.slug)
            print(json.dumps({"deleted": args.slug}, indent=2))
            return 0
        if args.command == "serve":
            bind_mode = SERVE_BIND_MODE_TAILSCALE if bool(args.tailscale) else _normalize_bind_mode(os.getenv(SERVE_BIND_MODE_ENV))
            return _run_server_supervisor(force_scan=bool(args.force_scan), bind_mode=bind_mode)
        if args.command == "serve-worker":
            bind_mode = SERVE_BIND_MODE_TAILSCALE if bool(args.tailscale) else _normalize_bind_mode(os.getenv(SERVE_BIND_MODE_ENV))
            return _run_server_worker(force_scan=bool(args.force_scan), bind_mode=bind_mode)
        parser.error(f"Unknown command: {args.command}")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
