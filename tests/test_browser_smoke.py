from __future__ import annotations

import os
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path
from urllib import request as urlrequest

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
PYTHON_BIN = sys.executable


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _playwright_available() -> bool:
    if shutil.which("node") is None:
        return False
    result = subprocess.run(
        [
            "node",
            "-e",
            "require('playwright')",
        ],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
    )
    return result.returncode == 0


def _wait_for_health(url: str, *, timeout_seconds: float = 30.0) -> None:
    deadline = time.time() + timeout_seconds
    last_error = "server did not start"
    while time.time() < deadline:
        try:
            with urlrequest.urlopen(url, timeout=1.5) as response:
                if int(getattr(response, "status", 200) or 200) == 200:
                    return
        except Exception as exc:  # pragma: no cover - exercised only in failure cases
            last_error = str(exc)
        time.sleep(0.25)
    raise RuntimeError(last_error)


@pytest.mark.browser
@pytest.mark.skipif(
    os.getenv("CREATURE_OS_RUN_BROWSER_SMOKE", "").strip() != "1",
    reason="Set CREATURE_OS_RUN_BROWSER_SMOKE=1 to run browser smoke tests.",
)
def test_browser_smoke_loads_ecosystem_chooser(tmp_path):
    if not _playwright_available():
        pytest.skip("Playwright is not available in this environment.")

    port = _free_port()
    workspace_root = tmp_path / "workspace"
    data_dir = tmp_path / "browser-smoke-data"
    db_path = data_dir / "browser-smoke.sqlite3"
    workspace_root.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env.update(
        {
            "CREATURE_OS_WORKSPACE_ROOT": str(workspace_root),
            "CREATURE_OS_DATA_DIR": str(data_dir),
            "CREATURE_OS_DB_PATH": str(db_path),
            "CREATURE_OS_HOST": "127.0.0.1",
            "CREATURE_OS_PUBLIC_HOST": "127.0.0.1",
            "CREATURE_OS_PORT": str(port),
        }
    )
    server = subprocess.Popen(
        [PYTHON_BIN, "-m", "creatureos.cli", "serve-worker"],
        cwd=str(REPO_ROOT),
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        _wait_for_health(f"http://127.0.0.1:{port}/healthz")
        subprocess.run(
            [
                "node",
                str(REPO_ROOT / "scripts" / "browser_smoke.js"),
                f"http://127.0.0.1:{port}/",
                "Choose an Ecosystem",
            ],
            cwd=str(REPO_ROOT),
            check=True,
        )
    finally:
        server.terminate()
        try:
            server.wait(timeout=5)
        except subprocess.TimeoutExpired:  # pragma: no cover
            server.kill()
