from __future__ import annotations

import json
import selectors
import subprocess
import time
from dataclasses import dataclass
from typing import Any, Callable

from . import config


class CodexCommandError(RuntimeError):
    """Raised when a Codex CLI command fails."""


class CodexTimeoutError(CodexCommandError):
    """Raised when a Codex CLI command exceeds the configured timeout."""


@dataclass
class CodexRunResult:
    thread_id: str | None
    final_text: str
    stdout_lines: list[str]


def _base_command(
    *,
    workdir: str,
    model: str | None = None,
    reasoning_effort: str | None = None,
) -> list[str]:
    return [
        config.codex_bin(),
        "exec",
        "--json",
        "--skip-git-repo-check",
        "-m",
        str(model or config.creature_model()),
        "-c",
        f'model_reasoning_effort="{str(reasoning_effort or config.creature_reasoning_effort())}"',
        "-C",
        workdir,
    ]


def _handle_event_line(
    raw_line: str,
    *,
    stdout_lines: list[str],
    on_event: Callable[[dict[str, Any]], None] | None,
    state: dict[str, str | None],
) -> None:
    line = raw_line.strip()
    if not line:
        return
    stdout_lines.append(line)
    try:
        event = json.loads(line)
    except json.JSONDecodeError:
        if on_event is not None:
            on_event({"type": "log", "message": line, "_raw_line": line})
        return
    if on_event is not None:
        payload = dict(event)
        payload["_raw_line"] = line
        on_event(payload)
    if event.get("type") == "thread.started":
        state["thread_id"] = str(event.get("thread_id") or "").strip() or state["thread_id"]
    item = event.get("item") or {}
    if event.get("type") == "item.completed" and item.get("type") == "agent_message":
        state["final_text"] = str(item.get("text") or "")


def _invoke(
    command: list[str],
    *,
    prompt: str,
    workdir: str,
    timeout_seconds: int | None = None,
    on_event: Callable[[dict[str, Any]], None] | None = None,
) -> CodexRunResult:
    process = subprocess.Popen(
        command + [prompt],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        cwd=workdir,
        bufsize=1,
    )
    if process.stdout is None:
        process.kill()
        process.wait()
        raise CodexCommandError("Codex process did not expose a stdout pipe.")

    selector = selectors.DefaultSelector()
    selector.register(process.stdout, selectors.EVENT_READ)
    thread_id: str | None = None
    final_text = ""
    stdout_lines: list[str] = []
    state: dict[str, str | None] = {"thread_id": None, "final_text": None}
    started_at = time.monotonic()
    try:
        while True:
            remaining: float | None = None
            if timeout_seconds is not None:
                remaining = timeout_seconds - (time.monotonic() - started_at)
                if remaining <= 0:
                    process.kill()
                    process.wait()
                    minutes = timeout_seconds // 60
                    if timeout_seconds % 60:
                        duration = f"{timeout_seconds} seconds"
                    elif minutes == 1:
                        duration = "1 minute"
                    else:
                        duration = f"{minutes} minutes"
                    raise CodexTimeoutError(f"Codex run timed out after {duration}.")
            ready = selector.select(timeout=remaining if remaining is not None else 0.5)
            if not ready:
                if process.poll() is not None:
                    break
                continue
            for key, _ in ready:
                line = key.fileobj.readline()
                if not line:
                    selector.unregister(key.fileobj)
                    continue
                _handle_event_line(
                    line,
                    stdout_lines=stdout_lines,
                    on_event=on_event,
                    state=state,
                )
            if process.poll() is not None and not selector.get_map():
                break
    finally:
        try:
            selector.close()
        except Exception:
            pass
        try:
            process.stdout.close()
        except Exception:
            pass

    returncode = process.wait()
    stderr_text = "\n".join(line for line in stdout_lines if line.startswith("ERROR"))
    thread_id = str(state["thread_id"] or "").strip() or None
    final_text = str(state["final_text"] or "")
    if returncode != 0:
        detail = stderr_text or "\n".join(stdout_lines[-12:]) or "no stderr or stdout captured"
        raise CodexCommandError(f"Codex exited with status {returncode}: {detail[:600]}")
    if not final_text:
        detail = stderr_text or "\n".join(stdout_lines[-12:]) or "<empty>"
        raise CodexCommandError(f"Codex returned no final assistant message. stderr={detail[:600]}")
    return CodexRunResult(thread_id=thread_id, final_text=final_text, stdout_lines=stdout_lines)


def start_thread(
    *,
    workdir: str,
    prompt: str,
    model: str | None = None,
    reasoning_effort: str | None = None,
    sandbox_mode: str = "read-only",
    timeout_seconds: int | None = None,
    on_event: Callable[[dict[str, Any]], None] | None = None,
) -> CodexRunResult:
    command = _base_command(workdir=workdir, model=model, reasoning_effort=reasoning_effort) + ["-s", sandbox_mode]
    return _invoke(
        command,
        prompt=prompt,
        workdir=workdir,
        timeout_seconds=config.creature_timeout_seconds(sandbox_mode) if timeout_seconds is None else timeout_seconds,
        on_event=on_event,
    )


def resume_thread(
    *,
    workdir: str,
    thread_id: str,
    prompt: str,
    model: str | None = None,
    reasoning_effort: str | None = None,
    sandbox_mode: str = "read-only",
    timeout_seconds: int | None = None,
    on_event: Callable[[dict[str, Any]], None] | None = None,
) -> CodexRunResult:
    command = _base_command(workdir=workdir, model=model, reasoning_effort=reasoning_effort) + ["-s", sandbox_mode, "resume", thread_id]
    return _invoke(
        command,
        prompt=prompt,
        workdir=workdir,
        timeout_seconds=config.creature_timeout_seconds(sandbox_mode) if timeout_seconds is None else timeout_seconds,
        on_event=on_event,
    )
