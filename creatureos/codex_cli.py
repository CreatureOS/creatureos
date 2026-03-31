from __future__ import annotations

import json
import queue
import subprocess
import threading
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


def _sanitize_prompt_text(prompt: str) -> str:
    normalized = str(prompt or "").replace("\r\n", "\n").replace("\r", "\n")
    return "".join(char for char in normalized if char in {"\n", "\t"} or (ord(char) >= 32 and char != "\x7f"))


def _invoke(
    command: list[str],
    *,
    prompt: str,
    workdir: str,
    timeout_seconds: int | None = None,
    on_event: Callable[[dict[str, Any]], None] | None = None,
) -> CodexRunResult:
    prompt_bytes = _sanitize_prompt_text(prompt).encode("utf-8")
    try:
        process = subprocess.Popen(
            command + ["-"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            cwd=workdir,
            bufsize=0,
        )
    except OSError as exc:
        executable = str(command[0] if command else config.codex_bin()).strip() or config.codex_bin()
        raise CodexCommandError(f"Failed to launch Codex command '{executable}': {exc}") from exc
    if process.stdin is None:
        process.kill()
        process.wait()
        raise CodexCommandError("Codex process did not expose a stdin pipe.")
    if process.stdout is None:
        process.kill()
        process.wait()
        raise CodexCommandError("Codex process did not expose a stdout pipe.")

    thread_id: str | None = None
    final_text = ""
    stdout_lines: list[str] = []
    state: dict[str, str | None] = {"thread_id": None, "final_text": None}
    started_at = time.monotonic()
    stdout_queue: queue.Queue[bytes | None] = queue.Queue()

    def _drain_stdout() -> None:
        try:
            for line in process.stdout:
                stdout_queue.put(line)
        finally:
            stdout_queue.put(None)

    def _feed_stdin() -> None:
        try:
            process.stdin.write(prompt_bytes)
            process.stdin.flush()
        finally:
            try:
                process.stdin.close()
            except Exception:
                pass

    stdin_thread = threading.Thread(target=_feed_stdin, daemon=True)
    stdin_thread.start()
    stdout_thread = threading.Thread(target=_drain_stdout, daemon=True)
    stdout_thread.start()
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

            wait_timeout = 0.5 if remaining is None else max(0.0, min(0.5, remaining))
            try:
                line = stdout_queue.get(timeout=wait_timeout)
            except queue.Empty:
                if process.poll() is not None:
                    break
                continue

            if line is None:
                if process.poll() is not None:
                    break
                continue

            decoded_line = line.decode("utf-8", errors="replace")
            _handle_event_line(
                decoded_line,
                stdout_lines=stdout_lines,
                on_event=on_event,
                state=state,
            )
            if process.poll() is not None and stdout_queue.empty() and not stdout_thread.is_alive():
                break
    finally:
        stdin_thread.join(timeout=1)
        try:
            process.stdout.close()
        except Exception:
            pass
        stdout_thread.join(timeout=1)

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
