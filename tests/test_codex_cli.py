from __future__ import annotations

import io
import subprocess

import pytest

from creatureos import codex_cli


def test_start_thread_wraps_process_launch_failures(monkeypatch):
    def _explode(*args, **kwargs):
        raise PermissionError("[WinError 5] Access is denied")

    monkeypatch.setattr(subprocess, "Popen", _explode)

    with pytest.raises(codex_cli.CodexCommandError) as excinfo:
        codex_cli.start_thread(
            workdir="C:\\",
            prompt="Reply with READY only.",
        )

    message = str(excinfo.value)
    assert "Failed to launch Codex command" in message
    assert "Access is denied" in message


def test_invoke_parses_json_events_without_selector_support(monkeypatch):
    captured = {"args": None, "stdin": b""}

    class FakeStdin:
        def write(self, value):
            captured["stdin"] += value
            return len(value)

        def flush(self):
            return None

        def close(self):
            return None

    class FakeProcess:
        def __init__(self):
            self.stdin = FakeStdin()
            self.stdout = io.BytesIO(
                b'{"type":"thread.started","thread_id":"thread-123"}\n'
                b'{"type":"item.completed","item":{"type":"agent_message","text":"READY"}}\n'
            )
            self.returncode = 0

        def poll(self):
            return self.returncode

        def wait(self):
            return self.returncode

        def kill(self):
            self.returncode = -9

    def _fake_popen(args, **kwargs):
        captured["args"] = list(args)
        return FakeProcess()

    monkeypatch.setattr(subprocess, "Popen", _fake_popen)

    result = codex_cli._invoke(
        ["codex"],
        prompt="Reply with READY only.",
        workdir="C:\\",
    )

    assert captured["args"] == ["codex", "-"]
    assert captured["stdin"] == b"Reply with READY only."
    assert result.thread_id == "thread-123"
    assert result.final_text == "READY"


def test_invoke_writes_utf8_prompt_bytes_and_strips_control_chars(monkeypatch):
    captured = {"stdin": b""}

    class FakeStdin:
        def write(self, value):
            captured["stdin"] += value
            return len(value)

        def flush(self):
            return None

        def close(self):
            return None

    class FakeProcess:
        def __init__(self):
            self.stdin = FakeStdin()
            self.stdout = io.BytesIO(
                b'{"type":"item.completed","item":{"type":"agent_message","text":"READY"}}\n'
            )
            self.returncode = 0

        def poll(self):
            return self.returncode

        def wait(self):
            return self.returncode

        def kill(self):
            self.returncode = -9

    monkeypatch.setattr(subprocess, "Popen", lambda *args, **kwargs: FakeProcess())

    codex_cli._invoke(
        ["codex"],
        prompt="Line one\x00\r\nLine two…",
        workdir="C:\\",
    )

    assert captured["stdin"] == "Line one\nLine two…".encode("utf-8")
