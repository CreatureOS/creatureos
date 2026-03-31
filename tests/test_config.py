from __future__ import annotations

import sys
from pathlib import Path

from creatureos import config


def test_python_bin_defaults_to_current_interpreter(monkeypatch):
    monkeypatch.delenv("CREATURE_OS_PYTHON_BIN", raising=False)
    monkeypatch.delenv("PYTHON_BIN", raising=False)

    assert config.python_bin() == sys.executable


def test_codex_bin_prefers_windows_npm_shim(monkeypatch, tmp_path: Path):
    appdata = tmp_path / "AppData" / "Roaming"
    npm_dir = appdata / "npm"
    npm_dir.mkdir(parents=True)
    shim_path = npm_dir / "codex.cmd"
    shim_path.write_text("@echo off\r\n", encoding="utf-8")

    monkeypatch.delenv("CREATURE_OS_CODEX_BIN", raising=False)
    monkeypatch.delenv("CODEX_BIN", raising=False)
    monkeypatch.setattr(config.os, "name", "nt")
    monkeypatch.setenv("APPDATA", str(appdata))
    monkeypatch.setattr(config.Path, "home", lambda: tmp_path / "home")

    assert config.codex_bin() == str(shim_path)
