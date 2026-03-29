from __future__ import annotations

import itertools
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from creatureos import config
from creatureos import service
from creatureos import storage
from creatureos import web


def _reset_service_runtime_state() -> None:
    service._RUNTIME_INITIALIZED = False
    service._DISPLAY_TIMEZONE_NAME_CACHE = None
    service._DISPLAY_TIMEZONE_CACHE = None
    service._CODEX_RATE_LIMIT_CACHE = None
    service._CODEX_RATE_LIMIT_CACHE_AT = None
    service._CODEX_MODEL_CHOICES_CACHE = None
    service._CODEX_MODEL_CHOICES_CACHE_AT = None
    service._RUN_FEED_ACTIVE_TURNS.clear()


@pytest.fixture
def runtime_env(tmp_path: Path):
    workspace_root = tmp_path / "workspace"
    data_dir = tmp_path / "data"
    db_path = data_dir / "test.sqlite3"
    workspace_root.mkdir(parents=True, exist_ok=True)
    with config.override_runtime_paths(workspace_root=workspace_root, data_dir=data_dir, db_path=db_path):
        _reset_service_runtime_state()
        storage.init_db()
        yield {"workspace_root": workspace_root, "data_dir": data_dir, "db_path": db_path}
        _reset_service_runtime_state()


@pytest.fixture
def client(runtime_env):
    _reset_service_runtime_state()
    with TestClient(web.app) as test_client:
        yield test_client
    _reset_service_runtime_state()


@pytest.fixture
def create_test_creature(runtime_env):
    counter = itertools.count(1)

    def _create(**overrides):
        index = next(counter)
        name = overrides.pop("display_name", f"Test Creature {index}")
        concern = overrides.pop("concern", f"Help with test concern {index}.")
        slug = overrides.pop("slug", f"test-creature-{index}")
        return service.create_creature(
            display_name=name,
            concern=concern,
            slug=slug,
            bootstrap=False,
            **overrides,
        )

    return _create
