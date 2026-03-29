#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from creatureos import config
from creatureos import storage


def _iso_now() -> datetime:
    return datetime.now(timezone.utc)


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="creatureos-storage-smoke-") as tmp_dir:
        runtime_root = Path(tmp_dir)
        workspace_root = runtime_root / "workspace"
        data_dir = runtime_root / "data"
        db_path = data_dir / "smoke.sqlite3"
        workspace_root.mkdir(parents=True, exist_ok=True)
        with config.override_runtime_paths(workspace_root=workspace_root, data_dir=data_dir, db_path=db_path):
            storage.init_db()

            creature = storage.save_creature(
                slug="storage-smoke-creature",
                display_name="Storage Smoke Creature",
                system_role="",
                is_pinned=False,
                can_delete=True,
                ecosystem="woodlands",
                purpose_summary="Exercise storage write surfaces.",
                temperament="steady",
                concern="Verify the storage layer.",
                system_prompt="You exist only for a storage smoke test.",
                workdir=str(workspace_root),
            )
            creature_id = int(creature["id"])

            try:
                conversation = storage.create_conversation(
                    creature_id,
                    title="Smoke Chat",
                    owner_mode="implement",
                )
                conversation_id = int(conversation["id"])

                run = storage.create_run(
                    creature_id,
                    trigger_type="manual",
                    prompt_text="storage smoke prompt",
                    thread_id=None,
                    conversation_id=conversation_id,
                    run_scope="chat",
                    sandbox_mode="read-only",
                )
                run_id = int(run["id"])

                message = storage.create_message(
                    creature_id,
                    conversation_id=conversation_id,
                    role="user",
                    body="Smoke test message",
                    run_id=run_id,
                    metadata={"source": "storage_smoke"},
                )
                message_id = int(message["id"])

                run_event = storage.create_run_event(
                    run_id,
                    event_type="status",
                    body='{"type":"status","phase":"started"}',
                    metadata={"phase": "started"},
                )
                run_event_id = int(run_event["id"])

                revision = storage.update_state_surface(
                    creature_id,
                    storage.PURPOSE_SURFACE_KEY,
                    content="Smoke purpose surface\n",
                    expected_revision=0,
                )
                assert revision == 1, revision
                assert storage.get_state_surface_content(creature_id, storage.PURPOSE_SURFACE_KEY) == "Smoke purpose surface\n"

                agenda_items = storage.replace_agenda_items(
                    creature_id,
                    [
                        {
                            "title": "Check the storage layer",
                            "priority": "high",
                            "details": "Smoke write path",
                            "spawn_conversation": False,
                        }
                    ],
                    source_run_id=run_id,
                    source_message_id=message_id,
                )
                assert len(agenda_items) == 1

                backlog_items = storage.replace_backlog_items(
                    creature_id,
                    ["Clean up the smoke data"],
                    source_run_id=run_id,
                    source_message_id=message_id,
                )
                assert len(backlog_items) == 1

                next_run_at = _iso_now() + timedelta(minutes=15)
                habit = storage.create_habit(
                    creature_id,
                    slug="check-storage-live",
                    title="Check storage live",
                    instructions="Run the storage smoke pass.",
                    schedule_kind="interval",
                    schedule_json={"every_minutes": 30, "window_start": "08:00", "window_end": "20:00"},
                    enabled=True,
                    next_run_at=next_run_at,
                )
                habit_id = int(habit["id"])
                storage.pause_habit(habit_id)
                paused_habit = storage.get_habit(habit_id)
                assert paused_habit is not None and int(paused_habit["enabled"] or 0) == 0
                storage.resume_habit(habit_id, next_run_at=next_run_at)
                resumed_habit = storage.get_habit(habit_id)
                assert resumed_habit is not None and int(resumed_habit["enabled"] or 0) == 1

                memory_record = storage.create_memory_record(
                    creature_id,
                    kind="instruction",
                    body="The smoke test should stay isolated.",
                    actor_type="system",
                    reason="Storage smoke validation.",
                    source_message_id=message_id,
                    source_run_id=run_id,
                    metadata={"source": "storage_smoke"},
                )
                memory_record_id = int(memory_record["id"])

                memory_event = storage.create_memory_event(
                    creature_id,
                    record_id=memory_record_id,
                    actor_type="system",
                    action="remember",
                    reason="Explicit memory event coverage.",
                    metadata={"source": "storage_smoke"},
                )
                memory_event_id = int(memory_event["id"])

                storage.finish_run(
                    run_id,
                    creature_id=creature_id,
                    status="completed",
                    raw_output_text="smoke output",
                    summary="Storage smoke completed.",
                    severity="info",
                    message_text="Storage smoke completed.",
                    error_text=None,
                    next_run_at=None,
                    metadata={"source": "storage_smoke"},
                    notes_markdown="# Smoke\n",
                    notes_path=None,
                )

                summary = {
                    "creature_id": creature_id,
                    "conversation_id": conversation_id,
                    "run_id": run_id,
                    "message_id": message_id,
                    "run_event_id": run_event_id,
                    "habit_id": habit_id,
                    "memory_record_id": memory_record_id,
                    "memory_event_id": memory_event_id,
                    "purpose_revision": revision,
                }
                print(json.dumps(summary, indent=2, sort_keys=True))
            finally:
                storage.delete_creature(creature_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
