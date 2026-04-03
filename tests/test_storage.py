from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from creatureos import storage


def test_storage_write_surfaces_round_trip(runtime_env):
    creature = storage.save_creature(
        slug="storage-roundtrip",
        display_name="Storage Roundtrip",
        system_role="",
        is_pinned=False,
        can_delete=True,
        ecosystem="woodlands",
        purpose_summary="Exercise storage writes.",
        temperament="steady",
        concern="Verify storage behavior.",
        system_prompt="Storage roundtrip prompt.",
        workdir="/tmp",
    )
    creature_id = int(creature["id"])

    conversation = storage.create_conversation(creature_id, title="Roundtrip Chat")
    assert int(conversation["creature_id"]) == creature_id

    run = storage.create_run(
        creature_id,
        trigger_type="manual",
        prompt_text="hello",
        thread_id=None,
        conversation_id=int(conversation["id"]),
        run_scope="chat",
        sandbox_mode="read-only",
    )
    run_id = int(run["id"])
    assert storage.get_creature(creature_id)["status"] == "running"

    message = storage.create_message(
        creature_id,
        conversation_id=int(conversation["id"]),
        role="user",
        body="hi there",
        run_id=run_id,
        metadata={"source": "test"},
    )
    assert int(message["conversation_id"]) == int(conversation["id"])

    run_event = storage.create_run_event(
        run_id,
        event_type="status",
        body='{"type":"status","phase":"started"}',
        metadata={"phase": "started"},
    )
    assert int(run_event["run_id"]) == run_id

    revision = storage.update_state_surface(
        creature_id,
        storage.PURPOSE_SURFACE_KEY,
        content="Roundtrip purpose\n",
        expected_revision=0,
    )
    assert revision == 1
    assert storage.get_state_surface_content(creature_id, storage.PURPOSE_SURFACE_KEY) == "Roundtrip purpose\n"

    agenda_items = storage.replace_agenda_items(
        creature_id,
        [{"title": "One thing", "priority": "high", "details": "Test it", "spawn_conversation": False}],
        source_run_id=run_id,
        source_message_id=int(message["id"]),
    )
    assert len(agenda_items) == 1

    backlog_items = storage.replace_backlog_items(
        creature_id,
        ["Someday"],
        source_run_id=run_id,
        source_message_id=int(message["id"]),
    )
    assert len(backlog_items) == 1

    next_run_at = datetime.now(timezone.utc) + timedelta(minutes=30)
    habit = storage.create_habit(
        creature_id,
        slug="live-habit",
        title="Live Habit",
        instructions="Do the live thing.",
        schedule_kind="interval",
        schedule_json={"every_minutes": 30},
        enabled=True,
        next_run_at=next_run_at,
    )
    assert int(habit["creature_id"]) == creature_id

    memory_record = storage.create_memory_record(
        creature_id,
        kind="instruction",
        body="Stay isolated during tests.",
        actor_type="system",
        reason="Storage test coverage",
        source_message_id=int(message["id"]),
        source_run_id=run_id,
        metadata={"source": "test"},
    )
    assert int(memory_record["creature_id"]) == creature_id

    memory_event = storage.create_memory_event(
        creature_id,
        record_id=int(memory_record["id"]),
        actor_type="system",
        action="remember",
        reason="Extra event coverage",
        metadata={"source": "test"},
    )
    assert int(memory_event["creature_id"]) == creature_id

    storage.finish_run(
        run_id,
        creature_id=creature_id,
        status="completed",
        raw_output_text="done",
        summary="Finished",
        severity="info",
        message_text="Finished",
        error_text=None,
        next_run_at=None,
        metadata={"source": "test"},
        notes_markdown="# Finished\n",
        notes_path=None,
    )
    finished_run = storage.get_run(run_id)
    assert finished_run is not None
    assert finished_run["status"] == "completed"
    assert storage.get_creature(creature_id)["status"] == "idle"


def test_pause_and_resume_habit_update_enabled_state(runtime_env):
    creature = storage.save_creature(
        slug="habit-toggle",
        display_name="Habit Toggle",
        system_role="",
        is_pinned=False,
        can_delete=True,
        ecosystem="sea",
        purpose_summary="Toggle a habit.",
        temperament="steady",
        concern="Verify habit pause and resume.",
        system_prompt="Habit toggle prompt.",
        workdir="/tmp",
    )
    creature_id = int(creature["id"])
    next_run_at = datetime.now(timezone.utc) + timedelta(minutes=15)
    habit = storage.create_habit(
        creature_id,
        slug="toggle-me",
        title="Toggle Me",
        instructions="Toggle me on and off.",
        schedule_kind="interval",
        schedule_json={"every_minutes": 15},
        enabled=True,
        next_run_at=next_run_at,
    )
    habit_id = int(habit["id"])

    storage.pause_habit(habit_id)
    paused = storage.get_habit(habit_id)
    assert paused is not None
    assert int(paused["enabled"] or 0) == 0
    assert paused["next_run_at"] is None

    storage.resume_habit(habit_id, next_run_at=next_run_at)
    resumed = storage.get_habit(habit_id)
    assert resumed is not None
    assert int(resumed["enabled"] or 0) == 1
    assert resumed["next_run_at"] is not None


def test_run_locks_allow_multiple_chat_runs_but_only_one_per_chat_and_activity(runtime_env):
    creature = storage.save_creature(
        slug="chat-locks",
        display_name="Chat Locks",
        system_role="",
        is_pinned=False,
        can_delete=True,
        ecosystem="terminal",
        purpose_summary="Exercise run locking.",
        temperament="steady",
        concern="Verify per-chat locking.",
        system_prompt="Chat lock prompt.",
        workdir="/tmp",
    )
    creature_id = int(creature["id"])
    conversation_one = storage.create_conversation(creature_id, title="One")
    conversation_two = storage.create_conversation(creature_id, title="Two")

    first_chat_run = storage.create_run(
        creature_id,
        trigger_type="user_reply",
        prompt_text="chat one",
        thread_id="thread-one",
        conversation_id=int(conversation_one["id"]),
        run_scope="chat",
        sandbox_mode="workspace-write",
    )
    second_chat_run = storage.create_run(
        creature_id,
        trigger_type="user_reply",
        prompt_text="chat two",
        thread_id="thread-two",
        conversation_id=int(conversation_two["id"]),
        run_scope="chat",
        sandbox_mode="workspace-write",
    )

    assert storage.latest_running_run_for_conversation(int(conversation_one["id"])) is not None
    assert storage.latest_running_run_for_conversation(int(conversation_two["id"])) is not None

    with pytest.raises(RuntimeError, match="active run lock"):
        storage.create_run(
            creature_id,
            trigger_type="user_reply",
            prompt_text="chat one again",
            thread_id="thread-three",
            conversation_id=int(conversation_one["id"]),
            run_scope="chat",
            sandbox_mode="workspace-write",
        )

    activity_run = storage.create_run(
        creature_id,
        trigger_type="habit",
        prompt_text="background pass",
        thread_id="activity-thread",
        conversation_id=None,
        run_scope="activity",
        sandbox_mode="read-only",
    )

    with pytest.raises(RuntimeError, match="active activity run lock"):
        storage.create_run(
            creature_id,
            trigger_type="habit",
            prompt_text="second background pass",
            thread_id="activity-thread-2",
            conversation_id=None,
            run_scope="activity",
            sandbox_mode="read-only",
        )

    storage.finish_run(
        int(first_chat_run["id"]),
        creature_id=creature_id,
        status="completed",
        raw_output_text="done",
        summary="done",
        severity="info",
        message_text="done",
        error_text=None,
        next_run_at=None,
        metadata={},
        notes_markdown=None,
        notes_path=None,
    )
    storage.finish_run(
        int(second_chat_run["id"]),
        creature_id=creature_id,
        status="completed",
        raw_output_text="done",
        summary="done",
        severity="info",
        message_text="done",
        error_text=None,
        next_run_at=None,
        metadata={},
        notes_markdown=None,
        notes_path=None,
    )
    still_running = storage.get_creature(creature_id)
    assert still_running is not None
    assert str(still_running["status"]) == "running"

    storage.finish_run(
        int(activity_run["id"]),
        creature_id=creature_id,
        status="completed",
        raw_output_text="done",
        summary="done",
        severity="info",
        message_text="done",
        error_text=None,
        next_run_at=None,
        metadata={},
        notes_markdown=None,
        notes_path=None,
    )
    finished_creature = storage.get_creature(creature_id)
    assert finished_creature is not None
    assert str(finished_creature["status"]) == "idle"
