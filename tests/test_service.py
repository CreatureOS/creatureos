from __future__ import annotations

from datetime import datetime, timedelta, timezone

from creatureos import service
from creatureos import storage


def _seed_intro(creature: dict[str, object]) -> None:
    conversation = storage.create_conversation(int(creature["id"]), title=service.INTRODUCTION_CHAT_TITLE)
    storage.create_message(
        int(creature["id"]),
        conversation_id=int(conversation["id"]),
        role="creature",
        body="I'm here.",
        metadata={},
    )


def test_dashboard_state_defaults_to_onboarding_ecosystem(runtime_env):
    state = service.dashboard_state()
    assert state["active_view"] == "onboarding-ecosystem"
    assert state["selected_creature"] is None


def test_create_creature_sets_up_purpose_and_default_ponder_habit(create_test_creature):
    creature = create_test_creature(
        display_name="Morrow",
        concern="Be a steady collaborator.",
        purpose_summary="Help steadily and calmly.",
        ecosystem="sea",
    )
    creature_id = int(creature["id"])

    purpose_text = storage.get_state_surface_content(creature_id, service.PURPOSE_DOC_KEY)
    assert "# Morrow purpose" in purpose_text
    assert "## Why you exist" in purpose_text

    habits = storage.list_habits(creature_id, include_disabled=True)
    assert any(str(row["slug"] or "") == service.PONDER_HABIT_SLUG for row in habits)


def test_send_user_message_creates_chat_and_queues_reply(create_test_creature):
    creature = create_test_creature(display_name="Juniper", concern="Keep the owner company.")
    result = service.send_user_message(str(creature["slug"]), None, "Hello there")

    assert result["status"] == "queued"
    assert result["conversation_id"] is not None
    assert result["message_id"] is not None

    messages = storage.list_messages(int(result["conversation_id"]))
    assert len(messages) == 1
    assert str(messages[0]["role"]) == "user"
    assert str(messages[0]["body"]) == "Hello there"


def test_set_creature_habit_enabled_toggles_habit(create_test_creature):
    creature = create_test_creature(display_name="Lumen", concern="Reflect after chats.")
    habit = next(
        row for row in storage.list_habits(int(creature["id"]), include_disabled=True)
        if str(row["slug"] or "") == service.PONDER_HABIT_SLUG
    )

    disabled = service.set_creature_habit_enabled(str(creature["slug"]), int(habit["id"]), enabled=False)
    assert disabled["enabled"] is False

    enabled = service.set_creature_habit_enabled(str(creature["slug"]), int(habit["id"]), enabled=True)
    assert enabled["enabled"] is True


def test_preview_summoning_name_depends_on_creature_ecosystem_not_current_habitat(runtime_env, monkeypatch):
    monkeypatch.setattr(
        service,
        "_codex_summoning_name_candidates",
        lambda **kwargs: [
            "Harbor Bell",
            "Reef Wake",
            "Brine Lantern",
            "Tide Morrow",
            "Salt Echo",
            "Current Hollow",
            "Cove Thread",
            "Kelp Ember",
            "Shoal Vesper",
            "Pearl Drift",
        ],
    )
    monkeypatch.setattr(
        service,
        "_codex_select_summoning_name",
        lambda **kwargs: {
            "display_name": "Harbor Bell",
            "alternates": ["Reef Wake", "Brine Lantern", "Tide Morrow"],
        },
    )
    brief = "A steady companion who stays close and helps me keep going."

    service.set_ecosystem(choice="woodlands")
    preview_one = service.preview_summoning(
        brief=brief,
        ecosystem="sea",
    )

    service.set_ecosystem(choice="terminal")
    preview_two = service.preview_summoning(
        brief=brief,
        ecosystem="sea",
    )

    assert preview_one["preview"]["ecosystem"] == preview_two["preview"]["ecosystem"]
    assert preview_one["preview"]["proposed_name"] == preview_two["preview"]["proposed_name"]
    assert preview_one["preview"]["alternates"] == preview_two["preview"]["alternates"]
    assert preview_one["preview"]["proposed_name"] == "Harbor Bell"
    assert preview_one["preview"]["alternates"] == ["Reef Wake", "Brine Lantern", "Tide Morrow"]


def test_summoning_name_prompt_includes_ecosystem_world_description(runtime_env):
    prompt = service._summoning_name_candidates_prompt(
        brief="A steady companion who stays close and helps me keep going.",
        purpose_summary="Stay near, keep me company, and help me continue.",
        ecosystem="sea",
    )

    assert "Creature ecosystem: The Sea" in prompt
    assert "The Sea is a world of currents, reefs, coves, shoals" in prompt
    assert "Creatures from here should feel tidal, brined, drifting" in prompt


def test_system_scan_roots_follow_likely_machine_work_dirs_not_workspace(runtime_env, tmp_path):
    home = tmp_path / "home"
    home.mkdir()
    (home / "Projects").mkdir()
    (home / "Client Work").mkdir()
    (home / "Archive").mkdir()

    specs = service._system_scan_root_specs(home)
    scanned_paths = {row["path"] for row in specs}

    assert home in scanned_paths
    assert home / "Projects" in scanned_paths
    assert home / "Client Work" in scanned_paths
    assert runtime_env["workspace_root"] not in scanned_paths


def test_activity_view_shows_selected_habit_log_oldest_first(create_test_creature):
    creature = create_test_creature(display_name="Mothwake", concern="Reflect after chats.")
    _seed_intro(creature)
    creature_id = int(creature["id"])
    ponder = next(
        row for row in storage.list_habits(creature_id, include_disabled=True)
        if str(row["slug"] or "") == service.PONDER_HABIT_SLUG
    )
    other_habit = storage.create_habit(
        creature_id,
        slug="morning-watch",
        title="Morning Watch",
        instructions="Check in each morning.",
        schedule_kind="daily",
        schedule_json={"time": "08:00"},
        enabled=True,
        next_run_at=datetime.now(timezone.utc) + timedelta(hours=8),
    )

    first_run = storage.create_run(
        creature_id,
        trigger_type="habit",
        prompt_text="first ponder run",
        thread_id=None,
        conversation_id=None,
        run_scope=service.RUN_SCOPE_ACTIVITY,
        sandbox_mode="read-only",
    )
    storage.finish_run(
        int(first_run["id"]),
        creature_id=creature_id,
        status="completed",
        raw_output_text="",
        summary="First ponder summary",
        severity="info",
        message_text="First ponder note",
        error_text=None,
        next_run_at=None,
        metadata={"habit_id": int(ponder["id"]), "habit_title": str(ponder["title"])},
        notes_markdown=None,
        notes_path=None,
    )

    second_run = storage.create_run(
        creature_id,
        trigger_type="habit",
        prompt_text="other habit run",
        thread_id=None,
        conversation_id=None,
        run_scope=service.RUN_SCOPE_ACTIVITY,
        sandbox_mode="read-only",
    )
    storage.finish_run(
        int(second_run["id"]),
        creature_id=creature_id,
        status="completed",
        raw_output_text="",
        summary="Other habit summary",
        severity="info",
        message_text="Other habit note",
        error_text=None,
        next_run_at=None,
        metadata={"habit_id": int(other_habit["id"]), "habit_title": str(other_habit["title"])},
        notes_markdown=None,
        notes_path=None,
    )

    third_run = storage.create_run(
        creature_id,
        trigger_type="habit",
        prompt_text="second ponder run",
        thread_id=None,
        conversation_id=None,
        run_scope=service.RUN_SCOPE_ACTIVITY,
        sandbox_mode="read-only",
    )
    storage.finish_run(
        int(third_run["id"]),
        creature_id=creature_id,
        status="completed",
        raw_output_text="",
        summary="Second ponder summary",
        severity="info",
        message_text="Second ponder note",
        error_text=None,
        next_run_at=None,
        metadata={"habit_id": int(ponder["id"]), "habit_title": str(ponder["title"])},
        notes_markdown=None,
        notes_path=None,
    )

    state = service.dashboard_state(
        selected_slug=str(creature["slug"]),
        view="activity",
        habit_target=str(int(ponder["id"])),
    )

    assert state["selected_habit"] is not None
    assert int(state["selected_habit"]["id"]) == int(ponder["id"])
    assert [item["summary"] for item in state["selected_activity_feed"]] == [
        "First ponder summary",
        "Second ponder summary",
    ]
