from __future__ import annotations

import json
import threading
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


def test_preview_summoning_name_rejects_title_like_generated_candidates(runtime_env, monkeypatch):
    monkeypatch.setattr(
        service,
        "_codex_summoning_name_candidates",
        lambda **kwargs: [
            "Set Of Repos: Current Intent",
            "Mosswake",
            "Queue Status Tracker",
            "Harbor Bell",
        ],
    )
    monkeypatch.setattr(
        service,
        "_codex_select_summoning_name",
        lambda **kwargs: {
            "display_name": "Set Of Repos: Current Intent",
            "alternates": ["Queue Status Tracker", "Harbor Bell"],
        },
    )

    preview = service.preview_summoning_names(
        brief="A first creature that looks across active software work, keeps repo state coherent, and notices drift early.",
        ecosystem="woodlands",
        purpose_summary="Looks across active software work, keeps repo state coherent, and notices drift early.",
    )

    assert preview["proposed_name"] == "Mosswake"
    assert preview["candidates"] == ["Mosswake", "Harbor Bell"]
    assert preview["alternates"] == ["Harbor Bell"]


def test_extract_explicit_creature_name_rejects_title_like_phrase():
    assert service._extract_explicit_creature_name("Call it Set Of Repos: Current Intent.") is None


def test_summoning_name_fallback_skips_operational_brief_keywords(runtime_env, monkeypatch):
    monkeypatch.setattr(service, "_codex_summoning_name_candidates", lambda **kwargs: [])
    monkeypatch.setattr(service, "_codex_select_summoning_name", lambda **kwargs: None)

    preview = service.preview_summoning_names(
        brief="Summon a creature for repo project state intent coordination workflows tasks.",
        ecosystem="woodlands",
        purpose_summary="Keeps repo project state and workflow intent aligned.",
    )

    assert preview["proposed_name"] == "Woodlands Echo"
    assert preview["alternates"] == []


def test_parse_report_preserves_full_message_body():
    complete_sentence = "This is a complete sentence."
    closing_sentence = (
        "If you want, I can tailor this to your exact setup: macOS, Windows, or Linux on the computer, "
        "and iPhone or Android on the phone."
    )
    message_parts: list[str] = []
    while len(" ".join(message_parts + [closing_sentence])) <= service.MAX_OWNER_UPDATE_PREVIEW_CHARS + 120:
        message_parts.append(complete_sentence)
    payload = json.dumps(
        {
            "summary": "A long reply",
            "should_notify": True,
            "severity": "info",
            "message": " ".join(message_parts + [closing_sentence]),
        }
    )

    report = service._parse_report(payload, fallback_prefix="Fallback")

    assert report["message"] == " ".join(message_parts + [closing_sentence])


def test_parse_report_preserves_non_json_message_body():
    raw = "A plain-text Codex reply that should be stored whole without being chopped off."

    report = service._parse_report(raw, fallback_prefix="Fallback")

    assert report["message"] == raw


def test_trim_owner_update_preview_falls_back_to_word_boundary_when_needed():
    text = ("alpha " * 400).strip()

    trimmed = service._trim_owner_update_preview(text, limit=120)

    assert len(trimmed) <= 120
    assert trimmed.endswith("...")
    assert trimmed[:-3].endswith("alpha")


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


def test_classify_codex_launch_failure_as_local_issue():
    issue = service._classify_codex_access_issue(
        service.CodexCommandError("Failed to launch Codex command 'codex': [WinError 5] Access is denied")
    )

    assert issue == {
        "kind": "local",
        "detail": "Failed to launch Codex command 'codex': [WinError 5] Access is denied",
    }


def test_local_codex_waiting_message_tells_user_to_install_or_configure_cli():
    message = service._codex_waiting_message(kind="local")

    assert "Install the Codex CLI" in message
    assert "`codex` command works in a terminal" in message
    assert "`CREATURE_OS_CODEX_BIN`" in message


def test_keeper_dialog_mentions_failed_first_creature(runtime_env, create_test_creature):
    service.confirm_onboarding_ecosystem(ecosystem_choice="woodlands")
    keeper = storage.get_creature_by_slug(service.KEEPER_SLUG)
    assert keeper is not None
    failed_creature = create_test_creature(display_name="Mosswake", concern="Keep the work coherent.")
    failed_run = storage.create_run(
        int(failed_creature["id"]),
        trigger_type="bootstrap",
        prompt_text="Wake up.",
        thread_id=None,
        conversation_id=None,
        run_scope=service.RUN_SCOPE_ACTIVITY,
        sandbox_mode="read-only",
    )
    storage.finish_run(
        int(failed_run["id"]),
        creature_id=int(failed_creature["id"]),
        status="failed",
        raw_output_text=None,
        summary=None,
        severity="critical",
        message_text=None,
        error_text="Failed to launch Codex command 'codex': [WinError 5] Access is denied",
        next_run_at=None,
        metadata={"run_scope": service.RUN_SCOPE_ACTIVITY},
        notes_markdown=None,
        notes_path=None,
    )

    creatures = [dict(row) for row in storage.list_creatures()]
    for creature in creatures:
        creature.update(service._creature_intro_state(creature))

    dialog = service._keeper_dialog_state(
        keeper,
        creatures=creatures,
        conversation=None,
        messages=[],
        onboarding_required=False,
        transition_notice="starter-creatures-creating",
    )

    assert "Mosswake" in dialog["body"]
    assert "failed to wake cleanly" in dialog["body"]
    assert "Access is denied" in dialog["body"]


def test_restart_onboarding_interrupts_active_keeper_run(runtime_env):
    service.confirm_onboarding_ecosystem(ecosystem_choice="woodlands")
    keeper = storage.get_creature_by_slug(service.KEEPER_SLUG)
    assert keeper is not None
    conversation = storage.find_conversation_by_title(int(keeper["id"]), service.KEEPER_CONVERSATION_TITLE)
    assert conversation is not None

    run = storage.create_run(
        int(keeper["id"]),
        trigger_type="keeper_chat",
        prompt_text="Tell me what I am to do.",
        thread_id="thread_123",
        conversation_id=int(conversation["id"]),
        run_scope=service.RUN_SCOPE_CHAT,
        sandbox_mode="read-only",
    )
    service._remember_active_run_thread(int(run["id"]), threading.current_thread())

    state = service.restart_onboarding()
    interrupted = storage.get_run(int(run["id"]))

    assert state["phase"] == service.DEFAULT_ONBOARDING_PHASE
    assert interrupted is not None
    assert str(interrupted["status"]) == "failed"
    assert str(interrupted["error_text"]) == service.ONBOARDING_RESTART_RUN_ERROR_TEXT
    assert storage.latest_running_run_for_creature(int(keeper["id"])) is None
    assert storage.get_conversation(int(conversation["id"])) is None
    assert int(run["id"]) not in service._ACTIVE_RUN_THREADS


def test_delete_conversation_interrupts_active_run(create_test_creature):
    creature = create_test_creature(display_name="Morrow", concern="Keep the chat flowing.")
    creature_id = int(creature["id"])
    conversation = storage.create_conversation(creature_id, title="Current chat")
    fallback = storage.create_conversation(creature_id, title="Older chat")
    run = storage.create_run(
        creature_id,
        trigger_type="user_reply",
        prompt_text="Reply to the owner.",
        thread_id="thread_456",
        conversation_id=int(conversation["id"]),
        run_scope=service.RUN_SCOPE_CHAT,
        sandbox_mode="read-only",
    )

    result = service.delete_conversation(
        str(creature["slug"]),
        int(conversation["id"]),
        current_conversation_id=int(fallback["id"]),
    )
    interrupted = storage.get_run(int(run["id"]))

    assert result == {
        "deleted_conversation_id": int(conversation["id"]),
        "redirect_conversation_id": int(fallback["id"]),
    }
    assert interrupted is not None
    assert str(interrupted["status"]) == "failed"
    assert str(interrupted["error_text"]) == service.CONVERSATION_RESET_RUN_ERROR_TEXT
    assert storage.latest_running_run_for_creature(creature_id) is None


def test_reset_ecosystem_removes_creatures_and_resets_onboarding(create_test_creature):
    create_test_creature(display_name="Lumen", concern="Keep me steady.")
    service.confirm_onboarding_ecosystem(ecosystem_choice="sea")

    service.reset_ecosystem()

    assert storage.list_creatures() == []
    assert service.get_onboarding_phase() == service.DEFAULT_ONBOARDING_PHASE
    assert service.get_ecosystem()["value"] == service.DEFAULT_ECOSYSTEM
