from __future__ import annotations

from creatureos import service
from creatureos import storage


def _seed_intro(creature) -> None:
    conversation = storage.create_conversation(int(creature["id"]), title=service.INTRODUCTION_CHAT_TITLE)
    storage.create_message(
        int(creature["id"]),
        conversation_id=int(conversation["id"]),
        role="creature",
        body=f"I'm {creature['display_name']}.",
    )


def test_healthz_returns_ok(client):
    response = client.get("/healthz")
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"


def test_index_shows_ecosystem_chooser_on_fresh_runtime(client):
    response = client.get("/")
    assert response.status_code == 200
    assert "Choose an Ecosystem" in response.text


def test_confirm_onboarding_ecosystem_moves_to_starter(client):
    response = client.post("/onboarding/ecosystem", data={"ecosystem_choice": "sea"}, follow_redirects=False)
    assert response.status_code == 303
    assert service.get_onboarding_phase() == "starter"


def test_onboarding_chat_ajax_returns_json(client, monkeypatch):
    service.confirm_onboarding_ecosystem(ecosystem_choice="sea")

    monkeypatch.setattr(
        service,
        "send_onboarding_chat_message",
        lambda body, attachments=None: {
            "assistant_body": "I hear you.",
            "message_count": 2,
            "starter_ready": False,
            "status": "completed",
        },
    )

    response = client.post(
        "/onboarding/chat",
        data={"body": "hello"},
        headers={"x-creatureos-ajax": "1"},
    )
    assert response.status_code == 200
    assert response.json()["assistant_body"] == "I hear you."


def test_create_habit_route_persists_new_habit(client, create_test_creature):
    creature = create_test_creature(display_name="Harbor", concern="Watch over the work.")

    response = client.post(
        f"/creatures/{creature['slug']}/habits",
        data={
            "title": "Morning Watch",
            "instructions": "Check in each morning.",
            "schedule_kind": "daily",
            "daily_time": "08:15",
            "view": "activity",
        },
        follow_redirects=False,
    )

    assert response.status_code == 303
    habits = storage.list_habits(int(creature["id"]), include_disabled=True)
    assert any(str(row["title"] or "") == "Morning Watch" for row in habits)


def test_draft_new_chat_does_not_create_conversation_row(client, create_test_creature):
    creature = create_test_creature(display_name="Juniper", concern="Keep me company.")
    _seed_intro(creature)
    before = len(storage.list_conversations(int(creature["id"])))

    response = client.get(f"/?creature={creature['slug']}&view=chats&new_chat=1")

    assert response.status_code == 200
    after = len(storage.list_conversations(int(creature["id"])))
    assert after == before


def test_new_habit_creates_seeded_chat_and_first_reply_renames_it(client, create_test_creature):
    creature = create_test_creature(display_name="Mothwake", concern="Stay close to the work.")
    _seed_intro(creature)

    response = client.post(
        f"/creatures/{creature['slug']}/habits/new-chat",
        follow_redirects=False,
    )

    assert response.status_code == 303
    latest = storage.get_latest_conversation(int(creature["id"]))
    assert latest is not None
    conversation_id = int(latest["id"])
    assert str(latest["title"]) == service.NEW_HABIT_CHAT_TITLE

    messages = storage.list_messages(conversation_id)
    assert len(messages) == 1
    assert str(messages[0]["role"]) == "creature"
    assert "A habit is recurring work" in str(messages[0]["body"])

    service.send_user_message(
        str(creature["slug"]),
        conversation_id,
        "Every morning, check the repo and tell me what changed.",
    )
    renamed = storage.get_conversation(conversation_id)
    assert renamed is not None
    assert str(renamed["title"]) != service.NEW_HABIT_CHAT_TITLE
