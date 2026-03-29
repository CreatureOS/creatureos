from creatureos import cli


def test_prepare_server_runtime_initializes_and_refreshes(monkeypatch):
    calls: list[tuple[str, str | None]] = []

    monkeypatch.setattr(cli.storage, "init_db", lambda: calls.append(("init_db", None)))
    monkeypatch.setattr(
        cli.storage,
        "delete_meta",
        lambda key: calls.append(("delete_meta", str(key))),
    )
    monkeypatch.setattr(
        cli.service,
        "ensure_runtime_ready",
        lambda: calls.append(("ensure_runtime_ready", None)),
    )

    cli._prepare_server_runtime(force_scan=True)

    assert calls == [
        ("init_db", None),
        ("delete_meta", cli.service.ONBOARDING_ENVIRONMENT_KEY),
        ("delete_meta", cli.service.ONBOARDING_BRIEFING_KEY),
        ("ensure_runtime_ready", None),
    ]


def test_prepare_server_runtime_without_force_scan(monkeypatch):
    calls: list[str] = []

    monkeypatch.setattr(cli.storage, "init_db", lambda: calls.append("init_db"))
    monkeypatch.setattr(
        cli.storage,
        "delete_meta",
        lambda key: calls.append(f"delete_meta:{key}"),
    )
    monkeypatch.setattr(
        cli.service,
        "ensure_runtime_ready",
        lambda: calls.append("ensure_runtime_ready"),
    )

    cli._prepare_server_runtime(force_scan=False)

    assert calls == ["init_db", "ensure_runtime_ready"]
