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


def test_acquire_supervisor_lock_uses_fcntl_when_available(monkeypatch):
    calls: list[tuple[int, int]] = []

    class FakeFcntl:
        LOCK_EX = 1
        LOCK_NB = 2
        LOCK_UN = 4

        @staticmethod
        def flock(fd: int, mode: int) -> None:
            calls.append((fd, mode))

    class FakeFile:
        def fileno(self) -> int:
            return 17

    monkeypatch.setattr(cli, "fcntl", FakeFcntl)
    monkeypatch.setattr(cli, "msvcrt", None)

    cli._acquire_supervisor_lock(FakeFile())
    cli._release_supervisor_lock(FakeFile())

    assert calls == [(17, 3), (17, 4)]


def test_acquire_supervisor_lock_uses_msvcrt_when_fcntl_missing(monkeypatch):
    calls: list[tuple[int, int, int]] = []

    class FakeMsvcrt:
        LK_NBLCK = 7
        LK_UNLCK = 8

        @staticmethod
        def locking(fd: int, mode: int, size: int) -> None:
            calls.append((fd, mode, size))

    class FakeFile:
        def __init__(self):
            self.contents = ""
            self.pointer = 0

        def fileno(self) -> int:
            return 21

        def seek(self, offset: int) -> None:
            self.pointer = offset

        def read(self, size: int = -1) -> str:
            if size < 0:
                return self.contents[self.pointer :]
            return self.contents[self.pointer : self.pointer + size]

        def write(self, value: str) -> int:
            self.contents = value
            self.pointer = len(value)
            return len(value)

        def flush(self) -> None:
            return None

    fake_file = FakeFile()

    monkeypatch.setattr(cli, "fcntl", None)
    monkeypatch.setattr(cli, "msvcrt", FakeMsvcrt)

    cli._acquire_supervisor_lock(fake_file)
    cli._release_supervisor_lock(fake_file)

    assert fake_file.contents == "1"
    assert calls == [(21, 7, 1), (21, 8, 1)]
