# CreatureOS

CreatureOS is a local-first runtime for persistent Codex creatures.

It gives you a small ambient habitat of specialized creatures that can:
- watch a workspace over time
- keep durable notes, memory, and activity reports
- chat in a web UI
- practice habits on a schedule and surface useful things without needing a fresh prompt every time
- work with local documents, attachments, and browser flows as part of their normal job
- keep private workshop scripts, files, templates, and reports that make repeated work easier

CreatureOS is intentionally opinionated:
- chats get fresh Codex threads
- habit runs stay on a persistent creature thread
- state lives locally in SQLite
- creatures use purpose and habits as their authority rather than a matrix of manual capability grants
- The Keeper helps summon and shape the rest of the habitat

## Prerequisites

CreatureOS itself is a Python app, but it wraps the Codex CLI.

You should have:
- Python `3.12+`
- the `codex` CLI available on your `PATH`
- Codex authenticated locally

SQLite uses Python's built-in `sqlite3` module. There is no separate database server to install.

Optional:
- Tailscale, if you want private cross-device access with `--tailscale`

## Install

Install from a checkout:

```bash
python3 -m pip install -e .
```

Once published to PyPI, the install command will be:

```bash
pip install creatureos
```

## Start

Initialize the database once:

```bash
creatureos init-db
```

Start CreatureOS in safe local-only mode:

```bash
creatureos serve --workspace /path/to/workspace
```

That binds to:
- `127.0.0.1:404` by default

Serve on localhost plus the detected Tailscale IPv4:

```bash
creatureos serve --workspace /path/to/workspace --tailscale
```

That binds to:
- `127.0.0.1:404`
- the detected Tailscale IPv4 on the same port

If no Tailscale IPv4 is detected, it falls back to localhost-only.

Force a fresh onboarding environment scan on boot:

```bash
creatureos serve --workspace /path/to/workspace --force-scan
```

## Working Root And Data

If you do not pass `--workspace`, CreatureOS uses your current working directory as the primary working root for creature file work.

The onboarding scan is broader than that. It looks across likely work directories on the machine so The Keeper can form a first impression of the kind of work you do.

For repeatable launches, prefer setting the workspace explicitly with:
- `creatureos --workspace /path/to/workspace serve`

or with:
- `CREATURE_OS_WORKSPACE_ROOT`

Runtime state lives under:

```text
~/.local/state/creatureos
```

on Linux by default.

On macOS the default is:

```text
~/Library/Application Support/CreatureOS
```

On Windows the default is:

```text
%LOCALAPPDATA%\CreatureOS
```

Override that explicitly with:
- `creatureos --data-dir /path/to/data serve`
- `CREATURE_OS_DATA_DIR`
- `CREATURE_OS_DB_PATH`

## Environment

- `CREATURE_OS_WORKSPACE_ROOT`: primary working root for creature file work
- `CREATURE_OS_DATA_DIR`: runtime data directory
- `CREATURE_OS_DB_PATH`: SQLite path override
- `CREATURE_OS_HOST`: bind host override for single-bind serve mode
- `CREATURE_OS_PUBLIC_HOST`: display host used in generated URLs
- `CREATURE_OS_PORT`: port override
- `CREATURE_OS_CODEX_BIN`: Codex CLI binary
- `CREATURE_OS_MODEL`: model override
- `CREATURE_OS_REASONING_EFFORT`: reasoning effort override
- `CREATURE_OS_TIMEOUT_SECONDS`: read-only run timeout
- `CREATURE_OS_WRITE_TIMEOUT_SECONDS`: write-enabled run timeout
- `CREATURE_OS_PYTHON_BIN`: Python interpreter used by helper scripts

## Development

Common commands:

```bash
python3 -m py_compile creatureos/cli.py creatureos/web.py creatureos/service.py creatureos/storage.py creatureos/config.py creatureos/codex_cli.py
node --check creatureos/static/creature_os.js
python3 -m creatureos.cli --help
python3 -m creatureos.cli serve --help
```

Health check:

```bash
curl http://127.0.0.1:404/healthz
```

Testing:

```bash
python3 -m pytest
```

Browser smoke test:

```bash
CREATURE_OS_RUN_BROWSER_SMOKE=1 python3 -m pytest -m browser
```

Storage-focused guardrails:

```bash
python3 scripts/check_storage_sql.py
python3 scripts/storage_smoke.py
```

## Packaging

Build an sdist and wheel:

```bash
python3 -m pip install build twine
python3 -m build
python3 -m twine check dist/*
```

CreatureOS wheels bundle:
- templates
- static assets

so the installed app can run outside a source checkout.

## Publishing

CreatureOS is set up for trusted publishing with GitHub Actions.

Test phase on TestPyPI:
- add this GitHub repo as a trusted publisher in TestPyPI
- run the `Publish Package` workflow manually with target `testpypi`

Real release on PyPI:
- add this GitHub repo as a trusted publisher in PyPI
- create a GitHub release
- the same workflow publishes to PyPI automatically on `release.published`

The CI workflow also smoke-installs the built wheel so packaging regressions get caught before release.

## Repo Layout

- runtime package: `creatureos/`
- UI assets: `creatureos/templates/`, `creatureos/static/`
- helper scripts: `scripts/`
- tests: `tests/`

## Notes

- `serve` runs a small supervisor that restarts the worker when core runtime files change.
- Static JS and CSS are served with revalidation headers to avoid stale browser state.
- If a server is already running for the same CreatureOS data directory, a second `serve` exits instead of starting a duplicate process.

## License

CreatureOS is licensed under Apache 2.0. See [LICENSE](LICENSE).
