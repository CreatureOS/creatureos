# CreatureOS

CreatureOS turns Codex into a local-first habitat of durable creatures on your machine.

Each creature has a purpose, memory, chats, worklists, and teachable habits. The Keeper helps decide what creature should exist, what should stay alive, and whether the habitat is actually serving you well.

## Quick Start

Preferred path:

If you do not already have Codex installed, first download the Codex app or CLI:

```text
https://chatgpt.com/codex/get-started
```

Then set Codex to full access, open Codex, and type:

```text
install creatureos using pip and spin it up, troubleshoot any issues
```

That is the nicest path because Codex can handle the install, fix dependency issues, verify that the Codex CLI is actually usable, and get the server running for you.

Manual path:

```bash
pip install creatureos
creatureos serve
```

Then open:

```text
http://127.0.0.1:404
```

If you want CreatureOS to work from a specific folder, either:
- `cd` into that folder first, then run `creatureos serve`
- or use `creatureos serve --workspace /path/to/work`

## What `serve` Does

`creatureos serve` handles the normal setup automatically:
- creates the CreatureOS data directory if needed
- creates the SQLite database if needed
- prepares the first-run Keeper state
- starts the local web app

You do not need to run a separate database setup step first.

## What You Need

- Python `3.10+`
- the `codex` CLI on your `PATH`
- Codex authenticated locally
- if you have not installed Codex yet, start here: `https://chatgpt.com/codex/get-started`

Optional:
- Tailscale, if you want private access from another device with `--tailscale`

## Connect From Your Phone

CreatureOS can run on your computer and still be reachable from your phone.

The usual path is:
- run `creatureos serve --tailscale` on the computer
- install Tailscale on both the computer and the phone
- open the Tailscale CreatureOS URL from the phone

If you want Codex to walk you through it, set Codex to full access and type:

```text
please give me exact instructions on how to connect to this on my phone using Tailscale on this device and my phone
```

## What Creatures Can Do

CreatureOS is meant for ongoing work, not one-off prompting. A creature can:

- copy-edit a book chapter and keep the next revision alive
- audit a repo or server config and come back with concrete follow-ups
- monitor logs, configs, and security drift and flag what needs attention
- review a UI, suggest improvements, and help implement cleaner interactions
- maintain a technical worklist across bugs, follow-ups, and unfinished refactors
- draft and post on social media for you from the machine where your tools live
- organize long-running research and keep the open threads worth returning to
- maintain an ongoing worklist instead of forgetting what was open
- remember unresolved threads when you return tomorrow
- learn a habit once the shape of repeated work is actually clear

## A Little More Context

CreatureOS gives you a small habitat of durable creatures that can:
- chat in a web UI
- keep memory, notes, and worklists
- practice habits over time
- work with local files, attachments, and browser flows
- keep their own private workshop files and scripts

CreatureOS is intentionally opinionated:
- chats get fresh Codex threads
- habit runs stay on a persistent creature thread
- state lives locally in SQLite
- creatures use purpose and habits as their authority
- The Keeper helps shape the rest of the habitat

## Useful Startup Variants

Serve on localhost plus the detected Tailscale IPv4:

```bash
creatureos serve --tailscale
```

Force a fresh onboarding environment scan on boot:

```bash
creatureos serve --force-scan
```

Anchor creature file work to a specific directory:

```bash
creatureos serve --workspace /path/to/work
```

## Where CreatureOS Stores Things

If you do not pass `--workspace`, CreatureOS uses your current working directory as the primary working root for creature file work.

The onboarding scan is broader than that. It looks across likely work directories on the machine so The Keeper can form a first impression of the kind of work you do.

Runtime state lives in a user-local data directory:

- Linux: `~/.local/state/creatureos`
- macOS: `~/Library/Application Support/CreatureOS`
- Windows: `%LOCALAPPDATA%\\CreatureOS`

You can override that with:
- `creatureos --data-dir /path/to/data serve`
- `CREATURE_OS_DATA_DIR`
- `CREATURE_OS_DB_PATH`

## Development

If you are working from a checkout instead of PyPI:

```bash
python3 -m pip install -e .
```

Common commands:

```bash
python3 -m py_compile creatureos/cli.py creatureos/web.py creatureos/service.py creatureos/storage.py creatureos/config.py creatureos/codex_cli.py
node --check creatureos/static/creature_os.js
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

## Advanced Configuration

Environment variables:

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

Notes:

- `serve` runs a small supervisor that restarts the worker when core runtime files change.
- Static JS and CSS are served with revalidation headers to avoid stale browser state.
- If a server is already running for the same CreatureOS data directory, a second `serve` exits instead of starting a duplicate process.

## Contributing

If you want to work on CreatureOS itself, see [CONTRIBUTING.md](CONTRIBUTING.md).

## License

CreatureOS is licensed under Apache 2.0. See [LICENSE](LICENSE).
