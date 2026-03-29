# CreatureOS Creature Notes

This repo is a local-first runtime for persistent Codex creatures. Treat this file as the project-specific source of truth for how CreatureOS is supposed to behave when you make changes.

## Product model

- CreatureOS is not a generic chat app. It is a habitat for durable creatures with a purpose, memory, worklist, chats, and teachable habits.
- Creatures should feel like teammates, not rigid job titles. Prefer first-person language and soft preferences over fixed professional identity labels.
- The Keeper is special:
  - it is the permanent built-in presence
  - it stays pinned at the top of the creature list
  - it helps summon other creatures conservatively
  - it should review how the human is using CreatureOS and think about both new creatures and better ways to use the app

## Runtime model

- Each creature has one persistent background thread for habit runs, introductions, and reflective follow-up.
- Each chat gets its own Codex thread.
- Durable state lives in SQLite-backed records plus one authored purpose surface. Do not add alternate shadow state unless there is a strong reason.
- `creatureos serve` is the normal startup path. It should create the data dir if needed, initialize SQLite if needed, prepare first-run runtime state, and then start the web app. Do not drift back toward a separate required setup step for normal users.
- Current durable surfaces:
  - `Purpose`: authored identity and commitments
  - `memory`: structured memory records rendered as a view, including human instructions/preferences, learned routines, and lighter contextual notes
  - `worklist`: structured agenda + backlog rendered as one view

## Habit behavior

Background habit runs should follow this model:

- Bootstrap grounds the creature and sets up durable state, but does not publish the public intro by itself.
- The first completed non-bootstrap activity run becomes the creature's `Introduction`.
- Every habit run should:
  - review what changed since the last completed relevant background run
  - scan relevant user conversation turns since that last run
  - update memory when the human stated a stable instruction, preference, decision, or constraint
  - store or refine a routine memory when recurring background work now has a stable trigger, approach, and success condition
  - explicitly correct or supersede stale memory when new user instructions conflict with what is already stored
  - include dates in memory updates when relevant
  - do work according to the purpose, memory, worklist, and the current workspace
  - write a first-person markdown activity report
  - leave `message` empty and `should_notify = false` when nothing meaningfully changed for the owner

Keeper-specific behavior:

- When revisited, The Keeper should review the current creatures and ask whether they are serving the human well.
- Think conservatively about whether a new creature would genuinely help.
- If a new creature is not clearly warranted, prefer suggesting a better way to use CreatureOS or a habit the current creatures could learn.

## Chat behavior

- Each conversation uses a fresh Codex thread on first send.
- First turn in a chat may get fuller context injection.
- Follow-up turns should use lighter reinjection, not a full repeated context dump. Prefer a compact state view centered on purpose, human instructions/preferences, routines, and the active worklist.
- If a chat is spawned from activity, preserve that source context.
- Prefer relevance-filtered memory injection over dumping every stored memory into every run.

## Writing rules

- Creatures should speak in first person.
- Avoid third-person self-reference like `Watch Wren found...` when the creature is speaking for itself.
- Avoid over-introducing or re-introducing the creature in normal flows.
- Intros should be short, plain, and grounded in the creature's purpose.
- Welcome and onboarding copy can be a little more guided, but should still stay natural.

## UI expectations

- Simplicity matters. Avoid adding extra surface area unless it clearly helps the MVP.
- The Keeper should not look like a normal creature surface. Reuse the onboarding-style conversation feel where that helps.
- The sidebar should stay lean:
  - The Keeper first
  - most recently opened non-Keeper creature second
- Selected creature and selected chat should be clearly highlighted.
- Tooltips should be solid and opaque enough to sit above the scene art.
- Typing/typewriter behavior is limited to onboarding-style Keeper messages.

## Existing patterns to preserve

- Prefer extending the current `creatureos/service.py` and `creatureos/storage.py` pipeline instead of creating a second scheduler or alternate summary system.
- Prefer host-applied structured outputs over freeform hidden state.
- If a change affects chat behavior, check the interaction between:
  - `_prepare_run`
  - `_run_prompt`
  - `_execute_prepared_run`
  - `send_user_message`

## Verification

There is a real pytest suite now. For meaningful changes, prefer running:

```bash
python3 -m pytest
python3 -m py_compile creatureos/service.py creatureos/web.py creatureos/storage.py creatureos/cli.py creatureos/config.py creatureos/codex_cli.py
node --check creatureos/static/creature_os.js
```

If you are touching packaging or the storage layer, also consider:

```bash
python3 scripts/check_storage_sql.py
python3 scripts/storage_smoke.py
```

Health check:

```bash
curl http://127.0.0.1:404/healthz
```

If you need a browser-level check in this environment, prefer:

```bash
CREATURE_OS_RUN_BROWSER_SMOKE=1 python3 -m pytest -m browser
```

If that path is unavailable and you need a raw fallback, use Playwright via Node with Chromium sandboxing disabled, for example:

```bash
node - <<'NODE'
const { chromium } = require('playwright');
(async () => {
  const browser = await chromium.launch({ headless: true, chromiumSandbox: false });
  const page = await browser.newPage();
  await page.goto('http://127.0.0.1:404', { waitUntil: 'networkidle' });
  console.log(await page.title());
  await browser.close();
})();
NODE
```

## Useful runtime facts

- Main app modules:
  - `creatureos/service.py`
  - `creatureos/web.py`
  - `creatureos/storage.py`
  - `creatureos/cli.py`
  - `creatureos/config.py`
  - `creatureos/codex_cli.py`
- Templates live in `creatureos/templates/`
- Frontend assets live in `creatureos/static/`
- Health endpoint is `/healthz`
- Local default URL is `http://127.0.0.1:404`
- Contributor workflow, packaging notes, and release details live in `CONTRIBUTING.md`
- Public package publishing is handled through GitHub Actions trusted publishing to TestPyPI and PyPI

## When in doubt

- Prefer the durable state model over clever prompt tricks.
- Prefer fewer concepts and clearer behavior over more knobs.
- If a creature behavior feels too synthetic, too repetitive, or too system-y, simplify it and make it sound more like a teammate.
