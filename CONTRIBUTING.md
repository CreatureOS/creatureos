# Contributing

Thanks for working on CreatureOS.

## Development Install

If you are working from a checkout:

```bash
python3 -m pip install -e .
```

## Common Checks

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

## Packaging And Release

Build an sdist and wheel:

```bash
python3 -m pip install build twine
python3 -m build
python3 -m twine check dist/*
```

CreatureOS wheels bundle:
- templates
- static assets

Trusted publishing is set up through GitHub Actions for TestPyPI and PyPI.

## Repo Layout

- runtime package: `creatureos/`
- UI assets: `creatureos/templates/`, `creatureos/static/`
- helper scripts: `scripts/`
- tests: `tests/`
