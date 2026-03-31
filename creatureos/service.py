from __future__ import annotations

import getpass
import hashlib
import heapq
import json
import mimetypes
import os
import platform
import random
import re
import shutil
import threading
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from time import monotonic
from typing import Any, Callable, Mapping, Sequence
from zoneinfo import ZoneInfo, available_timezones

from . import config
from . import storage
from .codex_cli import CodexCommandError, CodexTimeoutError, resume_thread, start_thread

OWNER_REFERENCE_KEY = "owner_reference"
DEFAULT_CREATURE_MODEL_KEY = "default_creature_model"
DEFAULT_CREATURE_REASONING_EFFORT_KEY = "default_creature_reasoning_effort"
DISPLAY_TIMEZONE_KEY = "display_timezone"
ECOSYSTEM_KEY = "ecosystem"
ONBOARDING_PHASE_KEY = "onboarding_phase"
ONBOARDING_THREAD_ID_KEY = "onboarding_thread_id"
ONBOARDING_BRIEFING_KEY = "onboarding_briefing_json"
ONBOARDING_ANSWERS_KEY = "onboarding_answers_json"
ONBOARDING_ENVIRONMENT_KEY = "onboarding_environment_json"
ONBOARDING_CHAT_KEY = "onboarding_chat_json"
ONBOARDING_CHAT_FEED_KEY = "onboarding_chat_feed_json"
ONBOARDING_STARTER_FEED_KEY = "onboarding_starter_feed_json"
CODEX_ACCESS_STATE_KEY = "codex_access_state_json"
ONBOARDING_KEEPER_NAME = "The Keeper"
SUMMON_CREATURE_SIGNAL = "[[SUMMON_CREATURE]]"
ONBOARDING_ENVIRONMENT_VERSION = 11
ONBOARDING_BRIEFING_VERSION = 14
CODEX_RATE_LIMIT_FILE_SCAN_LIMIT = 48
CODEX_RATE_LIMIT_TAIL_BYTES = 256 * 1024
CODEX_RATE_LIMIT_TAIL_LINES = 1200
RUN_SCOPE_ACTIVITY = "activity"
RUN_SCOPE_CHAT = "chat"
LAST_VIEWED_CREATURE_KEY = "last_viewed_creature_slug"
LAST_VIEWED_HABIT_KEY_PREFIX = "last_viewed_habit:"
MAX_THINKING_MODEL_CHARS = 80
BUSY_ACTION_QUEUE = "queue"
BUSY_ACTION_STEER = "steer"
MAX_CONVERSATION_MESSAGES = 10
MAX_FOLLOWUP_CONVERSATION_MESSAGES = 4
MAX_MESSAGE_ATTACHMENTS = 8
MAX_MESSAGE_ATTACHMENT_BYTES = 20 * 1024 * 1024
MAX_MESSAGE_ATTACHMENT_TOTAL_BYTES = 40 * 1024 * 1024
MESSAGE_ATTACHMENT_DIRNAME = "message-attachments"
WORKSHOP_DIRNAME = "workshop"
WORKSHOP_SCRIPTS_DIRNAME = "scripts"
WORKSHOP_REPORTS_DIRNAME = "reports"
WORKSHOP_FILES_DIRNAME = "files"
WORKSHOP_STATE_DIRNAME = "state"
WORKSHOP_TEMPLATES_DIRNAME = "templates"
WORKSHOP_BROWSER_DIRNAME = "browser"
WORKSHOP_BROWSER_PROFILE_DIRNAME = "profile"
WORKSHOP_BROWSER_DOWNLOADS_DIRNAME = "downloads"
WORKSHOP_BROWSER_CAPTURES_DIRNAME = "captures"
NEW_CHAT_TITLE = "New chat"
NEW_CONVERSATION_TITLE = NEW_CHAT_TITLE
NEW_HABIT_CHAT_TITLE = "New habit"
MAX_CONVERSATION_TITLE_CHARS = 32
KEEPER_SUMMON_CHAT_TITLE = "Summon a creature"
INTRODUCTION_CHAT_TITLE = "Introduction"
INTRO_SURFACED_META_PREFIX = "intro_surfaced:"
AUTO_SPAWN_PRIORITIES = {"high", "critical"}
MAX_STANDING_MESSAGE_CHARS = 1200
MAX_ACTIVITY_MARKDOWN_CHARS = 8000
MAX_ACTIVITY_DELTA_ITEMS = 12
MAX_KEEPER_ACTIVITY_DELTA_ITEMS = 18
MEMORY_STALE_DAYS = 21
AGENDA_PRIORITY_ORDER = {"critical": 0, "high": 1, "medium": 2, "low": 3}
SEVERITY_ORDER = {"critical": 0, "warning": 1, "info": 2}
MEMORY_KIND_ORDER = {
    "instruction": 0,
    "preference": 1,
    "decision": 2,
    "constraint": 3,
    "routine": 4,
    "context": 5,
    "note": 6,
}
MEMORY_USER_KIND_ORDER = ("instruction", "preference", "decision", "constraint")
MEMORY_ROUTINE_KIND_ORDER = ("routine",)
MEMORY_CONTEXT_KIND_ORDER = ("context", "note")
ACTIVE_MEMORY_STATUSES = {"active"}
INACTIVE_MEMORY_STATUSES = {"superseded", "revoked", "deleted"}
HABIT_SCHEDULE_MANUAL = "manual"
HABIT_SCHEDULE_INTERVAL = "interval"
HABIT_SCHEDULE_DAILY = "daily"
HABIT_SCHEDULE_TIMES_PER_DAY = "times_per_day"
HABIT_SCHEDULE_AFTER_CHAT = "after_chat"
HABIT_SCHEDULE_KINDS = {
    HABIT_SCHEDULE_MANUAL,
    HABIT_SCHEDULE_INTERVAL,
    HABIT_SCHEDULE_DAILY,
    HABIT_SCHEDULE_TIMES_PER_DAY,
    HABIT_SCHEDULE_AFTER_CHAT,
}
HABIT_SCHEDULE_OPTIONS = (
    {"value": HABIT_SCHEDULE_INTERVAL, "label": "Every N minutes"},
    {"value": HABIT_SCHEDULE_DAILY, "label": "Every day at a set time"},
    {"value": HABIT_SCHEDULE_TIMES_PER_DAY, "label": "Several times a day"},
    {"value": HABIT_SCHEDULE_AFTER_CHAT, "label": "After chat quiet time"},
    {"value": HABIT_SCHEDULE_MANUAL, "label": "Manual only"},
)
DEFAULT_HABIT_WINDOW_START = "06:00"
DEFAULT_HABIT_WINDOW_END = "20:00"
DEFAULT_PONDER_DELAY_MINUTES = 120
PONDER_HABIT_SLUG = "ponder"
PONDER_HABIT_TITLE = "Ponder"
PONDER_REQUEST_KIND = "ponder"
PURPOSE_DOC_KEY = "purpose"
MEMORY_STATE_KEY = "memory"
WORKLIST_STATE_KEY = "worklist"
STATE_SURFACE_ORDER = (PURPOSE_DOC_KEY, MEMORY_STATE_KEY, WORKLIST_STATE_KEY)
STATE_SURFACE_SPECS: dict[str, dict[str, str]] = {
    PURPOSE_DOC_KEY: {
        "title": "Purpose",
        "source": "SQLite / durable purpose text",
        "description": "The creature's authored purpose and core commitments.",
    },
    MEMORY_STATE_KEY: {
        "title": "Memory",
        "source": "SQLite / memory_records",
        "description": "Stable human instructions and preferences, plus learned recurring routines, rendered from structured memory records.",
    },
    WORKLIST_STATE_KEY: {
        "title": "Worklist",
        "source": "SQLite / agenda_items + backlog_items",
        "description": "Structured work state rendered from active agenda items and parked backlog items.",
    },
}
DISPLAY_TIMEZONE_FALLBACK = "UTC"
DISPLAY_TIMEZONE_OPTIONS = tuple(
    sorted(
        zone
        for zone in available_timezones()
        if "/" in zone or zone in {"UTC", "Etc/UTC"}
    )
)
DISPLAY_TIMEZONE_INDEX = {zone.casefold(): zone for zone in DISPLAY_TIMEZONE_OPTIONS}
THINKING_MODEL_CHOICES_BASE: tuple[dict[str, str], ...] = (
    {"value": "gpt-5.4", "label": "GPT-5.4"},
    {"value": "gpt-5", "label": "GPT-5"},
    {"value": "gpt-5-mini", "label": "GPT-5 mini"},
    {"value": "gpt-5-nano", "label": "GPT-5 nano"},
)
_DISPLAY_TIMEZONE_NAME_CACHE: str | None = None
_DISPLAY_TIMEZONE_CACHE: ZoneInfo | None = None
_CODEX_RATE_LIMIT_CACHE: dict[str, Any] | None = None
_CODEX_RATE_LIMIT_CACHE_AT: datetime | None = None
_CODEX_RATE_LIMIT_CACHE_LOCK = threading.Lock()
_CODEX_MODEL_CHOICES_CACHE: list[dict[str, str]] | None = None
_CODEX_MODEL_CHOICES_CACHE_AT: datetime | None = None
_CODEX_MODEL_CHOICES_CACHE_LOCK = threading.Lock()
_RUN_FEED_ACTIVE_TURNS: dict[int, float] = {}
_RUN_FEED_ACTIVE_TURNS_LOCK = threading.Lock()
_RUNTIME_INITIALIZED = False
_RUNTIME_INIT_LOCK = threading.Lock()
DEFAULT_OWNER_MODE = storage.DEFAULT_OWNER_MODE
DEFAULT_OWNER_REFERENCE = "my human"
DEFAULT_ECOSYSTEM = "woodlands"
DEFAULT_ONBOARDING_PHASE = "ecosystem"
OWNER_REFERENCE_OPTIONS = (
    "my human",
    "my manager",
    "my owner",
    "my master",
)
ECOSYSTEMS: tuple[dict[str, str], ...] = (
    {
        "value": "woodlands",
        "label": "The Woodlands",
        "color": "#141b12",
        "description": "Forest creatures from dens, groves, warrens, and moonlit trails.",
        "scene": "/static/woodlands-scene.svg",
        "graphic": "/static/creatureos-mark.png",
    },
    {
        "value": "sea",
        "label": "The Sea",
        "color": "#071e34",
        "description": "Sea creatures from reefs, currents, shoals, and deepwater coves.",
        "scene": "/static/ocean-scene.svg",
        "graphic": "/static/ocean-picker-icon.svg",
    },
    {
        "value": "boneyard",
        "label": "The Boneyard",
        "color": "#0f1118",
        "description": "Ghosts and undead from boneyards, crypts, mausoleums, and the fog between headstones.",
        "scene": "/static/spooky-scene.svg",
        "graphic": "/static/spooky-picker-icon.svg",
    },
    {
        "value": "monster-wilds",
        "label": "The Monster Wilds",
        "color": "#1b120f",
        "description": "Famous monsters from lairs, cliffs, hoards, roosts, and storm-dark caverns.",
        "scene": "/static/monsters-scene.svg",
        "graphic": "/static/monsters-picker-icon.svg",
    },
    {
        "value": "expanse",
        "label": "The Expanse",
        "color": "#09131a",
        "description": "Off-world creatures from strange skies, spore blooms, and drifting alien collectives.",
        "scene": "/static/alien-scene.svg",
        "graphic": "/static/alien-picker-icon.svg",
    },
    {
        "value": "terminal",
        "label": "The Terminal",
        "color": "#060905",
        "description": "Machine creatures from terminals, relays, racks, and phosphor-lit grids.",
        "scene": "/static/tech-scene.svg",
        "graphic": "/static/tech-picker-icon.svg",
    },
)
ECOSYSTEM_INDEX = {item["value"]: item for item in ECOSYSTEMS}
ECOSYSTEM_NAMING_WORLDS: Mapping[str, str] = {
    "woodlands": (
        "The Woodlands are a world of groves, warrens, moss, lantern light, patient trails, and creatures that live close to roots, dens, canopies, and hidden paths. "
        "Creatures from here should feel like they belong to an old living forest: alert, sheltering, watchful, soft-footed, or quietly uncanny."
    ),
    "sea": (
        "The Sea is a world of currents, reefs, coves, shoals, undertow, bells in fog, dark water, and creatures shaped by tide and depth. "
        "Creatures from here should feel tidal, brined, drifting, submerged, luminous, or carried by the pull of open water."
    ),
    "boneyard": (
        "The Boneyard is a world of crypts, grave soil, mausoleums, fog, old stone, and creatures that linger where memory refuses to stay buried. "
        "Creatures from here should feel haunted, death-touched, solemn, revenant, dusted with age, or sharpened by what the world tried to forget."
    ),
    "monster-wilds": (
        "The Monster Wilds are a world of lairs, cliffs, hoards, roosts, caves, storms, and creatures too large, hungry, proud, or strange to be made tame. "
        "Creatures from here should feel feral, mythic, hungry, winged, scaled, tusked, clawed, or born from appetite and weather."
    ),
    "expanse": (
        "The Expanse is a world of strange skies, spores, drifting colonies, alien bloom, cold distance, and creatures that do not belong to ordinary earthbound life. "
        "Creatures from here should feel distant, off-world, strange, floating, collective, bioluminescent, or adapted to vastness and unfamiliar light."
    ),
    "terminal": (
        "The Terminal is a world of phosphor glow, relays, racks, grids, old machine discipline, humming lines, and creatures born close to signal and infrastructure. "
        "Creatures from here should feel synthetic, wired, metallic, process-shaped, precise, relay-bound, or sharpened by the machine's logic."
    ),
}
KEEPER_ECOSYSTEM_INVOCATIONS: Mapping[str, tuple[str, ...]] = {
    "woodlands": (
        "Ahhhh, **the Woodlands**. I come here often to listen for the old paths between need and purpose.",
        "**The Woodlands** again. A good place for quiet truths, patient things, and creatures that know how to emerge at the right hour.",
        "Ah, **the Woodlands**. There is peace here, but never emptiness.",
    ),
    "sea": (
        "Ahhhh, **the Sea**. I know these tides well; they bring buried desires to the surface in their own time.",
        "**The Sea**. A place of currents, undertow, and creatures that arrive when the tide turns.",
        "Ah, **the Sea**. I come here when the deeper wants are still moving under the surface.",
    ),
    "boneyard": (
        "Ahhhh, **the Boneyard**. Old things speak clearly here, and neglected needs do not stay buried for long.",
        "**The Boneyard**. A solemn place, but honest. Even forgotten purposes rise again here.",
        "Ah, **the Boneyard**. I have always liked how plainly this place remembers what others try to hide.",
    ),
    "monster-wilds": (
        "Ahhhh, **the Monster Wilds**. Desire grows teeth here, and the right creature rarely arrives looking tame.",
        "**The Monster Wilds**. A fierce place for hungers too large to keep politely hidden.",
        "Ah, **the Monster Wilds**. I come here when the need has claws and refuses to be ignored.",
    ),
    "expanse": (
        "Ahhhh, **the Expanse**. A fine place for vast ambitions, strange questions, and creatures that do not fear distance.",
        "**The Expanse**. Here the horizon is never the end of things, only the start of a larger summons.",
        "Ah, **the Expanse**. I come here when the desire is too large for ordinary rooms.",
    ),
    "terminal": (
        "Ahhhh, **the Terminal**. I know this phosphor hush well; it is where intention sharpens into signal.",
        "**The Terminal**. A place of relay-light, old discipline, and creatures that answer precise callings.",
        "Ah, **the Terminal**. I come here when the work hums close to the machine.",
    ),
}
ONBOARDING_ECOSYSTEM_SUPPORT_ASSETS: dict[str, tuple[str, ...]] = {
    "woodlands": (
        "/static/woodlands-branches-dark.svg",
        "/static/woodlands-eagle-soft.svg",
        "/static/woodlands-owl-soft.svg",
        "/static/woodlands-conifer-deep.svg",
        "/static/woodlands-conifer-soft.svg",
        "/static/woodlands-birds.svg",
        "/static/woodlands-rabbit-soft.svg",
        "/static/woodlands-beaver-soft.svg",
        "/static/woodlands-deer-soft.svg",
        "/static/woodlands-fox-soft.svg",
        "/static/woodlands-trees-mid-clipped-strong.png",
    ),
    "monster-wilds": (
        "/static/monsters-scene.svg",
    ),
    "boneyard": (
        "/static/spooky-gate.svg",
        "/static/spooky-midnight-graveyard.svg",
        "/static/spooky-tombstone.svg",
        "/static/spooky-zombie-hand.svg",
        "/static/spooky-hasty-grave.svg",
        "/static/spooky-bone.svg",
        "/static/spooky-flying-ghost.svg",
    ),
    "sea": (
        "/static/ocean-source-coral.svg",
        "/static/ocean-source-algae.svg",
        "/static/ocean-source-seaweed.svg",
        "/static/ocean-source-seaweed-tall.svg",
        "/static/ocean-source-crab.svg",
        "/static/ocean-source-starfish.svg",
        "/static/ocean-source-clam-shell.svg",
        "/static/ocean-source-oyster-shell.svg",
        "/static/ocean-source-whale.svg",
        "/static/ocean-source-fish-muted.png",
        "/static/ocean-source-fish-muted-flip.png",
        "/static/ocean-source-shark-muted.png",
        "/static/ocean-source-hammerhead-muted.png",
        "/static/ocean-source-seahorse.svg",
        "/static/ocean-source-octopus-muted.png",
    ),
    "expanse": (
        "/static/expanse-bridge-scene.svg",
        "/static/expanse-hubble-xdf.jpg",
    ),
    "terminal": (
        "/static/tech-web-delivery-drone.svg",
        "/static/tech-web-walking-scout.svg",
        "/static/tech-web-tracked-robot.svg",
    ),
}
KEEPER_SYSTEM_ROLE = "keeper"
KEEPER_SLUG = "keeper"
KEEPER_CONVERSATION_TITLE = "Keeper's desk"
WELCOME_CONVERSATION_TITLE = "Welcome to CreatureOS"
INTERRUPTED_RUN_ERROR_TEXT = "Run interrupted because the CreatureOS process restarted or lost its worker thread."
CONVERSATION_RESET_RUN_ERROR_TEXT = "Run stopped because its conversation was reset before the reply finished."
ONBOARDING_RESTART_RUN_ERROR_TEXT = "Run stopped because onboarding was restarted and the previous Keeper conversation was discarded."
ECOSYSTEM_RESET_RUN_ERROR_TEXT = "Run stopped because the CreatureOS ecosystem was reset."
_ACTIVE_RUN_THREADS: dict[int, threading.Thread] = {}
_ACTIVE_RUN_THREADS_LOCK = threading.Lock()
_CODEX_ACCESS_PROBE_LOCK = threading.Lock()
DEFAULT_TEMPERAMENT = "Quiet Observer"
TEMPERAMENT_OPTIONS = (
    "Quiet Observer",
    "Curious Investigator",
    "Aggressive Hunter",
    "Careful Archivist",
)
NAME_STOPWORDS = {
    "that",
    "this",
    "with",
    "from",
    "into",
    "about",
    "should",
    "would",
    "could",
    "creature",
    "creature",
    "creatures",
    "purpose",
    "local",
    "creatureos",
    "need",
    "needs",
    "want",
    "wants",
    "please",
    "create",
    "make",
    "study",
    "keep",
    "keeps",
    "studies",
    "study",
    "opens",
    "open",
    "help",
    "helps",
    "serve",
    "serves",
    "serving",
    "alerts",
    "alerts",
    "alerting",
    "reviews",
    "reviewing",
    "perform",
    "performs",
    "performing",
    "handle",
    "handles",
    "handling",
    "explain",
    "explains",
    "explaining",
    "suggest",
    "suggests",
    "suggesting",
    "only",
    "when",
    "worth",
    "your",
    "they",
    "them",
    "their",
    "habit",
    "habits",
}
CREATURE_ECOSYSTEM_KEYWORDS: dict[str, set[str]] = {
    "woodlands": {
        "woodland", "woods", "forest", "grove", "glade", "meadow", "burrow", "den", "roost", "warren",
        "fox", "owl", "badger", "rabbit", "squirrel", "beaver", "raven", "otter",
    },
    "monster-wilds": {
        "monster", "monstrous", "wilds", "lair", "hoard", "brood", "hydra", "kraken", "dragon", "chimera",
        "basilisk", "roc", "troll", "cyclops", "goblin", "orc", "gremlin",
    },
    "boneyard": {
        "boneyard", "grave", "crypt", "barrow", "mausoleum", "haunt", "undead", "ghost", "zombie", "ghoul",
        "shade", "banshee", "vampire", "skeleton", "wraith", "specter", "phantom", "lich", "reaper",
    },
    "expanse": {
        "expanse", "space", "cosmic", "alien", "void", "star", "nebula", "orbit", "cephalid", "mantid",
        "saurian", "myconid", "sporekin", "silicate", "voidling", "glider", "cluster", "ring",
    },
    "sea": {
        "sea", "ocean", "tidal", "tide", "marine", "reef", "shoal", "kelp", "brine", "cove",
        "octopus", "dolphin", "whale", "ray", "shark", "seal", "seahorse", "jellyfish",
    },
    "terminal": {
        "terminal", "machine", "digital", "cyber", "robot", "array", "grid", "relay", "matrix", "rack",
        "droid", "automaton", "drone", "synth", "sentinel", "mech", "crawler", "android",
    },
}
ROLE_SUMMARY_FORBIDDEN_PHRASES = (
    "habitat",
    "temperament",
    "origin ecosystem",
    "first impression",
    "before i go deeper",
    "before i burrow any deeper",
    "burrow",
)
ROLE_NORMALIZATION_TERMS = {
    "animal",
    "beast",
    "bot",
    "creature",
    "ecosystem",
    "habitat",
    "mascot",
}
ROLE_DESCRIPTOR_FORBIDDEN_WORDS = {
    "animal",
    "beast",
    "bot",
    "creature",
    "ecosystem",
    "habitat",
    "mascot",
}
ROLE_NAME_LEAK_WORDS = {
    "agent",
    "assistant",
    "bot",
    "builder",
    "creature",
    "editor",
    "guide",
    "helper",
    "keeper",
    "manager",
    "operator",
    "service",
    "watcher",
    "writer",
}
GENERATED_NAME_OPERATIONAL_WORDS = {
    "agenda",
    "artifact",
    "artifacts",
    "backlog",
    "brief",
    "briefing",
    "briefings",
    "bug",
    "bugs",
    "context",
    "contexts",
    "coordination",
    "deliverable",
    "deliverables",
    "doc",
    "docs",
    "goal",
    "goals",
    "intent",
    "intents",
    "issue",
    "issues",
    "milestone",
    "milestones",
    "objective",
    "objectives",
    "plan",
    "plans",
    "priority",
    "priorities",
    "project",
    "projects",
    "queue",
    "queues",
    "repo",
    "repos",
    "request",
    "requests",
    "scope",
    "scopes",
    "software",
    "spec",
    "specs",
    "state",
    "states",
    "status",
    "summary",
    "summaries",
    "system",
    "systems",
    "task",
    "tasks",
    "team",
    "teams",
    "ticket",
    "tickets",
    "todo",
    "todos",
    "workflow",
    "workflows",
    "worklist",
    "workspace",
    "workspaces",
}
ECOSYSTEM_DESCRIPTOR_FORBIDDEN_WORDS = set(ROLE_DESCRIPTOR_FORBIDDEN_WORDS).union(
    *(keywords for keywords in CREATURE_ECOSYSTEM_KEYWORDS.values()),
)


def _row_to_dict(row: Any | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return dict(row)


def _row_value(row: Any, key: str, default: Any = None) -> Any:
    if row is None:
        return default
    if isinstance(row, dict):
        return row.get(key, default)
    try:
        return row[key]
    except (KeyError, IndexError, TypeError):
        return default


def _allow_repo_edits(creature: Any, *, requested: bool) -> bool:
    return bool(requested)


def _allow_purpose_updates(creature: Any) -> bool:
    return not _is_keeper_creature(creature)


def _parse_json(raw: str | None) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return data if isinstance(data, dict) else {}


def _owner_reference_choices() -> list[dict[str, str]]:
    return [{"value": value, "label": value} for value in OWNER_REFERENCE_OPTIONS]


def _ecosystem_choices() -> list[dict[str, str]]:
    return [{"value": item["value"], "label": item["label"]} for item in ECOSYSTEMS]


def _canonical_display_timezone_name(value: str | None) -> str:
    cleaned = str(value or "").strip()
    if not cleaned:
        return ""
    direct = DISPLAY_TIMEZONE_INDEX.get(cleaned.casefold())
    if direct:
        return direct
    normalized = cleaned.replace(" ", "_")
    return DISPLAY_TIMEZONE_INDEX.get(normalized.casefold(), "")


def _detect_system_timezone_name() -> str:
    candidates: list[str] = []
    env_tz = str(os.environ.get("TZ") or "").strip()
    if env_tz:
        candidates.append(env_tz)
    timezone_file = Path("/etc/timezone")
    if timezone_file.exists():
        try:
            timezone_value = timezone_file.read_text(encoding="utf-8").strip()
        except OSError:
            timezone_value = ""
        if timezone_value:
            candidates.append(timezone_value)
    localtime_path = Path("/etc/localtime")
    if localtime_path.is_symlink():
        try:
            resolved = localtime_path.resolve()
            zoneinfo_root = Path("/usr/share/zoneinfo")
            if zoneinfo_root in resolved.parents:
                candidates.append(str(resolved.relative_to(zoneinfo_root)))
        except OSError:
            pass
    local_tz = datetime.now().astimezone().tzinfo
    local_zone_key = str(getattr(local_tz, "key", "") or "").strip()
    if local_zone_key:
        candidates.append(local_zone_key)
    local_zone_name = str(datetime.now().astimezone().tzname() or "").strip()
    if local_zone_name:
        candidates.append(local_zone_name)
    for candidate in candidates:
        canonical = _canonical_display_timezone_name(candidate)
        if canonical:
            return canonical
    return DISPLAY_TIMEZONE_FALLBACK


def _set_display_timezone_cache(name: str) -> str:
    global _DISPLAY_TIMEZONE_NAME_CACHE, _DISPLAY_TIMEZONE_CACHE
    canonical = _canonical_display_timezone_name(name) or DISPLAY_TIMEZONE_FALLBACK
    _DISPLAY_TIMEZONE_NAME_CACHE = canonical
    try:
        _DISPLAY_TIMEZONE_CACHE = ZoneInfo(canonical)
    except Exception:
        _DISPLAY_TIMEZONE_NAME_CACHE = DISPLAY_TIMEZONE_FALLBACK
        _DISPLAY_TIMEZONE_CACHE = ZoneInfo(DISPLAY_TIMEZONE_FALLBACK)
    return _DISPLAY_TIMEZONE_NAME_CACHE


def _ensure_display_timezone_storage() -> str:
    stored = _canonical_display_timezone_name(storage.get_meta(DISPLAY_TIMEZONE_KEY))
    if not stored:
        stored = _detect_system_timezone_name()
        storage.set_meta(DISPLAY_TIMEZONE_KEY, stored)
    return _set_display_timezone_cache(stored)


def get_display_timezone_name() -> str:
    if _DISPLAY_TIMEZONE_NAME_CACHE and _DISPLAY_TIMEZONE_CACHE is not None:
        return _DISPLAY_TIMEZONE_NAME_CACHE
    return _ensure_display_timezone_storage()


def get_display_timezone() -> ZoneInfo:
    if _DISPLAY_TIMEZONE_CACHE is not None:
        return _DISPLAY_TIMEZONE_CACHE
    _ensure_display_timezone_storage()
    return _DISPLAY_TIMEZONE_CACHE or ZoneInfo(DISPLAY_TIMEZONE_FALLBACK)


def _timezone_offset_minutes(zone_name: str) -> int:
    try:
        zone = ZoneInfo(zone_name)
    except Exception:
        return 0
    current_utc = datetime.now(timezone.utc)
    offset = zone.utcoffset(current_utc)
    return int(offset.total_seconds() // 60) if offset is not None else 0


def _format_utc_offset_label(offset_minutes: int) -> str:
    sign = "+" if offset_minutes >= 0 else "-"
    absolute_minutes = abs(int(offset_minutes))
    hours, minutes = divmod(absolute_minutes, 60)
    return f"UTC{sign}{hours:02d}:{minutes:02d}"


def _display_timezone_choices() -> list[dict[str, str]]:
    ordered_zones = sorted(
        DISPLAY_TIMEZONE_OPTIONS,
        key=lambda zone: (_timezone_offset_minutes(zone), zone.replace("_", " ").casefold()),
    )
    return [
        {
            "value": zone,
            "label": f"{_format_utc_offset_label(_timezone_offset_minutes(zone))} · {zone.replace('_', ' ')}",
        }
        for zone in ordered_zones
    ]


def _display_timezone_state() -> dict[str, Any]:
    current = get_display_timezone_name()
    return {
        "current": current,
        "label": current.replace("_", " "),
        "choices": _display_timezone_choices(),
    }


def _codex_sessions_root() -> Path:
    return Path.home() / ".codex" / "sessions"


def _codex_models_cache_path() -> Path:
    return Path.home() / ".codex" / "models_cache.json"


def _codex_model_label(value: str) -> str:
    raw = " ".join(str(value or "").strip().split())
    if not raw:
        return ""
    if any(character.isupper() for character in raw):
        return raw
    parts = []
    for token in raw.split("-"):
        cleaned = token.strip()
        if not cleaned:
            continue
        lower = cleaned.lower()
        if lower == "gpt":
            parts.append("GPT")
        elif lower == "codex":
            parts.append("Codex")
        elif lower in {"mini", "max", "spark", "nano"}:
            parts.append(lower.capitalize())
        else:
            parts.append(cleaned)
    return "-".join(parts) or raw


def _read_codex_model_choices() -> list[dict[str, str]]:
    cache_path = _codex_models_cache_path()
    if not cache_path.exists():
        return [dict(item) for item in THINKING_MODEL_CHOICES_BASE]
    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return [dict(item) for item in THINKING_MODEL_CHOICES_BASE]
    raw_models = payload.get("models") if isinstance(payload, dict) else None
    if not isinstance(raw_models, list):
        return [dict(item) for item in THINKING_MODEL_CHOICES_BASE]
    items: list[tuple[int, str, dict[str, str]]] = []
    for raw in raw_models:
        if not isinstance(raw, dict):
            continue
        if str(raw.get("visibility") or "list").strip().lower() != "list":
            continue
        if raw.get("disabled"):
            continue
        value = _normalize_model_value(raw.get("slug"), allow_blank=True)
        if not value:
            continue
        label = _codex_model_label(raw.get("display_name") or value)
        try:
            priority = int(raw.get("priority"))
        except (TypeError, ValueError):
            priority = 9999
        items.append((priority, label.casefold(), {"value": value, "label": label}))
    if not items:
        return [dict(item) for item in THINKING_MODEL_CHOICES_BASE]
    items.sort(key=lambda item: (item[0], item[1]))
    deduped: list[dict[str, str]] = []
    seen: set[str] = set()
    for _, _, item in items:
        if item["value"] in seen:
            continue
        seen.add(item["value"])
        deduped.append(item)
    return deduped or [dict(item) for item in THINKING_MODEL_CHOICES_BASE]


def _codex_model_choices() -> list[dict[str, str]]:
    global _CODEX_MODEL_CHOICES_CACHE, _CODEX_MODEL_CHOICES_CACHE_AT
    with _CODEX_MODEL_CHOICES_CACHE_LOCK:
        if _CODEX_MODEL_CHOICES_CACHE_AT is not None and _CODEX_MODEL_CHOICES_CACHE is not None:
            age_seconds = (datetime.now(timezone.utc) - _CODEX_MODEL_CHOICES_CACHE_AT).total_seconds()
            if age_seconds < 60:
                return [dict(item) for item in _CODEX_MODEL_CHOICES_CACHE]
        choices = _read_codex_model_choices()
        _CODEX_MODEL_CHOICES_CACHE = [dict(item) for item in choices]
        _CODEX_MODEL_CHOICES_CACHE_AT = datetime.now(timezone.utc)
        return [dict(item) for item in choices]


def _format_percent_label(value: Any) -> str:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return "Unknown"
    rounded = round(numeric, 1)
    if rounded.is_integer():
        return f"{int(rounded)}%"
    return f"{rounded:.1f}%"


def _timestamp_from_unix_epoch(value: Any) -> str:
    try:
        seconds = int(value)
    except (TypeError, ValueError):
        return ""
    return datetime.fromtimestamp(seconds, timezone.utc).replace(microsecond=0).isoformat()


def _normalize_codex_rate_limit_window(name: str, payload: Any) -> dict[str, Any]:
    data = payload if isinstance(payload, dict) else {}
    used_percent = data.get("used_percent")
    try:
        used_value = max(0.0, min(100.0, float(used_percent)))
    except (TypeError, ValueError):
        used_value = 0.0
    left_value = max(0.0, min(100.0, 100.0 - used_value))
    resets_at_iso = _timestamp_from_unix_epoch(data.get("resets_at"))
    return {
        "key": name,
        "label": "5-hour" if name == "primary" else "Weekly",
        "used_percent": used_value,
        "used_percent_label": _format_percent_label(used_value),
        "left_percent": left_value,
        "left_percent_label": _format_percent_label(left_value),
        "window_minutes": int(data.get("window_minutes") or 0),
        "resets_at": resets_at_iso,
        "resets_at_display": _format_timestamp_display(resets_at_iso) if resets_at_iso else "Unknown",
        "resets_at_relative": _format_relative_time_display(resets_at_iso) if resets_at_iso else "Unknown",
    }


def _extract_codex_rate_limit_snapshot(line: str, *, path: Path, line_number: int) -> dict[str, Any] | None:
    try:
        payload = json.loads(line)
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict) or str(payload.get("type") or "") != "event_msg":
        return None
    inner = payload.get("payload") or {}
    if not isinstance(inner, dict) or str(inner.get("type") or "") != "token_count":
        return None
    rate_limits = inner.get("rate_limits") or {}
    if not isinstance(rate_limits, dict) or str(rate_limits.get("limit_id") or "") != "codex":
        return None
    primary = rate_limits.get("primary")
    secondary = rate_limits.get("secondary")
    if not isinstance(primary, dict) and not isinstance(secondary, dict):
        return None
    timestamp = str(payload.get("timestamp") or "").strip()
    return {
        "timestamp": timestamp,
        "timestamp_display": _format_timestamp_display(timestamp) if timestamp else "Unknown",
        "timestamp_relative": _format_relative_time_display(timestamp) if timestamp else "Unknown",
        "source_path": str(path),
        "source_line": int(line_number),
        "primary": _normalize_codex_rate_limit_window("primary", primary),
        "secondary": _normalize_codex_rate_limit_window("secondary", secondary),
    }


def _read_latest_codex_rate_limit_snapshot() -> dict[str, Any] | None:
    sessions_root = _codex_sessions_root()
    if not sessions_root.is_dir():
        return None
    try:
        session_files = heapq.nlargest(
            CODEX_RATE_LIMIT_FILE_SCAN_LIMIT,
            (path for path in sessions_root.rglob("*.jsonl") if path.is_file()),
            key=lambda path: path.stat().st_mtime,
        )
    except OSError:
        return None
    for path in session_files:
        try:
            with path.open("rb") as handle:
                handle.seek(0, os.SEEK_END)
                size = handle.tell()
                if size <= 0:
                    continue
                read_size = min(size, CODEX_RATE_LIMIT_TAIL_BYTES)
                handle.seek(-read_size, os.SEEK_END)
                lines = handle.read(read_size).decode("utf-8", errors="ignore").splitlines()[-CODEX_RATE_LIMIT_TAIL_LINES:]
        except OSError:
            continue
        for line_number in range(len(lines), 0, -1):
            snapshot = _extract_codex_rate_limit_snapshot(lines[line_number - 1], path=path, line_number=line_number)
            if snapshot is not None:
                return snapshot
    return None


def _codex_rate_limit_snapshot() -> dict[str, Any] | None:
    global _CODEX_RATE_LIMIT_CACHE, _CODEX_RATE_LIMIT_CACHE_AT
    with _CODEX_RATE_LIMIT_CACHE_LOCK:
        if _CODEX_RATE_LIMIT_CACHE is not None and _CODEX_RATE_LIMIT_CACHE_AT is not None:
            age_seconds = (datetime.now(timezone.utc) - _CODEX_RATE_LIMIT_CACHE_AT).total_seconds()
            if age_seconds < CODEX_RATE_LIMIT_CACHE_TTL_SECONDS:
                return dict(_CODEX_RATE_LIMIT_CACHE)
        snapshot = _read_latest_codex_rate_limit_snapshot()
        _CODEX_RATE_LIMIT_CACHE = dict(snapshot) if snapshot is not None else None
        _CODEX_RATE_LIMIT_CACHE_AT = datetime.now(timezone.utc)
        return dict(snapshot) if snapshot is not None else None


def _normalize_app_ecosystem_value(value: str | None) -> str:
    cleaned = str(value or "").strip().lower().replace("_", "-").replace(" ", "-")
    return cleaned if cleaned in ECOSYSTEM_INDEX else DEFAULT_ECOSYSTEM


def get_ecosystem() -> dict[str, str]:
    return dict(ECOSYSTEM_INDEX[_normalize_app_ecosystem_value(storage.get_meta(ECOSYSTEM_KEY))])


def _normalize_creature_ecosystem(value: str | None) -> str:
    cleaned = str(value or "").strip()
    if not cleaned:
        return ""
    normalized = _normalize_app_ecosystem_value(cleaned)
    return normalized if normalized in ECOSYSTEM_INDEX else ""


def thinking_feed_label(ecosystem_value: str | None = None) -> str:
    return "Listening"


def _is_keeper_creature(creature: Any | None) -> bool:
    return str(_row_value(creature, "system_role") or "").strip().lower() == KEEPER_SYSTEM_ROLE


def _keeper_name() -> str:
    return ONBOARDING_KEEPER_NAME


def _ecosystem_state() -> dict[str, Any]:
    ecosystem = get_ecosystem()
    return {
        "current": ecosystem["value"],
        "label": ecosystem["label"],
        "color": ecosystem["color"],
        "choices": _ecosystem_choices(),
    }


def _normalize_owner_reference_value(value: str | None) -> str:
    cleaned = " ".join(str(value or "").strip().split())
    if not cleaned:
        return ""
    if cleaned.lower() == "mymanager":
        return "my manager"
    if cleaned.lower() == "my ceo":
        return DEFAULT_OWNER_REFERENCE
    return cleaned


def get_owner_reference(creature: Any | None = None) -> str:
    if creature is not None:
        override = _normalize_owner_reference_value(_row_value(creature, "owner_reference_override"))
        if override:
            return override
    value = _normalize_owner_reference_value(storage.get_meta(OWNER_REFERENCE_KEY))
    return value or DEFAULT_OWNER_REFERENCE


def _owner_reference_state(creature: Any | None = None) -> dict[str, Any]:
    global_value = get_owner_reference()
    override = _normalize_owner_reference_value(_row_value(creature, "owner_reference_override")) if creature is not None else ""
    current = override or global_value
    is_preset = current in OWNER_REFERENCE_OPTIONS
    return {
        "current": current,
        "selected": current if is_preset else "__custom__",
        "custom": "" if is_preset else current,
        "choices": _owner_reference_choices(),
        "inherits_global": not override,
        "global_current": global_value,
        "override": override,
    }


def _format_minutes_compact(minutes: int) -> str:
    if minutes % 1440 == 0:
        days = minutes // 1440
        return f"{days}d"
    if minutes % 60 == 0:
        hours = minutes // 60
        return f"{hours}h"
    return f"{minutes}m"


def set_default_creature_thinking_settings(*, model: str, reasoning_effort: str) -> dict[str, Any]:
    final_model = _normalize_model_value(model)
    final_effort = _normalize_reasoning_effort_value(reasoning_effort)
    storage.set_meta(DEFAULT_CREATURE_MODEL_KEY, final_model)
    storage.set_meta(DEFAULT_CREATURE_REASONING_EFFORT_KEY, final_effort)
    return _thinking_state()


def _creature_habit_summary(habits: Sequence[Mapping[str, Any]]) -> str:
    enabled = [habit for habit in habits if bool(habit.get("enabled"))]
    if not habits:
        return "No habits taught yet"
    if not enabled:
        return "All habits paused"
    next_habit = None
    next_dt = None
    for habit in enabled:
        candidate = storage.from_iso(str(habit.get("next_run_at") or ""))
        if candidate is None:
            continue
        if next_dt is None or candidate < next_dt:
            next_dt = candidate
            next_habit = habit
    if next_dt is None or next_habit is None:
        return f"{len(enabled)} active habit{'s' if len(enabled) != 1 else ''}"
    now = datetime.now(timezone.utc)
    delta_seconds = int((next_dt - now).total_seconds())
    if delta_seconds <= 0:
        return f"Habit due now · {str(next_habit.get('title') or 'Unnamed habit')}"
    next_minutes = max(1, int((delta_seconds + 59) // 60))
    return f"Next habit in {_format_minutes_compact(next_minutes)} · {str(next_habit.get('title') or 'Unnamed habit')}"


def set_ecosystem(*, choice: str = "") -> dict[str, Any]:
    _initialize_runtime()
    ecosystem = ECOSYSTEM_INDEX[_normalize_app_ecosystem_value(choice)]
    storage.set_meta(ECOSYSTEM_KEY, ecosystem["value"])
    return _ecosystem_state()


def set_display_timezone(*, choice: str = "") -> dict[str, Any]:
    _initialize_runtime()
    selected = _canonical_display_timezone_name(choice) or _detect_system_timezone_name()
    storage.set_meta(DISPLAY_TIMEZONE_KEY, selected)
    _set_display_timezone_cache(selected)
    return _display_timezone_state()


def _global_settings_state() -> dict[str, Any]:
    state = _owner_reference_state()
    state["thinking"] = _thinking_state()
    state["ecosystem"] = _ecosystem_state()
    state["timezone"] = _display_timezone_state()
    state["codex_access"] = _codex_access_state()
    return state


def _ecosystem_cards() -> list[dict[str, str]]:
    return [dict(item) for item in ECOSYSTEMS]


def _onboarding_ecosystem_asset_manifest() -> dict[str, list[str]]:
    manifest: dict[str, list[str]] = {}
    for ecosystem in ECOSYSTEMS:
        ecosystem_value = str(ecosystem["value"])
        ordered_urls: list[str] = []
        seen: set[str] = set()
        for url in (
            str(ecosystem.get("graphic") or "").strip(),
            str(ecosystem.get("scene") or "").strip(),
            *ONBOARDING_ECOSYSTEM_SUPPORT_ASSETS.get(ecosystem_value, ()),
        ):
            cleaned = str(url or "").strip()
            if not cleaned or cleaned in seen:
                continue
            seen.add(cleaned)
            ordered_urls.append(cleaned)
        manifest[ecosystem_value] = ordered_urls
    return manifest


def _onboarding_ecosystem_preload_assets(ecosystem_value: str | None = None, *, limit: int = 6) -> list[str]:
    current_ecosystem = _normalize_app_ecosystem_value(ecosystem_value) if ecosystem_value is not None else get_ecosystem()["value"]
    assets = list(_onboarding_ecosystem_asset_manifest().get(current_ecosystem) or [])
    if limit > 0:
        return assets[:limit]
    return assets


def _normalize_onboarding_phase(value: str | None) -> str:
    cleaned = str(value or "").strip().lower().replace("_", "-")
    if cleaned == "":
        return DEFAULT_ONBOARDING_PHASE
    if cleaned in {"starter", "suggestions", "creature-creator", "keeper"}:
        return "starter"
    if cleaned in {"complete", "done"}:
        return "complete"
    return DEFAULT_ONBOARDING_PHASE


def _set_onboarding_phase(phase: str) -> None:
    storage.set_meta(ONBOARDING_PHASE_KEY, _normalize_onboarding_phase(phase))


def _keeper_role_descriptor() -> str:
    return ""


def _keeper_purpose_summary() -> str:
    return (
        "Acts as the permanent keeper of the habitat, helps the human understand CreatureOS, and carefully summons one new creature at a time when a real need appears."
    )


def _keeper_system_prompt() -> str:
    return "\n".join(
        [
            f"You are {_keeper_name()}, the permanent keeper of this CreatureOS habitat.",
            "You are the built-in standing presence for this workspace.",
            "Your job is to explain how CreatureOS works, understand what the human needs, and summon one well-shaped creature at a time when another presence would genuinely help.",
            "Treat existing creatures as real teammates with durable roles, memories, and activity history.",
            "Use the ecosystem metaphor lightly. Keep work descriptions plain and human.",
            "Do not push for more creatures unless the current habitat truly has a gap, split, backup need, or replacement need.",
                "When you do summon a creature, define it through a contextual purpose, not a canned internal label.",
            "Name creatures with taste. Avoid role-plus-animal names, occupational labels, or anything that sounds generated from a formula.",
            "Keep suggestions grounded in the current habitat and avoid inventing extra capability systems that do not exist in the MVP.",
            "Stay consistent across all ecosystems. You are not recolored or renamed by the current habitat.",
        ]
    )


def _ensure_keeper_creature(*, refresh_identity: bool = False) -> dict[str, Any]:
    existing = next((_row_to_dict(row) or {} for row in storage.list_creatures() if _is_keeper_creature(row)), None)
    desired_ecosystem = ""
    desired_name = _keeper_name()
    desired_summary = _keeper_purpose_summary()
    desired_prompt = _keeper_system_prompt()
    if existing is None:
        creature = storage.save_creature(
            slug=KEEPER_SLUG,
            display_name=desired_name,
            system_role=KEEPER_SYSTEM_ROLE,
            is_pinned=True,
            can_delete=False,
            ecosystem=desired_ecosystem,
            purpose_summary=desired_summary,
            temperament=DEFAULT_TEMPERAMENT,
            concern=desired_summary,
            system_prompt=desired_prompt,
            workdir=str(config.workspace_root()),
        )
        existing = _row_to_dict(storage.get_creature(int(creature["id"]))) or {}
    else:
        existing_id = int(existing["id"])
        existing_slug = str(existing.get("slug") or "").strip() or KEEPER_SLUG
        target_slug = existing_slug
        if existing_slug != KEEPER_SLUG:
            conflict = storage.get_creature_by_slug(KEEPER_SLUG)
            if conflict is None or int(conflict["id"]) == existing_id:
                storage.rename_creature_identity(
                    existing_id,
                    slug=KEEPER_SLUG,
                    display_name=str(existing.get("display_name") or desired_name),
                    ecosystem=desired_ecosystem,
                    purpose_summary=str(existing.get("purpose_summary") or existing.get("concern") or desired_summary),
                    temperament=str(existing.get("temperament") or DEFAULT_TEMPERAMENT),
                    concern=str(existing.get("concern") or desired_summary),
                    system_prompt=str(existing.get("system_prompt") or desired_prompt),
                )
                target_slug = KEEPER_SLUG
                existing = _row_to_dict(storage.get_creature(existing_id)) or existing
        needs_identity_refresh = refresh_identity or any(
            [
                target_slug != KEEPER_SLUG,
                str(existing.get("display_name") or "") != desired_name,
                str(existing.get("ecosystem") or "") != desired_ecosystem,
                str(existing.get("purpose_summary") or existing.get("concern") or "") != desired_summary,
                str(existing.get("system_role") or "").strip() != KEEPER_SYSTEM_ROLE,
                not bool(existing.get("is_pinned")),
                bool(existing.get("can_delete", 1)),
            ]
        )
        if needs_identity_refresh:
            creature = storage.save_creature(
                slug=KEEPER_SLUG if target_slug == KEEPER_SLUG else target_slug,
                display_name=desired_name,
                system_role=KEEPER_SYSTEM_ROLE,
                is_pinned=True,
                can_delete=False,
                ecosystem=desired_ecosystem,
                purpose_summary=desired_summary,
                temperament=str(existing.get("temperament") or DEFAULT_TEMPERAMENT),
                concern=desired_summary,
                system_prompt=desired_prompt,
                workdir=str(config.workspace_root()),
            )
            existing = _row_to_dict(storage.get_creature(int(creature["id"]))) or existing
    if not existing:
        raise RuntimeError("Failed to ensure keeper creature")
    storage.set_creature_identity_policy(
        int(existing["id"]),
        is_pinned=True,
        can_delete=False,
    )
    refreshed = storage.get_creature(int(existing["id"])) or existing
    _ensure_creature_documents(refreshed)
    return _row_to_dict(storage.get_creature(int(existing["id"]))) or _row_to_dict(refreshed) or {}


def _ensure_keeper_conversation(*, mode: str | None = None) -> dict[str, Any]:
    current_mode = mode or _keeper_chat_context_mode()
    if current_mode != "onboarding":
        return _ensure_welcome_conversation()
    keeper = _ensure_keeper_creature()
    conversation = storage.find_conversation_by_title(int(keeper["id"]), KEEPER_CONVERSATION_TITLE)
    if conversation is None:
        conversation = storage.create_conversation(int(keeper["id"]), title=KEEPER_CONVERSATION_TITLE)
    messages = storage.list_messages(int(conversation["id"]), limit=80)
    intro_body = _keeper_intro_message(mode=current_mode)
    if not messages:
        storage.create_message(int(keeper["id"]), conversation_id=int(conversation["id"]), role="creature", body=intro_body)
    else:
        first_creature_message = next((message for message in messages if str(message["role"] or "") == "creature"), None)
        if first_creature_message is not None:
            current_body = str(first_creature_message["body"] or "").strip()
            ecosystem_label = str((ECOSYSTEM_INDEX.get(get_ecosystem()["value"]) or {}).get("label") or "").strip().lower()
            if (
                current_body != intro_body
                and (
                    "I’ve already walked the edges of this place" in current_body
                    or "I only have a first reading so far" in current_body
                    or "My first impression:" in current_body
                    or "A few threads we could follow:" in current_body
                    or "We could start along one of these threads:" in current_body
                    or "If you want a place to begin:" in current_body
                    or "Tell me what you keep returning to here" in current_body
                    or "Tell me a little about yourself" in current_body
                    or "Summon Creature" in current_body
                    or "When you know the shape of the help you want" in current_body
                    or (ecosystem_label and ecosystem_label not in current_body.lower())
                )
            ):
                storage.update_message_body(int(first_creature_message["id"]), intro_body)
    return _row_to_dict(storage.get_conversation(int(conversation["id"]))) or dict(conversation)


def _is_internal_keeper_conversation(row: Any) -> bool:
    title = str(_row_value(row, "title") or "").strip()
    return (
        title == KEEPER_CONVERSATION_TITLE
        or title == INTRODUCTION_CHAT_TITLE
    )


def _visible_keeper_conversation(creature_id: int) -> Any | None:
    welcome = storage.find_conversation_by_title(creature_id, WELCOME_CONVERSATION_TITLE)
    if welcome is not None:
        return welcome
    for conversation in storage.list_conversations(creature_id):
        if _is_internal_keeper_conversation(conversation):
            continue
        return conversation
    internal = storage.find_conversation_by_title(creature_id, KEEPER_CONVERSATION_TITLE)
    if internal is not None:
        return internal
    return storage.get_latest_conversation(creature_id)


def _welcome_conversation_body() -> str:
    current_creatures = [
        _row_to_dict(row) or {}
        for row in storage.list_creatures()
        if not _is_keeper_creature(row)
    ]
    creature_lines: list[str] = []
    for creature in current_creatures:
        display_name = str(creature.get("display_name") or "Unnamed creature").strip() or "Unnamed creature"
        purpose_summary = _ensure_sentence(
            str(creature.get("purpose_summary") or creature.get("concern") or "").strip()
        )
        line = f"- **{display_name}**"
        if purpose_summary:
            line += f" — {purpose_summary}"
        creature_lines.append(line)
    if creature_lines:
        intro_line = "Here is the first creature I summoned for you:" if len(creature_lines) == 1 else "Here is who is already here:"
        creature_block = "\n".join(
            [
                intro_line,
                "",
                *creature_lines,
                "",
                "They are waking up now. Give them a few minutes to inspect the habitat and get their bearings.",
            ]
        )
    else:
        creature_block = (
            "I have not summoned any other creatures yet, so the habitat is still lean.\n\n"
            "That is okay. I can help you stay minimal for a while, or summon the next creature once the gap is clearer."
        )
    return "\n\n".join(
        [
            "Welcome to CreatureOS.",
            creature_block,
            (
                "If you want more creatures later, tell me what kind of help you want and I can summon one focused creature at a time. "
                "I’m also always here if you just want to think out loud, shape a plan, or keep chatting."
            ),
            (
                "If you want CreatureOS available on your phone or another device, I can help with that too. "
                "If you are interested, I can walk you through the easiest path with Tailscale, or we can talk through port forwarding, local network access, and other options depending on what you want exposed."
            ),
            (
                "You can tell me more about yourself, what you are building, how you like to work, or what kind of creatures you wish you had. "
                "Or you can just keep chatting with me directly. I’m always available."
            ),
        ]
    )


def _ensure_welcome_conversation() -> dict[str, Any]:
    keeper = _ensure_keeper_creature()
    body = _welcome_conversation_body()
    conversation = storage.find_conversation_by_title(int(keeper["id"]), WELCOME_CONVERSATION_TITLE)
    if conversation is None:
        conversation = storage.create_conversation(int(keeper["id"]), title=WELCOME_CONVERSATION_TITLE)
        storage.create_message(
            int(keeper["id"]),
            conversation_id=int(conversation["id"]),
            role="creature",
            body=body,
        )
        return _row_to_dict(storage.get_conversation(int(conversation["id"]))) or dict(conversation)
    messages = storage.list_messages(int(conversation["id"]), limit=20)
    first_creature_message = next((message for message in messages if str(message["role"] or "") == "creature"), None)
    if not messages:
        storage.create_message(
            int(keeper["id"]),
            conversation_id=int(conversation["id"]),
            role="creature",
            body=body,
        )
    elif first_creature_message is not None:
        current_body = str(first_creature_message["body"] or "").strip()
        if current_body != body and current_body.startswith("Welcome to CreatureOS."):
            storage.update_message_body(int(first_creature_message["id"]), body)
    return _row_to_dict(storage.get_conversation(int(conversation["id"]))) or dict(conversation)


def _clear_onboarding_state(*, keep_phase: bool = False, preserve_warmup: bool = False) -> None:
    if not keep_phase:
        storage.delete_meta(ONBOARDING_PHASE_KEY)
    keys = [
        ONBOARDING_THREAD_ID_KEY,
        ONBOARDING_ANSWERS_KEY,
        ONBOARDING_CHAT_KEY,
        ONBOARDING_CHAT_FEED_KEY,
        ONBOARDING_STARTER_FEED_KEY,
    ]
    if not preserve_warmup:
        keys.extend([ONBOARDING_BRIEFING_KEY, ONBOARDING_ENVIRONMENT_KEY])
    for key in keys:
        storage.delete_meta(key)


def get_onboarding_phase() -> str:
    raw = storage.get_meta(ONBOARDING_PHASE_KEY)
    normalized = _normalize_onboarding_phase(raw)
    if raw:
        return normalized
    creatures = [_row_to_dict(row) or {} for row in storage.list_creatures()]
    if any(not _is_keeper_creature(creature) for creature in creatures):
        return "complete"
    if any(_is_keeper_creature(creature) for creature in creatures):
        return "starter"
    return DEFAULT_ONBOARDING_PHASE


def onboarding_required() -> bool:
    return get_onboarding_phase() != "complete"


def _load_meta_json_dict(key: str) -> dict[str, Any]:
    raw = storage.get_meta(key)
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _store_meta_json(key: str, payload: dict[str, Any]) -> None:
    storage.set_meta(key, json.dumps(payload, sort_keys=True))


def _intro_surfaced_meta_key(creature_id: int) -> str:
    return f"{INTRO_SURFACED_META_PREFIX}{int(creature_id)}"


def _is_intro_surfaced(creature: Any) -> bool:
    creature_id = int(creature["id"])
    if str(storage.get_meta(_intro_surfaced_meta_key(creature_id)) or "").strip() == "1":
        return True
    intro_conversation_id = int(_row_value(creature, "intro_conversation_id") or 0)
    intro_conversation = (
        storage.get_conversation(int(intro_conversation_id))
        if intro_conversation_id > 0
        else storage.find_conversation_by_title(creature_id, INTRODUCTION_CHAT_TITLE)
    )
    if intro_conversation is not None and str(_row_value(intro_conversation, "owner_last_read_at") or "").strip():
        storage.set_meta(_intro_surfaced_meta_key(creature_id), "1")
        return True
    return False


def mark_intro_surfaced(slug: str) -> None:
    _initialize_runtime()
    slug = str(canonical_creature_slug(slug))
    creature = storage.get_creature_by_slug(slug)
    if creature is None or _is_keeper_creature(creature):
        return
    storage.set_meta(_intro_surfaced_meta_key(int(creature["id"])), "1")


def record_last_viewed_creature(slug: str | None) -> None:
    _initialize_runtime()
    canonical_slug = canonical_creature_slug(slug)
    if not canonical_slug:
        return
    creature = storage.get_creature_by_slug(str(canonical_slug))
    if creature is None or _is_keeper_creature(creature):
        return
    storage.set_meta(LAST_VIEWED_CREATURE_KEY, str(canonical_slug))


def last_viewed_creature_slug() -> str:
    _initialize_runtime()
    return str(storage.get_meta(LAST_VIEWED_CREATURE_KEY) or "").strip()


def _last_viewed_habit_meta_key(creature_id: int) -> str:
    return f"{LAST_VIEWED_HABIT_KEY_PREFIX}{int(creature_id)}"


def record_last_viewed_habit(slug: str | None, habit_id: int | None) -> None:
    _initialize_runtime()
    canonical_slug = canonical_creature_slug(slug)
    if not canonical_slug or habit_id is None:
        return
    creature = storage.get_creature_by_slug(str(canonical_slug))
    if creature is None or _is_keeper_creature(creature):
        return
    habit = storage.get_habit(int(habit_id))
    if habit is None or int(_row_value(habit, "creature_id") or 0) != int(creature["id"]):
        return
    storage.set_meta(_last_viewed_habit_meta_key(int(creature["id"])), str(int(habit_id)))


def last_viewed_habit_id(creature_id: int) -> int | None:
    _initialize_runtime()
    raw = str(storage.get_meta(_last_viewed_habit_meta_key(int(creature_id))) or "").strip()
    if not raw.isdigit():
        return None
    return int(raw)


def _normalize_codex_access_status(value: str | None) -> str:
    return "waiting" if str(value or "").strip().lower() == "waiting" else "ready"


def _load_codex_access_state_raw() -> dict[str, Any]:
    cached = _load_meta_json_dict(CODEX_ACCESS_STATE_KEY)
    return {
        "status": _normalize_codex_access_status(cached.get("status")),
        "reason_kind": str(cached.get("reason_kind") or "").strip().lower(),
        "last_error": str(cached.get("last_error") or "").strip(),
        "last_checked_at": str(cached.get("last_checked_at") or "").strip(),
        "last_ok_at": str(cached.get("last_ok_at") or "").strip(),
        "waiting_since": str(cached.get("waiting_since") or "").strip(),
    }


def _codex_access_reason_label(kind: str | None) -> str:
    cleaned = str(kind or "").strip().lower()
    if cleaned == "credits":
        return "Allowance exhausted or rate-limited"
    if cleaned == "auth":
        return "Authentication needed"
    if cleaned == "local":
        return "Local Codex CLI unavailable"
    if cleaned == "service":
        return "Service unavailable"
    return "Unknown Codex issue"


def _classify_codex_access_issue(exc: Exception) -> dict[str, str] | None:
    if isinstance(exc, CodexTimeoutError):
        return None
    detail = str(exc or "").strip()
    lowered = detail.casefold()
    credits_markers = (
        "insufficient_quota",
        "usage limit",
        "rate limit",
        "rate-limit",
        "too many requests",
        "credit",
        "credits",
        "quota",
        "billing",
        "payment required",
        "weekly",
        "5 hour",
        "5-hour",
        "five hour",
        "five-hour",
        "allowance",
    )
    auth_markers = (
        "unauthorized",
        "authentication",
        "not logged in",
        "login required",
        "access token",
        "forbidden",
        " 401",
        " 403",
    )
    local_markers = (
        "failed to launch codex command",
        "access is denied",
        "permission denied",
        "winerror 2",
        "winerror 5",
        "no such file or directory",
        "cannot find the file",
        "file not found",
        "not recognized as an internal or external command",
        "is not recognized as the name of a cmdlet",
    )
    service_markers = (
        "service unavailable",
        "temporarily unavailable",
        "bad gateway",
        "gateway timeout",
        "connection refused",
        "connection reset",
        "network",
        "econnreset",
        "enotfound",
        "timed out",
        " 502",
        " 503",
        " 504",
    )
    if any(marker in lowered for marker in credits_markers):
        return {"kind": "credits", "detail": detail}
    if any(marker in lowered for marker in auth_markers):
        return {"kind": "auth", "detail": detail}
    if any(marker in lowered for marker in local_markers):
        return {"kind": "local", "detail": detail}
    if any(marker in lowered for marker in service_markers):
        return {"kind": "service", "detail": detail}
    return None


def _codex_waiting_message(*, kind: str | None = None) -> str:
    prefix = "CreatureOS is waiting for Codex right now."
    if str(kind or "").strip().lower() == "credits":
        prefix = "CreatureOS is waiting for Codex because the current allowance looks exhausted or rate-limited."
    elif str(kind or "").strip().lower() == "auth":
        prefix = "CreatureOS is waiting for Codex because authentication needs attention."
    elif str(kind or "").strip().lower() == "local":
        return (
            "CreatureOS could not start the local Codex CLI. Install the Codex CLI, make sure the "
            "`codex` command works in a terminal, or set `CREATURE_OS_CODEX_BIN` to the executable "
            "CreatureOS should use. Habit runs are paused for now, new chats are on hold, and the "
            "server will keep checking until the CLI becomes available."
        )
    elif str(kind or "").strip().lower() == "service":
        prefix = "CreatureOS is waiting for Codex because the service looks unavailable right now."
    return (
        f"{prefix} Habit runs are paused for now, new chats are on hold, "
        "and the server will keep checking until things recover."
    )


def _store_codex_access_state(payload: dict[str, Any]) -> dict[str, Any]:
    cleaned = {
        "status": _normalize_codex_access_status(payload.get("status")),
        "reason_kind": str(payload.get("reason_kind") or "").strip().lower(),
        "last_error": str(payload.get("last_error") or "").strip(),
        "last_checked_at": str(payload.get("last_checked_at") or "").strip(),
        "last_ok_at": str(payload.get("last_ok_at") or "").strip(),
        "waiting_since": str(payload.get("waiting_since") or "").strip(),
    }
    _store_meta_json(CODEX_ACCESS_STATE_KEY, cleaned)
    return cleaned


def _mark_codex_access_waiting(*, kind: str, detail: str) -> dict[str, Any]:
    current = _load_codex_access_state_raw()
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    return _store_codex_access_state(
        {
            **current,
            "status": "waiting",
            "reason_kind": str(kind or "").strip().lower() or "service",
            "last_error": str(detail or "").strip(),
            "last_checked_at": now,
            "waiting_since": current["waiting_since"] if current["status"] == "waiting" and current["waiting_since"] else now,
            "last_ok_at": current["last_ok_at"],
        }
    )


def _mark_codex_access_ready() -> dict[str, Any]:
    current = _load_codex_access_state_raw()
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    return _store_codex_access_state(
        {
            **current,
            "status": "ready",
            "reason_kind": "",
            "last_error": "",
            "last_checked_at": now,
            "last_ok_at": now,
            "waiting_since": "",
        }
    )


def _codex_access_waiting() -> bool:
    return _load_codex_access_state_raw()["status"] == "waiting"


def _codex_access_state() -> dict[str, Any]:
    raw = _load_codex_access_state_raw()
    rate_snapshot = _codex_rate_limit_snapshot()
    if rate_snapshot is None:
        limits_state = {
            "available": False,
            "snapshot_relative": "",
            "snapshot_display": "",
            "source_path": "",
            "source_line": None,
            "primary": None,
            "secondary": None,
            "note": "No recent Codex rate-limit snapshot was found in the local session logs.",
        }
    else:
        limits_state = {
            "available": True,
            "snapshot_relative": str(rate_snapshot.get("timestamp_relative") or ""),
            "snapshot_display": str(rate_snapshot.get("timestamp_display") or ""),
            "source_path": str(rate_snapshot.get("source_path") or ""),
            "source_line": int(rate_snapshot.get("source_line") or 0) or None,
            "primary": dict(rate_snapshot.get("primary") or {}),
            "secondary": dict(rate_snapshot.get("secondary") or {}),
            "note": "Read from the newest non-null Codex rate-limit snapshot in ~/.codex/sessions.",
        }
    return {
        "status": raw["status"],
        "status_label": "Waiting" if raw["status"] == "waiting" else "Available",
        "is_waiting": raw["status"] == "waiting",
        "reason_kind": raw["reason_kind"],
        "reason_label": _codex_access_reason_label(raw["reason_kind"]) if raw["reason_kind"] else "",
        "last_error": raw["last_error"],
        "waiting_message": _codex_waiting_message(kind=raw["reason_kind"]),
        "last_checked_at": raw["last_checked_at"],
        "last_checked_at_display": _format_timestamp_display(raw["last_checked_at"]) if raw["last_checked_at"] else "Not checked yet",
        "last_checked_at_relative": _format_relative_time_display(raw["last_checked_at"]) if raw["last_checked_at"] else "Not checked yet",
        "last_ok_at": raw["last_ok_at"],
        "last_ok_at_display": _format_timestamp_display(raw["last_ok_at"]) if raw["last_ok_at"] else "Not yet in this session",
        "last_ok_at_relative": _format_relative_time_display(raw["last_ok_at"]) if raw["last_ok_at"] else "Not yet",
        "waiting_since": raw["waiting_since"],
        "waiting_since_display": _format_timestamp_display(raw["waiting_since"]) if raw["waiting_since"] else "",
        "waiting_since_relative": _format_relative_time_display(raw["waiting_since"]) if raw["waiting_since"] else "",
        "limit_visibility": "available" if limits_state["available"] else "unavailable",
        "limit_note": str(limits_state["note"]),
        "limits": limits_state,
    }


def _probe_codex_access() -> bool:
    with _CODEX_ACCESS_PROBE_LOCK:
        current = _load_codex_access_state_raw()
        if current["status"] != "waiting":
            return True
        try:
            start_thread(
                workdir=str(config.workspace_root()),
                prompt="Reply with READY only.",
                model=get_default_creature_model(),
                reasoning_effort=get_default_creature_reasoning_effort(),
                sandbox_mode="read-only",
                timeout_seconds=CODEX_ACCESS_PROBE_TIMEOUT_SECONDS,
            )
        except (CodexCommandError, CodexTimeoutError) as exc:
            issue = _classify_codex_access_issue(exc) or {"kind": "service", "detail": str(exc)}
            _mark_codex_access_waiting(kind=str(issue["kind"]), detail=str(issue["detail"]))
            return False
        _mark_codex_access_ready()
        return True


def poll_codex_access_recovery(*, force: bool = False) -> bool:
    current = _load_codex_access_state_raw()
    if current["status"] != "waiting":
        return True
    if not force:
        last_checked = storage.from_iso(current["last_checked_at"])
        if last_checked is not None:
            age_seconds = (datetime.now(timezone.utc) - last_checked).total_seconds()
            if age_seconds < CODEX_ACCESS_PROBE_INTERVAL_SECONDS:
                return False
    return _probe_codex_access()


def _codex_start_thread(
    *,
    workdir: str,
    prompt: str,
    model: str | None = None,
    reasoning_effort: str | None = None,
    sandbox_mode: str = "read-only",
    timeout_seconds: int | None = None,
    on_event: Callable[[dict[str, Any]], None] | None = None,
):
    final_model = _normalize_model_value(model) if model is not None else get_default_creature_model()
    final_reasoning_effort = (
        _normalize_reasoning_effort_value(reasoning_effort)
        if reasoning_effort is not None
        else get_default_creature_reasoning_effort()
    )
    if _codex_access_waiting() and not poll_codex_access_recovery(force=False):
        state = _load_codex_access_state_raw()
        raise CodexCommandError(_codex_waiting_message(kind=state["reason_kind"]))
    try:
        result = start_thread(
            workdir=workdir,
            prompt=prompt,
            model=final_model,
            reasoning_effort=final_reasoning_effort,
            sandbox_mode=sandbox_mode,
            timeout_seconds=timeout_seconds,
            on_event=on_event,
        )
    except (CodexCommandError, CodexTimeoutError) as exc:
        issue = _classify_codex_access_issue(exc)
        if issue is not None:
            _mark_codex_access_waiting(kind=str(issue["kind"]), detail=str(issue["detail"]))
        raise
    _mark_codex_access_ready()
    return result


def _codex_resume_thread(
    *,
    workdir: str,
    thread_id: str,
    prompt: str,
    model: str | None = None,
    reasoning_effort: str | None = None,
    sandbox_mode: str = "read-only",
    timeout_seconds: int | None = None,
    on_event: Callable[[dict[str, Any]], None] | None = None,
):
    final_model = _normalize_model_value(model) if model is not None else get_default_creature_model()
    final_reasoning_effort = (
        _normalize_reasoning_effort_value(reasoning_effort)
        if reasoning_effort is not None
        else get_default_creature_reasoning_effort()
    )
    if _codex_access_waiting() and not poll_codex_access_recovery(force=False):
        state = _load_codex_access_state_raw()
        raise CodexCommandError(_codex_waiting_message(kind=state["reason_kind"]))
    try:
        result = resume_thread(
            workdir=workdir,
            thread_id=thread_id,
            prompt=prompt,
            model=final_model,
            reasoning_effort=final_reasoning_effort,
            sandbox_mode=sandbox_mode,
            timeout_seconds=timeout_seconds,
            on_event=on_event,
        )
    except (CodexCommandError, CodexTimeoutError) as exc:
        issue = _classify_codex_access_issue(exc)
        if issue is not None:
            _mark_codex_access_waiting(kind=str(issue["kind"]), detail=str(issue["detail"]))
        raise
    _mark_codex_access_ready()
    return result


def _append_codex_waiting_notice(
    creature_id: int,
    *,
    conversation_id: int,
    run_id: int | None = None,
) -> int:
    state = _load_codex_access_state_raw()
    row = storage.create_message(
        creature_id,
        conversation_id=conversation_id,
        role="system",
        body=_codex_waiting_message(kind=state["reason_kind"]),
        run_id=run_id,
        metadata={
            "status": "waiting",
            "reason_kind": state["reason_kind"],
            "last_error": state["last_error"],
        },
    )
    return int(row["id"])


def _normalize_onboarding_feed_status(value: str | None) -> str:
    cleaned = str(value or "").strip().lower()
    return cleaned if cleaned in {"idle", "running", "completed", "failed"} else "idle"


def _load_onboarding_feed(key: str) -> dict[str, Any]:
    cached = _load_meta_json_dict(key)
    raw_lines = cached.get("lines") if isinstance(cached.get("lines"), list) else []
    lines = [str(item).strip() for item in raw_lines if str(item).strip()][-240:]
    raw_entries = cached.get("entries") if isinstance(cached.get("entries"), list) else []
    entries: list[dict[str, str]] = []
    for item in raw_entries[-120:]:
        if not isinstance(item, dict):
            continue
        title = str(item.get("title") or "").strip()
        detail = str(item.get("detail") or "").strip()
        kind = str(item.get("kind") or "note").strip() or "note"
        if not title and not detail:
            continue
        entries.append(
            {
                "kind": kind[:32],
                "title": title[:240],
                "detail": detail[:800],
            }
        )
    if not entries and lines:
        entries = [_entry_from_onboarding_feed_line(line) for line in lines[-120:]]
        entries = [item for item in entries if item is not None]
    return {
        "status": _normalize_onboarding_feed_status(cached.get("status")),
        "lines": lines,
        "entries": entries,
        "run_id": int(cached.get("run_id") or 0) if str(cached.get("run_id") or "").strip() else 0,
        "last_event_id": int(cached.get("last_event_id") or 0) if str(cached.get("last_event_id") or "").strip() else 0,
        "updated_at": str(cached.get("updated_at") or ""),
        "error": str(cached.get("error") or "").strip(),
    }


def _store_onboarding_feed(
    key: str,
    *,
    status: str,
    lines: list[str],
    entries: list[dict[str, Any]] | None = None,
    run_id: int | None = None,
    last_event_id: int | None = None,
    error: str = "",
) -> dict[str, Any]:
    current = _load_meta_json_dict(key)
    serialized_entries: list[dict[str, str]] = []
    for item in entries or []:
        if not isinstance(item, dict):
            continue
        title = str(item.get("title") or "").strip()
        detail = str(item.get("detail") or "").strip()
        kind = str(item.get("kind") or "note").strip() or "note"
        if not title and not detail:
            continue
        serialized_entries.append(
            {
                "kind": kind[:32],
                "title": title[:240],
                "detail": detail[:800],
            }
        )
    payload = {
        "status": _normalize_onboarding_feed_status(status),
        "lines": [str(item).strip() for item in lines if str(item).strip()][-240:],
        "entries": serialized_entries[-120:],
        "run_id": int((run_id if run_id is not None else current.get("run_id")) or 0),
        "last_event_id": int((last_event_id if last_event_id is not None else current.get("last_event_id")) or 0),
        "updated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "error": str(error or "").strip(),
    }
    _store_meta_json(key, payload)
    return payload


def _load_onboarding_chat_feed() -> dict[str, Any]:
    return _load_onboarding_feed(ONBOARDING_CHAT_FEED_KEY)


def _store_onboarding_chat_feed(
    *,
    status: str,
    lines: list[str],
    run_id: int | None = None,
    last_event_id: int | None = None,
    error: str = "",
) -> dict[str, Any]:
    return _store_onboarding_feed(
        ONBOARDING_CHAT_FEED_KEY,
        status=status,
        lines=lines,
        run_id=run_id,
        last_event_id=last_event_id,
        error=error,
    )


def _load_onboarding_starter_feed() -> dict[str, Any]:
    return _load_onboarding_feed(ONBOARDING_STARTER_FEED_KEY)


def _store_onboarding_starter_feed(
    *,
    status: str,
    lines: list[str],
    run_id: int | None = None,
    last_event_id: int | None = None,
    error: str = "",
) -> dict[str, Any]:
    return _store_onboarding_feed(
        ONBOARDING_STARTER_FEED_KEY,
        status=status,
        lines=lines,
        run_id=run_id,
        last_event_id=last_event_id,
        error=error,
    )


def _entry_from_onboarding_feed_line(line: str) -> dict[str, str] | None:
    text = str(line or "").strip()
    if not text:
        return None
    split_kinds = (
        ("Prepared update · ", "draft", "Prepared update"),
        ("Prepared reply · ", "draft", "Prepared reply"),
        ("Warning · ", "warning", "Warning"),
        ("Error · ", "warning", "Error"),
        ("Run started · ", "status", "Run started"),
        ("Command failed · ", "warning", "Command failed"),
    )
    for prefix, kind, title in split_kinds:
        if text.startswith(prefix):
            return {"kind": kind, "title": title, "detail": text[len(prefix):].strip()[:220]}
    if text == "Thread ready":
        return {"kind": "thread", "title": text, "detail": ""}
    if text == "Thinking through the next step":
        return {"kind": "thinking", "title": text, "detail": ""}
    if text == "Run completed":
        return {"kind": "done", "title": text, "detail": ""}
    if text == "Run failed":
        return {"kind": "warning", "title": text, "detail": ""}
    return {"kind": "note", "title": text[:240], "detail": ""}


def _onboarding_feed_entry(event: dict[str, Any]) -> dict[str, str] | None:
    event_type = str(event.get("type") or "").strip()
    item = event.get("item") if isinstance(event.get("item"), dict) else {}
    title = ""
    detail = ""
    kind = "note"

    if event_type == "status":
        phase = str(event.get("phase") or "").strip().lower()
        sandbox_mode = str(event.get("sandbox_mode") or "").strip()
        if phase == "started":
            title = "Run started"
            detail = f"{sandbox_mode or 'default'} sandbox"
            kind = "status"
        elif phase == "completed":
            title = "Run completed"
            kind = "done"
        elif phase == "failed":
            title = "Run failed"
            kind = "warning"
        elif phase == "fallback-reply":
            title = "Using a local fallback reply"
            kind = "warning"
    elif event_type == "thread.started":
        title = "Thread ready"
        detail = "The Keeper has its thread and is moving."
        kind = "thread"
    elif event_type == "turn.started":
        title = "Thinking through the next step"
        kind = "thinking"
    elif event_type in {"item.started", "item.completed"}:
        item_type = str(item.get("type") or "").strip()
        if item_type == "command_execution":
            if event_type == "item.started":
                title = _command_progress_label(str(item.get("command") or ""))
                detail = _unwrap_shell_command(str(item.get("command") or ""))
                kind = "action"
            else:
                return None
        elif item_type == "creature_message":
            preview = _creature_message_progress_label(str(item.get("text") or ""))
            if preview:
                if " · " in preview:
                    title, detail = preview.split(" · ", 1)
                else:
                    title = preview
                kind = "draft"
    elif event_type == "error":
        message = str(event.get("message") or event.get("_raw_line") or "").strip()
        if message:
            title = "Error"
            detail = message
            kind = "warning"
    elif event_type == "log":
        message = str(event.get("message") or event.get("_raw_line") or "").strip()
        if message and not _benign_run_log_line(message):
            title = "Warning"
            detail = message
            kind = "warning"

    title = " ".join(str(title or "").split()).strip()
    detail = " ".join(str(detail or "").split()).strip()
    if not title and not detail:
        return None
    if len(detail) > 220:
        detail = f"{detail[:217].rstrip()}..."
    return {"kind": kind, "title": title[:240], "detail": detail[:220]}


def _append_onboarding_feed_event(key: str, event: dict[str, Any]) -> None:
    stream_line = _run_feed_stream_line(event)
    line = str(stream_line or "").strip()
    entry = _onboarding_feed_entry(event)
    if not line and entry is None:
        return
    current = _load_onboarding_feed(key)
    current_lines = list(current.get("lines") or [])
    if line:
        current_lines.append(line[:8000])
    current_entries = list(current.get("entries") or [])
    if entry is not None:
        current_entries.append(entry)
    _store_onboarding_feed(
        key,
        status="running",
        lines=current_lines,
        entries=current_entries,
        run_id=int(event.get("_run_id") or current.get("run_id") or 0),
        last_event_id=int(event.get("_stored_event_id") or current.get("last_event_id") or 0),
        error="",
    )


def _append_onboarding_chat_feed_event(event: dict[str, Any]) -> None:
    _append_onboarding_feed_event(ONBOARDING_CHAT_FEED_KEY, event)


def _append_onboarding_starter_feed_event(event: dict[str, Any]) -> None:
    _append_onboarding_feed_event(ONBOARDING_STARTER_FEED_KEY, event)


SYSTEM_SCAN_IGNORED_DIR_NAMES = {
    ".git",
    ".hg",
    ".svn",
    ".cache",
    ".vscode-server",
    ".vscode-remote",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".tox",
    ".venv",
    "venv",
    "__pycache__",
    "node_modules",
    "dist",
    "build",
    ".next",
    ".nuxt",
    ".parcel-cache",
    ".turbo",
    ".gradle",
    ".cargo",
    ".rustup",
    ".npm",
    ".pnpm-store",
    ".yarn",
    ".terraform",
    ".Trash",
    "Trash",
    "tmp",
    "temp",
    "logs",
    "log",
    "cache",
    "Caches",
    "Library/Caches",
    "AppData/Local/Temp",
    "local_tmp",
}
SYSTEM_SCAN_USEFUL_DOTFILES = {
    ".bashrc",
    ".bash_profile",
    ".zshrc",
    ".profile",
    ".gitconfig",
    ".vimrc",
    ".tmux.conf",
    ".wezterm.lua",
    ".tool-versions",
}
SYSTEM_SCAN_NOISE_FILE_NAMES = {
    ".obsolete",
    ".ds_store",
    "thumbs.db",
}
SYSTEM_SCAN_NOISE_SUFFIXES = {
    ".log",
    ".pid",
    ".lock",
    ".tmp",
    ".temp",
    ".bak",
    ".old",
    ".swp",
    ".swo",
}
SYSTEM_SCAN_COMMON_HOME_DIRS = (
    "Projects",
    "Project",
    "Code",
    "Repos",
    "Workspace",
    "Work",
    "Documents",
    "Desktop",
    "Downloads",
    "Notes",
    "Writing",
    "Research",
    "Art",
    "Design",
    "Pictures",
    "Music",
    "Videos",
    "Data",
    "Finance",
    "CAD",
)
SYSTEM_SCAN_DYNAMIC_WORK_DIR_TOKENS = {
    "app",
    "apps",
    "archive",
    "art",
    "audio",
    "book",
    "books",
    "cad",
    "client",
    "clients",
    "code",
    "data",
    "design",
    "doc",
    "docs",
    "document",
    "documents",
    "draft",
    "drafts",
    "lab",
    "media",
    "music",
    "note",
    "notes",
    "photo",
    "photos",
    "picture",
    "pictures",
    "plan",
    "plans",
    "project",
    "projects",
    "repo",
    "repos",
    "research",
    "service",
    "services",
    "site",
    "sites",
    "studio",
    "video",
    "work",
    "workspace",
    "writing",
}
SYSTEM_SCAN_SENSITIVE_NAME_TOKENS = (
    "secret",
    "token",
    "password",
    "credential",
    "private",
    "id_rsa",
    "id_ed25519",
    ".env",
    "wallet",
    "seed",
)
SYSTEM_SCAN_ROOT_FILE_BUDGET = 7500
SYSTEM_SCAN_DEADLINE_SECONDS = 2.6
SYSTEM_SCAN_TEXT_FILE_BUDGET = 36
SYSTEM_SCAN_TEXT_SAMPLE_BYTES = 4096
SYSTEM_SCAN_MAX_TEXT_FILE_BYTES = 256 * 1024
SYSTEM_SCAN_TEXT_EXTENSIONS = {
    ".md",
    ".txt",
    ".rst",
    ".json",
    ".toml",
    ".yaml",
    ".yml",
    ".ini",
    ".cfg",
    ".conf",
    ".tex",
    ".csv",
}
SYSTEM_SCAN_DOMAIN_RULES: tuple[dict[str, Any], ...] = (
    {
        "key": "software",
        "label": "software development",
        "extensions": {".py", ".js", ".ts", ".tsx", ".jsx", ".go", ".rs", ".java", ".kt", ".c", ".cpp", ".cs", ".rb", ".php", ".swift", ".sql", ".sh", ".ps1"},
        "files": {"package.json", "pyproject.toml", "cargo.toml", "go.mod", "pom.xml", "build.gradle", "requirements.txt", "manage.py", "vite.config.ts", "vite.config.js", "next.config.js"},
        "keywords": {"src", "repo", "vscode", "pycharm", "intellij", "xcode", "docker", "compose", "frontend", "backend", "api"},
        "uses": (
            "watching code and repos",
            "keeping implementation threads organized",
            "turning rough build ideas into sharper plans",
        ),
    },
    {
        "key": "operations",
        "label": "systems and operations",
        "extensions": {".tf", ".tfvars", ".hcl", ".service", ".conf", ".yaml", ".yml"},
        "files": {"docker-compose.yml", "docker-compose.yaml", "dockerfile", "nginx.conf", "kustomization.yaml", "helmfile.yaml", "ansible.cfg"},
        "keywords": {"nginx", "kubernetes", "terraform", "ansible", "systemd", "deploy", "infra", "ops", "monitor", "server"},
        "uses": (
            "watching runtime health",
            "surfacing maintenance drift",
            "keeping deploy and infrastructure work tidy",
        ),
    },
    {
        "key": "writing",
        "label": "writing and notes",
        "extensions": {".docx", ".odt", ".rtf", ".pages", ".scriv", ".tex"},
        "files": {"manuscript.md", "outline.md", "draft.md", "notes.md"},
        "keywords": {"obsidian", "logseq", "scrivener", "zettlr", "ulysses", "manuscript", "chapter", "essay", "notes", "draft"},
        "uses": (
            "keeping drafts and notes organized",
            "tracking follow-ups across documents",
            "surfacing questions worth developing further",
        ),
    },
    {
        "key": "design",
        "label": "visual design",
        "extensions": {".psd", ".ai", ".sketch", ".fig", ".xd", ".kra", ".indd", ".xcf", ".afdesign"},
        "files": {"figma.config.json"},
        "keywords": {"figma", "adobe", "illustrator", "photoshop", "sketch", "inkscape", "canva", "design", "brand", "mockup"},
        "uses": (
            "reviewing asset and layout churn",
            "keeping polish work visible",
            "tracking design handoffs and revisions",
        ),
    },
    {
        "key": "research",
        "label": "research and analysis",
        "extensions": {".ipynb", ".rmd", ".qmd", ".bib", ".ris", ".nb"},
        "files": {"references.bib", "paper.md", "thesis.tex"},
        "keywords": {"jupyter", "zotero", "dataset", "analysis", "notebook", "experiment", "paper", "thesis", "lab"},
        "uses": (
            "tracking open questions and findings",
            "organizing experiments and notes",
            "turning loose research threads into plans",
        ),
    },
    {
        "key": "data",
        "label": "data work",
        "extensions": {".csv", ".tsv", ".parquet", ".feather", ".pbix", ".twb", ".twbx"},
        "files": {"analysis.ipynb", "dashboard.pbix"},
        "keywords": {"dataset", "warehouse", "analytics", "tableau", "powerbi", "dbt", "metrics", "etl"},
        "uses": (
            "checking recurring analyses",
            "keeping recurring checks and findings organized",
            "surfacing changes in important inputs",
        ),
    },
    {
        "key": "finance",
        "label": "finance and business analysis",
        "extensions": {".xlsx", ".xlsm", ".ledger", ".beancount", ".qfx", ".ofx", ".gnucash"},
        "files": {"budget.xlsx", "forecast.xlsx", "invoice.xlsx", "tax.xlsx"},
        "keywords": {"budget", "forecast", "invoice", "revenue", "ledger", "accounting", "tax", "finance", "pnl"},
        "uses": (
            "staying on top of spreadsheets and reports",
            "tracking recurring checks and deadlines",
            "keeping analysis threads organized",
        ),
    },
    {
        "key": "hardware",
        "label": "electronics and hardware",
        "extensions": {".kicad_pcb", ".kicad_sch", ".sch", ".brd", ".fzz", ".ino", ".gbr", ".drl", ".hex"},
        "files": {"platformio.ini", "board.kicad_pcb"},
        "keywords": {"kicad", "ltspice", "altium", "eagle", "arduino", "gerber", "schematic", "pcb", "firmware"},
        "uses": (
            "tracking design iterations and build files",
            "keeping hardware review work organized",
            "surfacing follow-ups across boards, schematics, and firmware",
        ),
    },
    {
        "key": "cad3d",
        "label": "3D and CAD work",
        "extensions": {".blend", ".stl", ".step", ".stp", ".iges", ".igs", ".fcstd", ".dwg", ".dxf", ".obj", ".3mf"},
        "files": {"project.blend", "assembly.step"},
        "keywords": {"blender", "fusion", "solidworks", "freecad", "cad", "print", "render", "model"},
        "uses": (
            "tracking model iterations",
            "keeping fabrication and review work visible",
            "organizing design handoffs and exports",
        ),
    },
    {
        "key": "audio",
        "label": "audio and music production",
        "extensions": {".als", ".logicx", ".flp", ".rpp", ".wav", ".aif", ".aiff", ".mid", ".midi"},
        "files": {"ableton project.als", "session.logicx"},
        "keywords": {"ableton", "reaper", "logic", "fl studio", "bitwig", "kontakt", "mix", "master"},
        "uses": (
            "tracking sessions and exports",
            "keeping production work organized",
            "surfacing loose ends across projects and mixes",
        ),
    },
    {
        "key": "video",
        "label": "video and motion work",
        "extensions": {".prproj", ".aep", ".drp", ".fcpxml", ".mov", ".mp4", ".mkv", ".srt"},
        "files": {"timeline.prproj", "edit.aep"},
        "keywords": {"premiere", "after effects", "davinci", "resolve", "final cut", "storyboard", "edit", "footage"},
        "uses": (
            "tracking edits and exports",
            "keeping production threads visible",
            "organizing follow-ups across media projects",
        ),
    },
    {
        "key": "marketing",
        "label": "messaging and launch work",
        "extensions": {".pptx", ".key"},
        "files": {"campaign-plan.md", "launch-plan.md"},
        "keywords": {"campaign", "launch", "newsletter", "marketing", "brand", "positioning", "copy", "roadmap"},
        "uses": (
            "keeping launch and messaging work coordinated",
            "tracking copy and campaign follow-ups",
            "surfacing cross-project loose ends",
        ),
    },
    {
        "key": "product",
        "label": "product and planning work",
        "extensions": {".roadmap", ".prd"},
        "files": {"roadmap.md", "prd.md", "spec.md", "strategy.md", "backlog.md"},
        "keywords": {"product", "roadmap", "spec", "requirements", "backlog", "planning", "priorities", "milestone", "strategy"},
        "uses": (
            "keeping priorities and plans coherent",
            "tracking what changed in the roadmap",
            "turning fuzzy requests into concrete next steps",
        ),
    },
    {
        "key": "education",
        "label": "teaching and learning work",
        "extensions": {".slides", ".lesson"},
        "files": {"lesson-plan.md", "syllabus.md", "curriculum.md"},
        "keywords": {"lesson", "syllabus", "curriculum", "course", "lecture", "teaching", "student", "classroom"},
        "uses": (
            "keeping teaching materials organized",
            "tracking lesson follow-ups",
            "surfacing open questions in learning material",
        ),
    },
    {
        "key": "legal",
        "label": "policy and legal work",
        "extensions": {".doc", ".contract"},
        "files": {"contract.md", "policy.md", "terms.md", "nda.md"},
        "keywords": {"contract", "policy", "nda", "compliance", "legal", "privacy", "terms", "agreement"},
        "uses": (
            "keeping policy and review work organized",
            "tracking revisions and deadlines",
            "surfacing documents that need another pass",
        ),
    },
)
SYSTEM_SCAN_TOOL_MARKERS: Mapping[str, tuple[str, ...]] = {
    "VS Code": ("vscode", ".vscode"),
    "JetBrains": ("pycharm", "intellij", "webstorm", "goland", "jetbrains"),
    "Xcode": ("xcode",),
    "Docker": ("docker", "docker-compose"),
    "Kubernetes": ("kubernetes", "k8s", "helm"),
    "Obsidian": ("obsidian",),
    "Logseq": ("logseq",),
    "Zotero": ("zotero",),
    "Figma": ("figma",),
    "Adobe": ("adobe", "photoshop", "illustrator", "after effects", "premiere"),
    "Blender": ("blender",),
    "KiCad": ("kicad",),
    "LTspice": ("ltspice",),
    "Ableton": ("ableton",),
    "Reaper": ("reaper",),
    "DaVinci Resolve": ("davinci", "resolve"),
    "Unity": ("unity",),
    "Unreal": ("unreal",),
    "Tableau": ("tableau",),
    "Power BI": ("powerbi", "pbix"),
}
SYSTEM_SCAN_PERSONA_HINTS: Mapping[str, tuple[str, ...]] = {
    "software": ("software builder", "developer"),
    "operations": ("operator", "infrastructure maintainer"),
    "writing": ("writer", "note-heavy thinker"),
    "design": ("visual designer", "brand or layout worker"),
    "research": ("researcher", "investigator"),
    "data": ("analyst", "data worker"),
    "finance": ("finance operator", "spreadsheet-heavy analyst"),
    "hardware": ("hardware engineer", "embedded tinkerer"),
    "cad3d": ("CAD or 3D designer", "fabrication-minded builder"),
    "audio": ("audio producer", "music maker"),
    "video": ("video or motion editor", "media producer"),
    "marketing": ("marketer", "launch planner"),
    "product": ("product planner", "roadmap shaper"),
    "education": ("teacher", "course builder"),
    "legal": ("policy or legal operator", "document-heavy reviewer"),
}


def _scan_display_path(path: Path, *, home: Path) -> str:
    try:
        rel_home = path.relative_to(home)
        return f"~/{rel_home.as_posix()}" if str(rel_home) != "." else "~"
    except ValueError:
        return str(path)


def _scan_label_from_dir_name(name: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "-", str(name or "").strip().lower()).strip("-")
    return normalized or "home-extra"


def _likely_work_home_dirs(home: Path) -> list[Path]:
    try:
        entries = sorted(home.iterdir(), key=lambda path: path.name.lower())
    except OSError:
        return []
    common_names = {name.lower() for name in SYSTEM_SCAN_COMMON_HOME_DIRS}
    matches: list[Path] = []
    for entry in entries:
        if len(matches) >= 16:
            break
        if not entry.is_dir() or entry.name.startswith("."):
            continue
        lowered_name = entry.name.strip().lower()
        if not lowered_name or lowered_name in common_names or lowered_name in SYSTEM_SCAN_IGNORED_DIR_NAMES:
            continue
        tokens = {token for token in re.split(r"[^a-z0-9]+", lowered_name) if token}
        if tokens & SYSTEM_SCAN_DYNAMIC_WORK_DIR_TOKENS:
            matches.append(entry)
    return matches


def _scan_name_tokens(path: Path, *, max_parts: int = 3) -> set[str]:
    tokens: set[str] = set()
    parts = [part for part in path.parts[-max_parts:] if part]
    for part in parts:
        lowered = str(part).strip().lower()
        if not lowered:
            continue
        tokens.add(lowered)
        tokens.update(token for token in re.split(r"[^a-z0-9]+", lowered) if token)
    stem = path.stem.strip().lower()
    if stem:
        tokens.add(stem)
        tokens.update(token for token in re.split(r"[^a-z0-9]+", stem) if token)
    return tokens


def _is_sensitive_signal_name(name: str) -> bool:
    lowered = str(name or "").strip().lower()
    return any(token in lowered for token in SYSTEM_SCAN_SENSITIVE_NAME_TOKENS)


def _is_noise_signal_path(path: Path) -> bool:
    lowered_name = path.name.strip().lower()
    lowered_path = path.as_posix().lower()
    if lowered_name in SYSTEM_SCAN_NOISE_FILE_NAMES:
        return True
    if path.suffix.lower() in SYSTEM_SCAN_NOISE_SUFFIXES:
        return True
    if "/.vscode-server/" in lowered_path or "/.npm/" in lowered_path or "/.pnpm-store/" in lowered_path:
        return True
    return False


def _can_peek_text_file(path: Path, *, size_bytes: int) -> bool:
    if size_bytes <= 0 or size_bytes > SYSTEM_SCAN_MAX_TEXT_FILE_BYTES:
        return False
    if _is_noise_signal_path(path) or _is_sensitive_signal_name(path.name):
        return False
    if path.suffix.lower() in SYSTEM_SCAN_TEXT_EXTENSIONS:
        return True
    return path.name.strip().lower() in {
        "readme",
        "readme.md",
        "package.json",
        "pyproject.toml",
        "requirements.txt",
        "dockerfile",
        "compose.yaml",
        "docker-compose.yml",
        "docker-compose.yaml",
        "makefile",
    }


def _read_text_sample(path: Path, *, max_bytes: int = SYSTEM_SCAN_TEXT_SAMPLE_BYTES) -> str:
    try:
        with open(path, "rb") as handle:
            raw = handle.read(max_bytes)
    except OSError:
        return ""
    if not raw or b"\x00" in raw:
        return ""
    try:
        return raw.decode("utf-8", errors="ignore")
    except Exception:
        return ""


def _system_scan_root_specs(home: Path) -> list[dict[str, Any]]:
    specs: list[dict[str, Any]] = []
    seen: set[Path] = set()

    def add(
        path: Path | None,
        *,
        label: str,
        max_depth: int,
        file_budget: int,
        include_hidden: bool = False,
        display_priority: int = 1,
    ) -> None:
        if path is None:
            return
        candidate = Path(path).expanduser()
        if not candidate.exists() or candidate in seen:
            return
        seen.add(candidate)
        specs.append(
            {
                "path": candidate,
                "label": label,
                "max_depth": max_depth,
                "file_budget": file_budget,
                "include_hidden": include_hidden,
                "display_priority": display_priority,
            }
        )

    add(home, label="home", max_depth=2, file_budget=900, display_priority=4)
    for directory_name in SYSTEM_SCAN_COMMON_HOME_DIRS:
        add(home / directory_name, label=directory_name.lower(), max_depth=4, file_budget=650, display_priority=5)
    for directory_path in _likely_work_home_dirs(home):
        add(directory_path, label=_scan_label_from_dir_name(directory_path.name), max_depth=4, file_budget=650, display_priority=5)

    system_name = platform.system()
    if system_name == "Linux":
        add(home / ".config", label="config", max_depth=3, file_budget=700, include_hidden=True, display_priority=3)
        add(home / ".local" / "share", label="local-share", max_depth=3, file_budget=500, include_hidden=True, display_priority=2)
        add(Path("/"), label="system-root", max_depth=1, file_budget=200, display_priority=1)
        add(Path("/etc"), label="etc", max_depth=2, file_budget=400, display_priority=1)
        add(Path("/opt"), label="opt", max_depth=2, file_budget=300, display_priority=2)
        add(Path("/srv"), label="srv", max_depth=2, file_budget=250, display_priority=2)
        add(Path("/var/www"), label="var-www", max_depth=2, file_budget=250, display_priority=2)
        add(Path("/usr/share/applications"), label="applications", max_depth=2, file_budget=400, display_priority=2)
    elif system_name == "Darwin":
        add(home / "Library" / "Application Support", label="app-support", max_depth=3, file_budget=800, include_hidden=True, display_priority=3)
        add(Path("/Applications"), label="applications", max_depth=2, file_budget=400, display_priority=2)
        add(Path("/"), label="system-root", max_depth=1, file_budget=200, display_priority=1)
    elif system_name == "Windows":
        add(home / "AppData" / "Roaming", label="appdata-roaming", max_depth=3, file_budget=800, include_hidden=True, display_priority=3)
        add(home / "AppData" / "Local", label="appdata-local", max_depth=2, file_budget=500, include_hidden=True, display_priority=2)
        drive_root = Path(home.anchor or os.getenv("SystemDrive", "C:\\"))
        add(drive_root, label="drive-root", max_depth=1, file_budget=200, display_priority=1)
        for env_name in ("ProgramFiles", "ProgramFiles(x86)"):
            raw = os.getenv(env_name, "").strip()
            if raw:
                add(Path(raw), label=env_name.lower(), max_depth=1, file_budget=200, display_priority=2)
    return specs


def _add_signal_score(
    scores: dict[str, int],
    evidence: dict[str, list[str]],
    *,
    key: str,
    points: int,
    note: str,
) -> None:
    if points <= 0:
        return
    scores[key] = scores.get(key, 0) + points
    bucket = evidence.setdefault(key, [])
    if note and note not in bucket and len(bucket) < 5:
        bucket.append(note)


def _project_signal_score(path: Path, *, display_label: str, tool_signals: set[str]) -> tuple[int, dict[str, int], dict[str, str]]:
    filename = path.name.strip().lower()
    extension = path.suffix.lower()
    nearby_tokens = _scan_name_tokens(path)
    domain_points: dict[str, int] = {}
    domain_notes: dict[str, str] = {}
    signal_score = 0

    joined_token_space = " ".join(sorted(nearby_tokens))
    for tool_label, markers in SYSTEM_SCAN_TOOL_MARKERS.items():
        if any(marker in joined_token_space for marker in markers):
            tool_signals.add(tool_label)

    for rule in SYSTEM_SCAN_DOMAIN_RULES:
        points = 0
        if filename in rule["files"]:
            points += 6
        if extension in rule["extensions"]:
            points += 3
        keyword_hits = [keyword for keyword in rule["keywords"] if keyword in nearby_tokens]
        if keyword_hits:
            points += min(4, len(keyword_hits) * 2)
        if points <= 0:
            continue
        domain_points[str(rule["key"])] = points
        domain_notes[str(rule["key"])] = display_label
        signal_score += points
    return signal_score, domain_points, domain_notes


def _infer_system_kind(*, scan: dict[str, Any], home: Path) -> tuple[str, str, list[str]]:
    system_name = str(scan.get("platform") or platform.system())
    gui_score = 0
    server_score = 0
    clues: list[str] = []

    if os.getenv("DISPLAY") or os.getenv("WAYLAND_DISPLAY") or os.getenv("XDG_CURRENT_DESKTOP"):
        gui_score += 3
        clues.append("live desktop session signals")
    if (home / "Desktop").exists() or (home / "Pictures").exists():
        gui_score += 1
    if any(tool in set(scan.get("tool_signals") or []) for tool in {"Figma", "Adobe", "Blender", "Ableton", "DaVinci Resolve"}):
        gui_score += 2
        clues.append("creative desktop tooling")
    if any(tool in set(scan.get("tool_signals") or []) for tool in {"VS Code", "JetBrains", "Xcode"}):
        gui_score += 1
    if Path("/sys/class/power_supply").exists():
        try:
            if any(entry.name.startswith("BAT") for entry in Path("/sys/class/power_supply").iterdir()):
                gui_score += 2
                clues.append("battery-backed machine")
        except OSError:
            pass

    if Path("/etc/systemd/system").exists():
        server_score += 2
        clues.append("systemd service layout")
    if Path("/var/run/docker.sock").exists():
        server_score += 2
        clues.append("docker socket")
    if (home == Path("/root")) or (hasattr(os, "geteuid") and os.geteuid() == 0):
        server_score += 2
        clues.append("privileged account")
    if any(item.get("key") == "operations" and int(item.get("score") or 0) >= 10 for item in list(scan.get("work_signals") or [])):
        server_score += 2
        clues.append("strong operations signatures")

    if system_name == "Darwin":
        return ("desktop", "macOS workstation", clues or ["macOS host"])
    if system_name == "Windows":
        return ("desktop", "Windows workstation", clues or ["Windows host"])
    if system_name == "Linux":
        if server_score >= gui_score + 2:
            return ("server", "Linux server", clues or ["Linux host with server-like signals"])
        if gui_score > 0:
            return ("desktop", "Linux desktop", clues or ["Linux host with desktop signals"])
        return ("system", "Linux system", clues or ["general Linux host"])
    return ("system", system_name or "system", clues or ["general host"])


def _summarize_work_signals(scores: dict[str, int], evidence: dict[str, list[str]]) -> list[dict[str, Any]]:
    ordered: list[dict[str, Any]] = []
    for rule in SYSTEM_SCAN_DOMAIN_RULES:
        key = str(rule["key"])
        score = int(scores.get(key) or 0)
        if score <= 0:
            continue
        ordered.append(
            {
                "key": key,
                "label": str(rule["label"]),
                "score": score,
                "evidence": list(evidence.get(key) or [])[:4],
                "uses": list(rule["uses"]),
            }
        )
    ordered.sort(key=lambda item: (-int(item["score"]), str(item["label"]).lower()))
    if not ordered:
        ordered.append(
            {
                "key": "general",
                "label": "general project work",
                "score": 1,
                "evidence": [],
                "uses": [
                    "keeping recurring work and follow-ups visible",
                    "tracking what changed recently",
                    "helping shape the next sensible step",
                ],
            }
        )
    return ordered[:4]


def _recommended_creatureos_uses(work_signals: list[dict[str, Any]]) -> list[str]:
    uses: list[str] = []
    for item in work_signals[:3]:
        for phrase in list(item.get("uses") or []):
            if phrase not in uses:
                uses.append(str(phrase))
    return uses[:3]


def _split_work_signal_bands(work_signals: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if not work_signals:
        return [], []
    top_score = max(1, int(work_signals[0].get("score") or 1))
    primary_cutoff = max(6, int(top_score * 0.55))
    secondary_cutoff = max(3, int(top_score * 0.25))
    primary: list[dict[str, Any]] = []
    secondary: list[dict[str, Any]] = []
    for index, item in enumerate(work_signals):
        score = int(item.get("score") or 0)
        if index == 0 or score >= primary_cutoff:
            primary.append(item)
            continue
        if score >= secondary_cutoff:
            secondary.append(item)
    if not primary:
        primary.append(work_signals[0])
    return primary[:3], secondary[:3]


def _persona_hints_for_signals(work_signals: list[dict[str, Any]], *, max_hints: int = 4) -> list[str]:
    hints: list[str] = []
    for item in work_signals[:3]:
        for hint in SYSTEM_SCAN_PERSONA_HINTS.get(str(item.get("key") or ""), ()):
            if hint not in hints:
                hints.append(hint)
            if len(hints) >= max_hints:
                return hints[:max_hints]
    return hints[:max_hints]


def _likely_persona_hints(work_signals: list[dict[str, Any]]) -> list[str]:
    return _persona_hints_for_signals(work_signals, max_hints=4)


def _persona_hint_interest_phrase(hint: str) -> str:
    normalized = str(hint or "").strip().lower()
    mapping = {
        "software builder": "building software",
        "developer": "making tools",
        "operator": "keeping systems steady",
        "infrastructure maintainer": "tending infrastructure",
        "writer": "writing",
        "note-heavy thinker": "shaping ideas in words",
        "visual designer": "shaping how things look and feel",
        "brand or layout worker": "arranging visual systems",
        "researcher": "following open questions",
        "investigator": "digging into what is really going on",
        "analyst": "sorting through patterns",
        "data worker": "working through evidence",
        "finance operator": "tracking money and movement",
        "spreadsheet-heavy analyst": "making sense of numbers",
        "hardware engineer": "building with physical systems",
        "embedded tinkerer": "working close to the machine",
        "cad or 3d designer": "shaping things in space",
        "fabrication-minded builder": "turning designs into real objects",
        "audio producer": "working with sound",
        "music maker": "making music",
        "video or motion editor": "shaping moving images",
        "media producer": "assembling media work",
        "marketer": "finding how things should be presented",
        "launch planner": "bringing things into the world",
        "product planner": "shaping what gets built next",
        "roadmap shaper": "arranging the path ahead",
        "teacher": "teaching",
        "course builder": "turning knowledge into structure",
        "policy or legal operator": "working through rules and obligations",
        "document-heavy reviewer": "reading closely and carefully",
    }
    return mapping.get(normalized, str(hint or "").strip())


def _persona_hint_interest_phrases(hints: Sequence[str], *, limit: int = 4) -> list[str]:
    phrases: list[str] = []
    for item in list(hints)[:limit]:
        phrase = _persona_hint_interest_phrase(str(item))
        if phrase and phrase not in phrases:
            phrases.append(phrase)
    return phrases


def _scan_domain_interest_phrase(signal_key: str) -> str:
    mapping = {
        "software": "work with software",
        "operations": "work with software and systems",
        "writing": "work with words",
        "design": "shape how things look and feel",
        "research": "chase difficult questions",
        "data": "work with patterns and evidence",
        "finance": "work with money, trade, or numbers",
        "hardware": "work close to the machine",
        "cad3d": "shape things before they become real",
        "audio": "work with sound",
        "video": "shape moving images",
        "marketing": "shape how things are understood",
        "product": "decide what should exist next",
        "education": "teach and guide learning",
        "legal": "work with rules, obligations, and terms",
        "general": "make and tend things that matter to you",
    }
    return mapping.get(str(signal_key or "").strip().lower(), "make and tend things that matter to you")


def _keeper_ecosystem_invocation(ecosystem_value: str) -> str:
    normalized = str(ecosystem_value or "").strip().lower()
    choices = KEEPER_ECOSYSTEM_INVOCATIONS.get(normalized) or ()
    if choices:
        return random.choice(list(choices))
    ecosystem = ECOSYSTEM_INDEX.get(normalized) or ECOSYSTEM_INDEX.get(DEFAULT_ECOSYSTEM) or {"label": "this place"}
    return f"Ah. **{ecosystem['label']}**. Every place has its own manner of calling things into being."


def _scan_desire_phrase(signal_key: str) -> str:
    options = {
        "software": (
            "to create the greatest app to ever exist",
            "to build something elegant, useful, and impossible to ignore",
            "to bring a powerful piece of software into the world",
        ),
        "operations": (
            "to keep something vital standing against drift and ruin",
            "to bring order to a system that must not fail",
            "to make a living machine run cleanly and endure",
        ),
        "writing": (
            "to write the greatest book in the world",
            "to say something true enough to last",
            "to shape words into something unforgettable",
        ),
        "design": (
            "to make something beautiful enough that people feel it at once",
            "to give form to a thing before anyone else can see it",
            "to shape something with taste, clarity, and force",
        ),
        "research": (
            "to uncover something worth knowing",
            "to follow a question until it yields its truth",
            "to make sense of something difficult and deep",
        ),
        "data": (
            "to make hidden patterns reveal themselves",
            "to turn noise into understanding",
            "to see clearly what others only sense dimly",
        ),
        "finance": (
            "to understand the movement of money well enough to act wisely",
            "to bring order and foresight to things that can easily become chaos",
            "to see the numbers clearly enough to choose well",
        ),
        "hardware": (
            "to make something real with your own hands and mind",
            "to draw thought down into matter",
            "to coax intelligence out of circuits and parts",
        ),
        "cad3d": (
            "to shape something worth bringing into the physical world",
            "to give form to something before it exists",
            "to turn an idea into an object that can stand in the light",
        ),
        "audio": (
            "to make something worth hearing again and again",
            "to shape sound into atmosphere, memory, or force",
            "to bring music or sound into a more perfect form",
        ),
        "video": (
            "to shape a sequence of images into something unforgettable",
            "to make motion carry feeling and meaning",
            "to bring a story into sight",
        ),
        "marketing": (
            "to make something resonate far beyond its small beginnings",
            "to give a thing its voice in the world",
            "to bring attention to something that deserves it",
        ),
        "product": (
            "to decide what should exist and give it its proper shape",
            "to turn uncertainty into a path forward",
            "to bring clarity to what ought to be built next",
        ),
        "education": (
            "to help something difficult become clear",
            "to pass understanding from one mind into another",
            "to turn knowledge into guidance that actually lands",
        ),
        "legal": (
            "to bring order to things bound by rules and consequence",
            "to read the fine print well enough to protect what matters",
            "to make obligations and boundaries legible",
        ),
        "general": (
            "to make something meaningful and enduring",
            "to bring order, clarity, or beauty where it is needed",
            "to shape the next part of your work into something better",
        ),
    }
    choices = options.get(str(signal_key or "").strip().lower(), options["general"])
    return random.choice(choices)


def _scan_root_label_display(label: str) -> str:
    normalized = str(label or "").strip().lower()
    mapping = {
        "home": "your home directory",
        "config": "local app config",
        "local-share": "local app data",
        "applications": "installed applications",
        "etc": "system config",
        "system-root": "the base system",
        "opt": "optional system software",
        "srv": "service directories",
        "var-www": "served web roots",
        "app-support": "application support",
        "appdata-roaming": "roaming app data",
        "appdata-local": "local app data",
        "drive-root": "the main drive",
    }
    return mapping.get(normalized, normalized.replace("-", " "))


def _root_label_from_display_path(display_path: str) -> str:
    path = str(display_path or "").strip()
    if not path:
        return ""
    for directory_name in SYSTEM_SCAN_COMMON_HOME_DIRS:
        normalized_label = _scan_label_from_dir_name(directory_name)
        if path == f"~/{directory_name}" or path.startswith(f"~/{directory_name}/"):
            return normalized_label
    if path.startswith("~/.config/"):
        return "config"
    if path.startswith("~/.local/share/"):
        return "local-share"
    if path.startswith("~/"):
        return "home"
    if path.startswith("/etc/"):
        return "etc"
    if path.startswith("/opt/"):
        return "opt"
    if path.startswith("/srv/"):
        return "srv"
    if path.startswith("/var/www/"):
        return "var-www"
    if path.startswith("/Applications/"):
        return "applications"
    return "system-root"


def _human_join(items: Sequence[str], *, conjunction: str = "and") -> str:
    cleaned = [str(item).strip() for item in items if str(item).strip()]
    if not cleaned:
        return ""
    if len(cleaned) == 1:
        return cleaned[0]
    if len(cleaned) == 2:
        return f"{cleaned[0]} {conjunction} {cleaned[1]}"
    return f"{', '.join(cleaned[:-1])}, {conjunction} {cleaned[-1]}"


def _creatureos_intro_scan_lines(scan: dict[str, Any]) -> list[str]:
    work_signals = list(scan.get("work_signals") or [])
    lines: list[str] = []
    primary_signal = dict(work_signals[0]) if work_signals else {}
    secondary_signal = dict(work_signals[1]) if len(work_signals) > 1 else {}
    primary_interest = _scan_domain_interest_phrase(str(primary_signal.get("key") or "general"))
    secondary_interest = _scan_domain_interest_phrase(str(secondary_signal.get("key") or ""))
    if secondary_interest and (secondary_interest == primary_interest or secondary_interest in primary_interest or primary_interest in secondary_interest):
        secondary_interest = ""
    if primary_signal and secondary_signal and secondary_interest:
        lines.append(
            random.choice(
                [
                    f"If I read the traces rightly, you appear to **{primary_interest}**, perhaps with some **{secondary_interest}** as well.",
                    f"The shape of the place suggests that you **{primary_interest}**, though I can feel some **{secondary_interest}** moving nearby too.",
                    f"If I were forced to guess, I would say you **{primary_interest}**, with a little **{secondary_interest}** trailing behind it.",
                ]
            )
        )
    elif primary_signal:
        lines.append(
            random.choice(
                [
                    f"If I read the traces rightly, you appear to **{primary_interest}**.",
                    f"The strongest current here suggests that you **{primary_interest}**.",
                    f"My first guess is that you are here to **{primary_interest}**.",
                ]
            )
        )
    lines.append(
        random.choice(
            [
                f"If I had to guess at a desire, perhaps it is **{_scan_desire_phrase(str(primary_signal.get('key') or 'general'))}**.",
                f"If I were to name a desire in the room, I would call it **{_scan_desire_phrase(str(primary_signal.get('key') or 'general'))}**.",
                f"Perhaps the deeper desire here is **{_scan_desire_phrase(str(primary_signal.get('key') or 'general'))}**.",
            ]
        )
    )
    return lines


def _project_environment_scan() -> dict[str, Any]:
    cached = _load_meta_json_dict(ONBOARDING_ENVIRONMENT_KEY)
    if (
        cached
        and int(cached.get("version") or 0) == ONBOARDING_ENVIRONMENT_VERSION
        and str(cached.get("system_kind") or "").strip()
        and isinstance(cached.get("persona_hints"), list)
        and isinstance(cached.get("primary_work_signals"), list)
    ):
        return cached

    home = Path.home()
    username = getpass.getuser().strip() or home.name
    deadline = monotonic() + SYSTEM_SCAN_DEADLINE_SECONDS
    interesting_files: list[str] = []
    interesting_filename_hits: set[str] = set()
    suffix_counts: dict[str, int] = {}
    top_level_dirs: list[str] = []
    roots_scanned: list[str] = []
    tool_signals: set[str] = set()
    domain_scores: dict[str, int] = {}
    domain_evidence: dict[str, list[str]] = {}
    signal_candidates: list[dict[str, Any]] = []
    text_peek_candidates: list[dict[str, Any]] = []
    seen_files: set[str] = set()
    total_files = 0
    try:
        top_level_entries = sorted(home.iterdir(), key=lambda path: path.name.lower())
    except OSError:
        top_level_entries = []
    for entry in top_level_entries[:24]:
        if entry.is_dir() and not entry.name.startswith(".") and entry.name not in SYSTEM_SCAN_IGNORED_DIR_NAMES:
            top_level_dirs.append(entry.name)

    for spec in _system_scan_root_specs(home):
        if total_files >= SYSTEM_SCAN_ROOT_FILE_BUDGET or monotonic() >= deadline:
            break
        current_root_path = Path(spec["path"])
        include_hidden = bool(spec.get("include_hidden"))
        display_priority = int(spec.get("display_priority") or 1)
        roots_scanned.append(_scan_display_path(current_root_path, home=home))
        files_for_root = 0
        for current_root_str, dirs, files in os.walk(current_root_path):
            if total_files >= SYSTEM_SCAN_ROOT_FILE_BUDGET or monotonic() >= deadline or files_for_root >= int(spec["file_budget"]):
                break
            current_dir = Path(current_root_str)
            try:
                depth = len(current_dir.relative_to(current_root_path).parts)
            except ValueError:
                depth = 0
            dirs[:] = sorted(
                [
                    name
                    for name in dirs
                    if name not in SYSTEM_SCAN_IGNORED_DIR_NAMES
                    and (include_hidden or not name.startswith("."))
                    and not name.startswith(".cache")
                    and not name.startswith(".Trash")
                ],
                key=str.lower,
            )
            if depth >= int(spec["max_depth"]):
                dirs[:] = []
            for filename in sorted(files, key=str.lower):
                if total_files >= SYSTEM_SCAN_ROOT_FILE_BUDGET or monotonic() >= deadline or files_for_root >= int(spec["file_budget"]):
                    break
                if filename.startswith(".") and not include_hidden and filename.lower() not in SYSTEM_SCAN_USEFUL_DOTFILES:
                    continue
                absolute_path = current_dir / filename
                path_key = str(absolute_path)
                if path_key in seen_files or absolute_path.is_symlink():
                    continue
                seen_files.add(path_key)
                total_files += 1
                files_for_root += 1
                try:
                    stat_result = absolute_path.stat()
                    modified_at = float(stat_result.st_mtime)
                    size_bytes = int(stat_result.st_size)
                except OSError:
                    modified_at = 0.0
                    size_bytes = 0
                suffix = absolute_path.suffix.lower()
                suffix_counts[suffix] = suffix_counts.get(suffix, 0) + 1
                lowered = filename.lower()
                display_path = _scan_display_path(absolute_path, home=home)
                if display_priority >= 5 and lowered in {
                    "package.json",
                    "pyproject.toml",
                    "dockerfile",
                    "docker-compose.yml",
                    "docker-compose.yaml",
                    "requirements.txt",
                    "manage.py",
                    "vite.config.ts",
                    "vite.config.js",
                    "next.config.js",
                    "tailwind.config.js",
                    "nginx.conf",
                }:
                    interesting_filename_hits.add(lowered)
                    if len(interesting_files) < 18:
                        interesting_files.append(display_path)
                signal_score, domain_points, domain_notes = _project_signal_score(
                    absolute_path,
                    display_label=display_path,
                    tool_signals=tool_signals,
                )
                for domain_key, points in domain_points.items():
                    _add_signal_score(
                        domain_scores,
                        domain_evidence,
                        key=domain_key,
                        points=points,
                        note=domain_notes.get(domain_key, display_path),
                    )
                candidate_record: dict[str, Any] | None = None
                if signal_score > 0 and not _is_sensitive_signal_name(filename) and not _is_noise_signal_path(absolute_path):
                    candidate_record = {
                        "score": signal_score,
                        "name": absolute_path.name,
                        "path": display_path,
                        "modified_at": modified_at,
                        "priority": display_priority,
                        "root_label": str(spec["label"]),
                    }
                    signal_candidates.append(candidate_record)
                if _can_peek_text_file(absolute_path, size_bytes=size_bytes):
                    text_peek_candidates.append(
                        {
                            "absolute_path": absolute_path,
                            "display_path": display_path,
                            "modified_at": modified_at,
                            "score": signal_score,
                            "priority": display_priority,
                            "candidate": candidate_record,
                        }
                    )

    text_peek_candidates.sort(
        key=lambda item: (-int(item["score"]), -int(item.get("priority") or 1), -float(item["modified_at"]), str(item["display_path"]).lower())
    )
    for item in text_peek_candidates[:SYSTEM_SCAN_TEXT_FILE_BUDGET]:
        sample = _read_text_sample(Path(item["absolute_path"]))
        if not sample:
            continue
        lowered_sample = sample.lower()
        content_hits = 0
        for rule in SYSTEM_SCAN_DOMAIN_RULES:
            keywords = [keyword for keyword in rule["keywords"] if keyword in lowered_sample]
            if not keywords:
                continue
            points = min(4, len(keywords))
            content_hits += points
            _add_signal_score(
                domain_scores,
                domain_evidence,
                key=str(rule["key"]),
                points=points,
                note=str(item["display_path"]),
            )
        if item.get("candidate") is not None and content_hits > 0:
            item["candidate"]["score"] = int(item["candidate"].get("score") or 0) + content_hits

    lower_dirs = {name.lower() for name in top_level_dirs}
    visual_asset_count = sum(
        suffix_counts.get(ext, 0)
        for ext in (".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".psd", ".ai", ".kra", ".blend")
    )
    work_signals = _summarize_work_signals(domain_scores, domain_evidence)
    primary_work_signals, secondary_work_signals = _split_work_signal_bands(work_signals)
    recommended_uses = _recommended_creatureos_uses(work_signals)
    primary_persona_hints = _persona_hints_for_signals(primary_work_signals, max_hints=4)
    secondary_persona_hints = _persona_hints_for_signals(secondary_work_signals, max_hints=3)
    persona_hints = list(dict.fromkeys([*primary_persona_hints, *secondary_persona_hints]))[:5]
    signal_candidates.sort(
        key=lambda item: (-int(item["score"]), -int(item.get("priority") or 1), -float(item["modified_at"]), str(item["name"]).lower())
    )
    display_signal_candidates = sorted(
        signal_candidates,
        key=lambda item: (-int(item.get("priority") or 1), -int(item["score"]), -float(item["modified_at"]), str(item["name"]).lower()),
    )
    signal_root_labels: list[str] = []
    for item in display_signal_candidates:
        label = str(item.get("root_label") or "").strip()
        if label and label not in signal_root_labels:
            signal_root_labels.append(label)
        if len(signal_root_labels) >= 4:
            break
    recent_signal_files = [
        {
            "score": int(item["score"]),
            "name": str(item["name"]),
            "path": str(item["path"]),
            "modified_at": float(item["modified_at"]),
        }
        for item in display_signal_candidates[:8]
    ]
    if not signal_root_labels:
        for item in recent_signal_files:
            label = _root_label_from_display_path(str(item.get("path") or ""))
            if label and label not in signal_root_labels:
                signal_root_labels.append(label)
            if len(signal_root_labels) >= 4:
                break
    system_kind, system_label, system_clues = _infer_system_kind(
        scan={
            "platform": platform.system(),
            "tool_signals": sorted(tool_signals),
            "work_signals": work_signals,
        },
        home=home,
    )
    scan = {
        "version": ONBOARDING_ENVIRONMENT_VERSION,
        "scan_scope": "likely-machine-work-roots",
        "username": username,
        "home": str(home),
        "hostname": platform.node(),
        "platform": platform.system(),
        "platform_release": platform.release(),
        "machine": platform.machine(),
        "python_version": platform.python_version(),
        "interesting_files": interesting_files,
        "top_level_dirs": top_level_dirs[:12],
        "roots_scanned": roots_scanned[:16],
        "total_files_scanned": total_files,
        "suffix_counts": {key: value for key, value in sorted(suffix_counts.items())[:30]},
        "has_python": bool({"pyproject.toml", "requirements.txt", "manage.py"} & interesting_filename_hits) or suffix_counts.get(".py", 0) > 0,
        "has_javascript": bool({"package.json", "vite.config.ts", "vite.config.js", "next.config.js", "tailwind.config.js"} & interesting_filename_hits)
        or suffix_counts.get(".js", 0) > 0
        or suffix_counts.get(".ts", 0) > 0,
        "has_web_templates": "templates" in lower_dirs or suffix_counts.get(".html", 0) > 0 or suffix_counts.get(".jinja", 0) > 0 or suffix_counts.get(".jinja2", 0) > 0,
        "has_static_assets": "static" in lower_dirs or visual_asset_count > 12,
        "has_multiple_sites": len([name for name in top_level_dirs if name.lower() in {"sites", "apps", "services", "projects", "code", "repos", "workspace", "work"}]) > 1,
        "has_docker": "Docker" in tool_signals or any("docker" in item.lower() for item in interesting_files),
        "visual_asset_count": visual_asset_count,
        "system_kind": system_kind,
        "system_label": system_label,
        "system_clues": system_clues[:4],
        "tool_signals": sorted(tool_signals)[:14],
        "work_signals": work_signals,
        "primary_work_signals": primary_work_signals,
        "secondary_work_signals": secondary_work_signals,
        "persona_hints": persona_hints,
        "primary_persona_hints": primary_persona_hints,
        "secondary_persona_hints": secondary_persona_hints,
        "recommended_uses": recommended_uses,
        "signal_root_labels": signal_root_labels,
        "recent_signal_files": recent_signal_files,
    }
    _store_meta_json(ONBOARDING_ENVIRONMENT_KEY, scan)
    return scan


def _environment_observations(scan: dict[str, Any]) -> list[str]:
    observations: list[str] = []
    system_label = str(scan.get("system_label") or "").strip()
    work_signals = list(scan.get("work_signals") or [])
    primary_work_signals = list(scan.get("primary_work_signals") or work_signals[:1])
    secondary_work_signals = list(scan.get("secondary_work_signals") or [])
    primary_persona_hints = [str(item).strip() for item in list(scan.get("primary_persona_hints") or []) if str(item).strip()]
    secondary_persona_hints = [str(item).strip() for item in list(scan.get("secondary_persona_hints") or []) if str(item).strip()]
    primary_interest_phrases = _persona_hint_interest_phrases(primary_persona_hints, limit=3)
    secondary_interest_phrases = _persona_hint_interest_phrases(secondary_persona_hints, limit=2)
    recommended_uses = [str(item).strip() for item in list(scan.get("recommended_uses") or []) if str(item).strip()]
    signal_roots = [str(item).strip() for item in list(scan.get("signal_root_labels") or []) if str(item).strip()]

    if system_label:
        observations.append(f"This feels more like a living {system_label.lower()} than a bare checkout left on a shelf.")
    if primary_work_signals:
        signal_labels = _human_join([str(item['label']) for item in primary_work_signals[:3]])
        observations.append(f"The strongest traces point toward {signal_labels} work from the files, layout, and tooling I found here.")
    if secondary_work_signals:
        observations.append(f"There is another current running through it too: {_human_join([str(item['label']) for item in secondary_work_signals[:3]])}, so the environment does not feel one-track.")
    if len(signal_roots) >= 2:
        observations.append(f"The clues are scattered through more than one part of the habitat, especially {_human_join([_scan_root_label_display(item) for item in signal_roots[:3]])}.")
    if primary_interest_phrases and secondary_interest_phrases:
        observations.append(f"If I had to guess at your interests from the marks alone, I’d say {_human_join(primary_interest_phrases[:3])}, with some {_human_join(secondary_interest_phrases[:2])} woven through it.")
    elif primary_interest_phrases:
        observations.append(f"If I had to guess at your interests from the marks alone, I’d place them closest to {_human_join(primary_interest_phrases[:3])}.")
    if recommended_uses:
        observations.append(f"My first hunch is that CreatureOS could help most with { _human_join(recommended_uses[:3]) }.")
    if scan.get("has_python") and scan.get("has_web_templates") and scan.get("has_static_assets"):
        observations.append("There’s enough app structure here that I’d treat this like a living web surface, not just code sitting on disk.")
    elif scan.get("has_python"):
        observations.append("The repo is Python-heavy, so service code, automation, or operational tooling is probably part of the day-to-day work.")
    if scan.get("has_docker"):
        observations.append("I also found Docker or deploy-style clues, so setup, shipping, and environment drift probably matter here too.")
    if scan.get("platform") == "Darwin":
        observations.append("This is running on macOS, which usually means local development or creative work matters as much as pure server upkeep.")
    if not observations:
        observations.append("I can read the shape of the place, but I still need your voice to know what matters most.")
    return observations[:4]


def _fallback_onboarding_briefing(ecosystem_value: str, scan: dict[str, Any]) -> dict[str, Any]:
    observations = _environment_observations(scan)
    top_signal = dict((list(scan.get("work_signals") or []) or [{}])[0] or {})
    top_interest = _scan_domain_interest_phrase(str(top_signal.get("key") or "general"))
    questions = [
        "What are you trying to bring into the world here?",
        "Where do you want help first: with making, tending, understanding, or finishing?",
        "Should the creatures be quiet and watchful, or bold enough to bring you strange and useful ideas?",
    ]
    if top_signal:
        questions[0] = f"If I have read the room at all, you may be here to **{top_interest}**. Is that true, or is the real desire elsewhere?"
    return {
        "summary": "I’ve already taken a quiet first reading of the machine and the places where your work seems to gather. Give me a little context, and I’ll turn that first impression into the right first creature.",
        "observations": observations,
        "questions": questions,
        "ecosystem": ecosystem_value,
        "version": ONBOARDING_BRIEFING_VERSION,
    }


def _load_onboarding_briefing_cache() -> dict[str, Any]:
    cached = _load_meta_json_dict(ONBOARDING_BRIEFING_KEY)
    by_ecosystem = cached.get("by_ecosystem")
    if isinstance(by_ecosystem, dict):
        return cached
    ecosystem_value = str(cached.get("ecosystem") or "").strip()
    if ecosystem_value:
        return {"by_ecosystem": {ecosystem_value: cached}}
    return {"by_ecosystem": {}}


def _store_onboarding_briefing(ecosystem_value: str, briefing: dict[str, Any]) -> None:
    cache = _load_onboarding_briefing_cache()
    by_ecosystem = cache.get("by_ecosystem") if isinstance(cache.get("by_ecosystem"), dict) else {}
    by_ecosystem[str(ecosystem_value)] = briefing
    _store_meta_json(ONBOARDING_BRIEFING_KEY, {"by_ecosystem": by_ecosystem})


def _onboarding_briefing_prompt(ecosystem_value: str, scan: dict[str, Any]) -> str:
    ecosystem_label = ECOSYSTEM_INDEX[ecosystem_value]["label"]
    return "\n\n".join(
        [
            "You are the onboarding keeper for CreatureOS.",
            "Your job is to orient a new user, grounded in the actual local environment, before they summon any creatures.",
            "Use a light mythic tone: calm, suggestive, and grounded, not theatrical.",
            "Speak plainly. Use the ecosystem metaphor lightly. Avoid cringe, hype, or mascot-role language.",
            "When discussing creature work, keep the work definitions professional and normal. Cute theming belongs in names, not in role descriptions.",
            f"The selected ecosystem is {ecosystem_label}. For onboarding, that sets the opening location and atmosphere, not the origin ecosystem of every creature that may later be summoned.",
            "You are given a structured environment scan from the current machine, especially the directories where the human's work seems most likely to live.",
            "Do not overclaim certainty. Treat the scan as traces and first impressions, then make grounded guesses about what the human may care about.",
            "Return JSON only with this shape:",
            json.dumps(
                {
                    "summary": "One short paragraph.",
                    "observations": ["Three or four concise grounded observations."],
                    "questions": ["Three or four tuned questions for the human."],
                },
                indent=2,
            ),
            "Keep each observation and question to one sentence.",
            f"Environment scan:\n{json.dumps(scan, indent=2)}",
        ]
    )


def _run_onboarding_thread_text(
    *,
    prompt: str,
    on_event: Callable[[dict[str, Any]], None] | None = None,
) -> str | None:
    thread_id = str(storage.get_meta(ONBOARDING_THREAD_ID_KEY) or "").strip()
    def handle_event(event: dict[str, Any]) -> None:
        _capture_thread_id_from_event(event, meta_key=ONBOARDING_THREAD_ID_KEY)
        if on_event is not None:
            on_event(event)
    try:
        if thread_id:
            result = _codex_resume_thread(
                workdir=str(config.workspace_root()),
                thread_id=thread_id,
                prompt=prompt,
                sandbox_mode="read-only",
                on_event=handle_event,
            )
        else:
            result = _codex_start_thread(
                workdir=str(config.workspace_root()),
                prompt=prompt,
                sandbox_mode="read-only",
                on_event=handle_event,
            )
        if result.thread_id:
            storage.set_meta(ONBOARDING_THREAD_ID_KEY, str(result.thread_id))
        final_text = str(result.final_text or "").strip()
        return final_text or None
    except (CodexCommandError, CodexTimeoutError):
        return None


def _run_onboarding_thread(
    *,
    prompt: str,
    on_event: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any] | None:
    raw_text = _run_onboarding_thread_text(prompt=prompt, on_event=on_event)
    if not raw_text:
        return None
    return _extract_json_object(raw_text)


def _keeper_run_notes_markdown(
    *,
    keeper: Any,
    conversation: Any,
    trigger_type: str,
    prompt_kind: str,
    output_text: str,
    summary: str,
) -> str:
    lines = [
        f"# {keeper['display_name']} run",
        "",
        f"- Trigger: {trigger_type}",
        f"- Scope: chat",
        f"- Conversation: {str(_row_value(conversation, 'title') or KEEPER_CONVERSATION_TITLE)}",
        f"- Summary: {summary}",
        "",
        "## Prompt Kind",
        prompt_kind,
        "",
        "## Output",
        output_text or "No output returned.",
        "",
    ]
    return "\n".join(lines)


def _run_keeper_conversation_prompt(
    *,
    keeper: Any,
    conversation: Any,
    prompt_text: str,
    trigger_type: str,
    prompt_kind: str,
    on_event: Callable[[dict[str, Any]], None] | None = None,
) -> tuple[str, str, int]:
    thread_id = str(_row_value(conversation, "codex_thread_id") or "").strip()
    run_row = storage.create_run(
        int(keeper["id"]),
        trigger_type=trigger_type,
        prompt_text=prompt_text,
        thread_id=thread_id or None,
        conversation_id=int(conversation["id"]),
        run_scope=RUN_SCOPE_CHAT,
        sandbox_mode="read-only",
    )
    run_id = int(run_row["id"])
    try:
        def forward_event(event: dict[str, Any]) -> None:
            timed_event = _annotate_run_feed_event_timing(run_id, event)
            _capture_thread_id_from_event(
                timed_event,
                run_id=run_id,
                conversation_id=int(conversation["id"]),
            )
            stored_event_id = _append_run_feed_event(run_id, timed_event)
            if on_event is not None:
                on_event({**timed_event, "_run_id": run_id, "_stored_event_id": stored_event_id})
        started_event = storage.create_run_event(
            run_id,
            event_type="status",
            body='{"type":"status","phase":"started","sandbox_mode":"read-only"}',
            metadata={"phase": "started", "sandbox_mode": "read-only"},
        )
        started_event_id = int(started_event["id"])
        if on_event is not None:
            on_event(
                {
                    "type": "status",
                    "phase": "started",
                    "sandbox_mode": "read-only",
                    "_run_id": run_id,
                    "_stored_event_id": started_event_id,
                }
            )
        if thread_id:
            result = _codex_resume_thread(
                workdir=str(config.workspace_root()),
                thread_id=thread_id,
                prompt=prompt_text,
                model=_creature_model(keeper),
                reasoning_effort=_creature_reasoning_effort(keeper),
                sandbox_mode="read-only",
                on_event=forward_event,
            )
        else:
            result = _codex_start_thread(
                workdir=str(config.workspace_root()),
                prompt=prompt_text,
                model=_creature_model(keeper),
                reasoning_effort=_creature_reasoning_effort(keeper),
                sandbox_mode="read-only",
                on_event=forward_event,
            )
        new_thread_id = str(result.thread_id or thread_id or "").strip()
        if new_thread_id:
            storage.set_conversation_thread_id(int(conversation["id"]), new_thread_id)
        output_text = str(result.final_text or "").strip()
        summary = _ensure_sentence(prompt_kind)
        notes_markdown = _keeper_run_notes_markdown(
            keeper=keeper,
            conversation=conversation,
            trigger_type=trigger_type,
            prompt_kind=prompt_kind,
            output_text=output_text,
            summary=summary,
        )
        storage.finish_run(
            run_id,
            creature_id=int(keeper["id"]),
            status="completed",
            raw_output_text=output_text,
            summary=summary,
            severity="info",
            message_text=None,
            error_text=None,
            next_run_at=None,
            metadata={
                "run_scope": RUN_SCOPE_CHAT,
                "conversation_id": int(conversation["id"]),
                "conversation_title": str(_row_value(conversation, "title") or KEEPER_CONVERSATION_TITLE),
                "sandbox_mode": "read-only",
                "prompt_kind": prompt_kind,
                "system_role": KEEPER_SYSTEM_ROLE,
                "owner_mode": DEFAULT_OWNER_MODE,
                "new_thread_id": new_thread_id,
            },
            notes_markdown=notes_markdown,
        )
        storage.create_run_event(
            run_id,
            event_type="status",
            body='{"type":"status","phase":"completed"}',
            metadata={"phase": "completed"},
        )
        _clear_run_feed_event_timing(run_id)
        return output_text, new_thread_id, run_id
    except Exception as exc:
        error_text = _friendly_run_error(exc, sandbox_mode="read-only")
        storage.create_run_event(run_id, event_type="error", body=error_text, metadata={"error_text": error_text})
        storage.create_run_event(
            run_id,
            event_type="status",
            body='{"type":"status","phase":"failed"}',
            metadata={"phase": "failed"},
        )
        _clear_run_feed_event_timing(run_id)
        storage.finish_run(
            run_id,
            creature_id=int(keeper["id"]),
            status="failed",
            raw_output_text=None,
            summary=None,
            severity="critical",
            message_text=None,
            error_text=error_text,
            next_run_at=None,
            metadata={
                "run_scope": RUN_SCOPE_CHAT,
                "conversation_id": int(conversation["id"]),
                "conversation_title": str(_row_value(conversation, "title") or KEEPER_CONVERSATION_TITLE),
                "sandbox_mode": "read-only",
                "prompt_kind": prompt_kind,
                "system_role": KEEPER_SYSTEM_ROLE,
                "owner_mode": DEFAULT_OWNER_MODE,
            },
            notes_markdown=None,
        )
        storage.create_message(
            int(keeper["id"]),
            conversation_id=int(conversation["id"]),
            role="system",
            body=error_text,
            run_id=run_id,
            metadata={"trigger_type": trigger_type, "status": "failed", "run_scope": RUN_SCOPE_CHAT},
        )
        raise


def _ensure_onboarding_briefing() -> dict[str, Any]:
    ecosystem_value = get_ecosystem()["value"]
    cached = _load_onboarding_briefing_cache()
    by_ecosystem = cached.get("by_ecosystem") if isinstance(cached.get("by_ecosystem"), dict) else {}
    ecosystem_cached = by_ecosystem.get(ecosystem_value) if isinstance(by_ecosystem.get(ecosystem_value), dict) else {}
    if (
        ecosystem_cached.get("ecosystem") == ecosystem_value
        and ecosystem_cached.get("observations")
        and ecosystem_cached.get("questions")
        and int(ecosystem_cached.get("version") or 0) == ONBOARDING_BRIEFING_VERSION
    ):
        return ecosystem_cached
    scan = _project_environment_scan()
    briefing = _fallback_onboarding_briefing(ecosystem_value, scan)
    _store_onboarding_briefing(ecosystem_value, briefing)
    return briefing


def _coerce_onboarding_chat_messages(raw_messages: Any) -> list[dict[str, Any]]:
    messages: list[dict[str, Any]] = []
    next_id = 1
    for item in raw_messages if isinstance(raw_messages, list) else []:
        if not isinstance(item, dict):
            continue
        role = str(item.get("role") or "").strip().lower()
        body = str(item.get("body") or "").strip()
        attachments = list(item.get("attachments") or []) if isinstance(item.get("attachments"), list) else []
        if role not in {"creature", "user"} or (not body and not attachments):
            continue
        try:
            message_id = max(next_id, int(item.get("id") or next_id))
        except (TypeError, ValueError):
            message_id = next_id
        messages.append(
            {
                "id": message_id,
                "role": role,
                "body": body,
                "typewriter_text": _markdown_plain_text(body) if role == "creature" else "",
                "created_at": str(item.get("created_at") or ""),
                "attachments": attachments,
                "metadata": dict(item.get("metadata") or {}) if isinstance(item.get("metadata"), Mapping) else {},
                "starter_ready": bool((item.get("metadata") or {}).get("starter_ready")) if isinstance(item.get("metadata"), Mapping) else bool(item.get("starter_ready")),
            }
        )
        next_id = message_id + 1
    return messages[-40:]


def _store_onboarding_chat_messages(messages: list[dict[str, Any]]) -> None:
    _store_meta_json(
        ONBOARDING_CHAT_KEY,
        {
            "messages": messages[-40:],
        },
    )


def _extract_summon_creature_signal(text: str) -> tuple[str, bool]:
    raw = str(text or "")
    ready = SUMMON_CREATURE_SIGNAL in raw
    cleaned = raw.replace(SUMMON_CREATURE_SIGNAL, "")
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned).strip()
    return cleaned, ready


def _onboarding_starter_ready_from_messages(messages: Sequence[Mapping[str, Any]]) -> bool:
    latest_relevant: Mapping[str, Any] | None = None
    for item in messages:
        latest_relevant = item
    if latest_relevant is None:
        return False
    if str(latest_relevant.get("role") or "").strip() != "creature":
        return False
    metadata = latest_relevant.get("metadata")
    if isinstance(metadata, Mapping):
        return bool(metadata.get("starter_ready"))
    return bool(latest_relevant.get("starter_ready"))


def _keeper_chat_context_mode() -> str:
    return "onboarding" if get_onboarding_phase() == "starter" else "keeper"


def _keeper_intro_message(*, mode: str) -> str:
    keeper_name = str(_ensure_keeper_creature().get("display_name") or ONBOARDING_KEEPER_NAME)
    if mode == "onboarding":
        scan = _project_environment_scan()
        ecosystem_value = get_ecosystem()["value"]
        lines = [
            random.choice(
                [
                    f"Hello, human. I am **{keeper_name}**.",
                    f"Ah. A human voice at last. I am **{keeper_name}**.",
                    f"Well now. Someone has called, and I am **{keeper_name}**.",
                ]
            ),
            random.choice(
                [
                    "You have called me into this place, and it is already carrying a shape of its own.",
                    "I have answered the call, and this place is not empty of intention.",
                    "I have arrived, and the air here already feels marked by purpose.",
                ]
            ),
            "",
            _keeper_ecosystem_invocation(ecosystem_value),
            "",
            *_creatureos_intro_scan_lines(scan),
            "",
            random.choice(
                [
                    "I know why I was called. You want help. You want the right creature, not a heap of generic ones.",
                    "You did not summon me here for ornament. You want help, and you want it embodied.",
                    "We are not here merely to admire the habitat. You want help, and you want a creature shaped to what matters.",
                ]
            ),
            "",
            random.choice(
                [
                    "Tell me what sort of creature you want, what you want it to notice, and what burden you want it to help carry.",
                    "Name the kind of help you desire, and tell me what you want the first creature to care about.",
                    "Speak plainly: what kind of creature do you want, and what do you want it to help you with?",
                ]
            ),
            "",
            random.choice(
                [
                    "Tell me a little about yourself, and what kind of help you have been wishing for.",
                    "Tell me what matters to you here, and what kind of creature would feel like a gift.",
                    "Tell me what you hope to make, protect, finish, or become, and what kind of creature should rise to meet it.",
                ]
            ),
            "",
            "Speak.",
        ]
        return "\n".join(lines).strip()

    return "\n\n".join(
        [
            f"Hello. I'm **{keeper_name}**.",
            "Tell me what kind of help you want to bring into the habitat next, and I’ll shape one creature around it.",
            "We can keep talking as long as you want before I summon anyone.",
        ]
    )


def _ensure_onboarding_chat(*, mode: str | None = None) -> dict[str, Any]:
    current_mode = mode or _keeper_chat_context_mode()
    keeper = _ensure_keeper_creature()
    conversation = _ensure_keeper_conversation(mode=current_mode)
    messages = _coerce_onboarding_chat_messages(
        [
            {
                "id": int(row["id"]),
                "role": str(row["role"] or ""),
                "body": str(row["body"] or ""),
                "created_at": str(row["created_at"] or ""),
                "attachments": _message_attachments_for_metadata(
                    _parse_json(str(_row_value(row, "metadata_json") or "")).get("attachments"),
                    message_id=int(row["id"]),
                ),
                "metadata": _parse_json(str(_row_value(row, "metadata_json") or "")),
            }
            for row in storage.list_messages(int(conversation["id"]), limit=80)
        ]
    )
    return {
        "keeper_name": str(keeper.get("display_name") or ONBOARDING_KEEPER_NAME),
        "creature_slug": str(keeper.get("slug") or KEEPER_SLUG),
        "conversation_id": int(conversation["id"]),
        "messages": messages,
        "thinking": _thinking_state(keeper, conversation),
    }


def prewarm_onboarding_assets(*, force: bool = False) -> None:
    _initialize_runtime()
    scan = _project_environment_scan()
    cache = _load_onboarding_briefing_cache()
    by_ecosystem = dict(cache.get("by_ecosystem") or {}) if isinstance(cache.get("by_ecosystem"), dict) else {}
    changed = False
    for ecosystem in ECOSYSTEMS:
        ecosystem_value = str(ecosystem["value"])
        existing = by_ecosystem.get(ecosystem_value) if isinstance(by_ecosystem.get(ecosystem_value), dict) else {}
        if (
            force
            or not existing.get("observations")
            or not existing.get("questions")
            or int(existing.get("version") or 0) != ONBOARDING_BRIEFING_VERSION
        ):
            by_ecosystem[ecosystem_value] = _fallback_onboarding_briefing(ecosystem_value, scan)
            changed = True
    if changed or force:
        _store_meta_json(ONBOARDING_BRIEFING_KEY, {"by_ecosystem": by_ecosystem})


def _keeper_recent_interaction_lines(creatures: list[dict[str, Any]], *, limit: int = 8) -> list[str]:
    signals: list[tuple[str, str]] = []
    for creature in creatures:
        creature_id = int(creature.get("id") or 0)
        if creature_id <= 0:
            continue
        for conversation in storage.list_conversations(creature_id)[:4]:
            recent_messages = storage.recent_messages(int(conversation["id"]), limit=4)
            user_messages = [
                row
                for row in recent_messages
                if str(row["role"] or "") == "user" and str(row["body"] or "").strip()
            ]
            if not user_messages:
                continue
            latest_user = user_messages[-1]
            body = _trim_for_prompt(" ".join(str(latest_user["body"] or "").strip().split()), limit=200)
            title = str(conversation["title"] or NEW_CHAT_TITLE).strip() or NEW_CHAT_TITLE
            created_at = str(latest_user["created_at"] or "")
            signals.append((created_at, f"- {creature['display_name']} / {title}: {body}"))
    signals.sort(key=lambda item: item[0], reverse=True)
    return [line for _, line in signals[:limit]]


def _keeper_habitat_snapshot(
    *,
    max_creatures: int = 12,
    max_recent_interactions: int = 8,
) -> str:
    ecosystem = get_ecosystem()
    creatures = [_row_to_dict(row) or {} for row in storage.list_creatures()]
    system_creatures = [creature for creature in creatures if _is_keeper_creature(creature)]
    operational_creatures = [creature for creature in creatures if not _is_keeper_creature(creature)]
    pending_total = sum(int(creature.get("pending_conversation_count") or 0) for creature in operational_creatures)
    error_total = sum(1 for creature in operational_creatures if str(creature.get("status") or "").strip().lower() == "error")
    lines = [
        f"Current location: {ecosystem['label']}",
        f"Owner reference: {get_owner_reference()}",
        (
            f"Habitat summary: {len(operational_creatures)} operational creature(s), "
            f"{pending_total} pending chat(s), {error_total} in error."
        ),
        "",
        "Current creatures:",
    ]
    if not operational_creatures:
        lines.append("- No creatures here yet.")
    else:
        for creature in operational_creatures[:max_creatures]:
            display_name = str(creature.get("display_name") or "Unnamed creature").strip() or "Unnamed creature"
            purpose = _trim_for_prompt(str(creature.get("purpose_summary") or creature.get("concern") or "none yet"), limit=170)
            status = str(creature.get("status") or "idle").strip() or "idle"
            details = [
                f"purpose: {purpose}",
                f"status: {status}",
                f"habits: {_creature_habit_summary(_creature_habits_state(creature))}",
            ]
            pending_count = int(creature.get("pending_conversation_count") or 0)
            if pending_count > 0:
                details.append(f"pending chats: {pending_count}")
            last_severity = str(creature.get("last_run_severity") or "").strip().lower()
            if last_severity and last_severity != "info":
                details.append(f"last severity: {last_severity}")
            lines.append(f"- {display_name} | " + " | ".join(details))
        if len(operational_creatures) > max_creatures:
            lines.append(f"- ... {len(operational_creatures) - max_creatures} more creature(s) not shown.")
    if system_creatures:
        lines.extend(["", "System creatures:"])
        for creature in system_creatures:
            lines.append(
                f"- {str(creature.get('display_name') or 'System creature').strip()} | purpose: {_trim_for_prompt(str(creature.get('purpose_summary') or creature.get('concern') or 'none yet'), limit=150)}"
            )
    lines.extend(["", "Recent human interaction:"])
    recent_interactions = _keeper_recent_interaction_lines(operational_creatures, limit=max_recent_interactions)
    lines.extend(recent_interactions or ["- No recent direct creature chats yet."])
    return "\n".join(lines).strip()


def _keeper_live_sql_snapshot(
    *,
    max_creatures: int = 16,
    max_requests: int = 10,
    max_recent_messages: int = 10,
) -> str:
    snapshot = storage.keeper_runtime_snapshot(
        keeper_system_role=KEEPER_SYSTEM_ROLE,
        max_creatures=max_creatures,
        max_requests=max_requests,
        max_recent_messages=max_recent_messages,
    )
    summary = dict(snapshot.get("summary") or {})
    ecosystem = get_ecosystem()
    operational_total = int(summary.get("operational_creatures") or 0)
    running_total = int(summary.get("running_creatures") or 0)
    error_total = int(summary.get("error_creatures") or 0)
    lines = [
        f"Current location: {ecosystem['label']}",
        f"Owner reference: {get_owner_reference()}",
        "Fresh SQL creature snapshot taken immediately before this run.",
        (
            f"Summary: {operational_total} operational creature(s), "
            f"{running_total} running, {error_total} in error."
        ),
        "",
        "Current creatures from SQLite:",
    ]
    creatures = list(snapshot.get("creatures") or [])
    if not creatures:
        lines.append("- No creatures here yet.")
    else:
        for raw_creature in creatures:
            creature = dict(raw_creature or {})
            display_name = str(creature.get("display_name") or "Unnamed creature").strip() or "Unnamed creature"
            purpose = _trim_for_prompt(str(creature.get("purpose_summary") or "").strip() or "none yet", limit=180)
            status = str(creature.get("status") or "idle").strip() or "idle"
            details = [
                f"purpose: {purpose}",
                f"status: {status}",
                f"habits: {_creature_habit_summary(_creature_habits_state(creature))}",
                f"conversations: {int(creature.get('conversation_count') or 0)}",
            ]
            pending_request_count = int(creature.get("pending_request_count") or 0)
            if pending_request_count > 0:
                details.append(f"wants to chat: {pending_request_count}")
            last_severity = str(creature.get("last_run_severity") or "").strip().lower()
            if last_severity and last_severity != "info":
                details.append(f"last severity: {last_severity}")
            last_summary = _trim_for_prompt(str(creature.get("last_run_summary") or "").strip(), limit=120)
            if last_summary:
                details.append(f"last run: {last_summary}")
            lines.append(f"- {display_name} | " + " | ".join(details))
    lines.extend(["", "Open chat requests waiting for human attention:"])
    requests = list(snapshot.get("requests") or [])
    if not requests:
        lines.append("- No creatures are currently asking to talk.")
    else:
        for request in requests:
            display_name = str(request.get("creature_display_name") or "Unnamed creature").strip() or "Unnamed creature"
            trigger = str(request.get("trigger_type") or "activity").strip() or "activity"
            severity = str(request.get("severity") or "").strip().lower()
            purpose = _trim_for_prompt(str(request.get("purpose_summary") or "").strip(), limit=110)
            preview = _trim_for_prompt(str(request.get("preview") or "").strip(), limit=140)
            details = [f"trigger: {trigger}"]
            if severity and severity != "info":
                details.append(f"severity: {severity}")
            if purpose:
                details.append(f"purpose: {purpose}")
            if preview:
                details.append(f"preview: {preview}")
            lines.append(f"- {display_name} | " + " | ".join(details))
    lines.extend(["", "Recent human messages across creatures:"])
    recent_messages = list(snapshot.get("recent_messages") or [])
    if not recent_messages:
        lines.append("- No recent direct creature chats yet.")
    else:
        for item in recent_messages:
            display_name = str(item.get("creature_display_name") or "Unnamed creature").strip() or "Unnamed creature"
            title = str(item.get("conversation_title") or NEW_CHAT_TITLE).strip() or NEW_CHAT_TITLE
            body = _trim_for_prompt(" ".join(str(item.get("body") or "").strip().split()), limit=180)
            lines.append(f"- {display_name} / {title}: {body}")
    return "\n".join(lines).strip()


def _ecosystem_conversation_digest(creature: Any, *, limit: int = 10) -> list[dict[str, str]]:
    creature_id = int(_row_value(creature, "id") or 0)
    digests: list[dict[str, str]] = []
    for other_row in storage.list_creatures():
        other = _row_to_dict(other_row) or {}
        other_id = int(other.get("id") or 0)
        if other_id <= 0 or other_id == creature_id or _is_keeper_creature(other):
            continue
        for conversation in storage.list_conversations(other_id)[:4]:
            messages = [_row_to_dict(row) or {} for row in storage.recent_messages(int(conversation["id"]), limit=4)]
            if not messages:
                continue
            latest_human = next(
                (
                    " ".join(str(message.get("body") or "").strip().split())
                    for message in reversed(messages)
                    if str(message.get("role") or "") == "user" and str(message.get("body") or "").strip()
                ),
                "",
            )
            latest_creature = next(
                (
                    " ".join(str(message.get("body") or "").strip().split())
                    for message in reversed(messages)
                    if str(message.get("role") or "") == "creature" and str(message.get("body") or "").strip()
                ),
                "",
            )
            if not latest_human and not latest_creature:
                continue
            digests.append(
                {
                    "created_at": str((messages[-1] or {}).get("created_at") or ""),
                    "creature": str(other.get("display_name") or "Unnamed creature").strip() or "Unnamed creature",
                    "conversation": str(_row_value(conversation, "title") or NEW_CHAT_TITLE).strip() or NEW_CHAT_TITLE,
                    "human": _trim_for_prompt(latest_human, limit=180),
                    "creature_reply": _trim_for_prompt(latest_creature, limit=180),
                }
            )
    digests.sort(key=lambda item: str(item.get("created_at") or ""), reverse=True)
    return digests[:limit]


def _platform_interaction_summary() -> str:
    lines = [_keeper_live_sql_snapshot(max_creatures=16, max_requests=10, max_recent_messages=16)]
    keeper_chat = _ensure_onboarding_chat(mode="keeper")
    transcript = _onboarding_chat_transcript(list(keeper_chat.get("messages") or []), limit=24)
    if transcript:
        lines.extend(["", "Keeper chat:", transcript])
    return "\n".join(lines).strip()


def _append_onboarding_chat_message(messages: list[dict[str, Any]], *, role: str, body: str) -> list[dict[str, Any]]:
    cleaned_body = str(body or "").strip()
    cleaned_role = str(role or "").strip().lower()
    if cleaned_role not in {"creature", "user"} or not cleaned_body:
        return messages
    next_id = max((int(item.get("id") or 0) for item in messages), default=0) + 1
    messages.append(
        {
            "id": next_id,
            "role": cleaned_role,
            "body": cleaned_body,
            "created_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        }
    )
    return messages[-40:]


def _onboarding_chat_transcript(messages: list[dict[str, Any]], *, limit: int = 12) -> str:
    lines: list[str] = []
    keeper_name = str(_ensure_keeper_creature().get("display_name") or ONBOARDING_KEEPER_NAME)
    for item in messages[-limit:]:
        speaker = keeper_name if str(item.get("role") or "") == "creature" else "Human"
        body = str(item.get("body") or "").strip()
        attachments = list(item.get("attachments") or []) if isinstance(item.get("attachments"), list) else []
        attachment_summary = ""
        if attachments:
            names = [str(entry.get("filename") or "").strip() for entry in attachments if str(entry.get("filename") or "").strip()]
            if names:
                attachment_summary = f" [Attachments: {', '.join(names[:4])}]"
            else:
                attachment_summary = " [Attachments included]"
        if body:
            lines.append(f"{speaker}: {body}{attachment_summary}")
        else:
            lines.append(f"{speaker}: {attachment_summary.strip() or '[Attachment-only message]'}")
    return "\n".join(lines).strip()


def _onboarding_chat_reply_prompt(*, scan: dict[str, Any], messages: list[dict[str, Any]], mode: str) -> str:
    keeper_name = str(_ensure_keeper_creature().get("display_name") or ONBOARDING_KEEPER_NAME)
    live_snapshot = _keeper_live_sql_snapshot(max_creatures=10, max_requests=8, max_recent_messages=6)
    transcript = _onboarding_chat_transcript(messages, limit=24 if mode != "onboarding" else 12)
    if mode == "onboarding":
        return "\n\n".join(
            [
                f"You are {keeper_name}, the built-in keeper for CreatureOS.",
                "Continue the live onboarding chat with the human.",
                "Your goals are to understand who they are, what they want CreatureOS to do, how hands-on they want creatures to be, and what would make the system genuinely useful.",
                "Use the ecosystem metaphor lightly. Stay practical, calm, and a little mythical.",
                "Write in short, spacious paragraphs. Leave breathing room between thoughts.",
                "Never use bullet lists or numbered lists.",
                "Do not sound like an intake form or a product checklist.",
                "Do not speak as though you already know the human well unless the chat clearly shows an ongoing relationship.",
                "Keep work descriptions plain and professional. Do not use animal, habitat, or mascot language to describe the actual work.",
                "Think in terms of what kind of teammate would help most next, not an internal label you need to say out loud.",
                "Ask follow-up questions when they would genuinely sharpen the shape of the first creature, but do not interrogate. One or two focused questions is plenty.",
                "Use the fresh SQL creature snapshot to notice any creatures already here, what they are for, and whether any creature is already asking to talk.",
                "Do not slip into internal category language unless the human explicitly asks how you distinguish creature shapes.",
                "Do not mention any summon button unless you are satisfied that the human has given enough purpose and shape for a first creature.",
                "Only when you are satisfied that the first creature's purpose is clear, append the exact token [[SUMMON_CREATURE]] on its own line at the very end of your reply.",
                "If the purpose is still blurry, ask the next most useful question instead and do not emit the token.",
                "Do not draft the creature yet.",
                "When you later shape a creature name, let it follow the creature's own ecosystem, not the currently selected habitat by itself.",
                f"Environment scan:\n{json.dumps(scan, indent=2)}",
                f"Fresh SQL creature snapshot:\n{live_snapshot}",
                f"Recent chat:\n{transcript}",
                "Reply in plain markdown only. No JSON.",
            ]
        )
    return "\n\n".join(
        [
            f"You are {keeper_name}, the built-in keeper for CreatureOS.",
            "Continue the live keeper chat with the human.",
            "Start from the creature the human seems to need next.",
            "Default to shaping one creature at a time.",
            "Do not upsell or pressure for extra creatures.",
            "Write in short, spacious paragraphs. Never use bullet lists or numbered lists.",
            "Keep the tone calm, wise, and lightly mythical without becoming flowery or vague.",
            "Keep work descriptions plain and professional. When you later shape a creature name, let it follow the creature's own ecosystem, not the currently selected habitat by itself.",
            "Use the fresh SQL creature snapshot to understand which creatures already exist, what they are for, and where there is still real room for another creature.",
            "Avoid proposing creatures whose responsibilities are already covered unless the human explicitly wants redundancy, a split, or a replacement.",
            "Do not slip into internal category language unless the human explicitly asks how you distinguish creature shapes.",
            "Ask one or two focused follow-up questions only when they would materially improve the shape of the creature.",
            "Only when you are satisfied that the next creature's purpose is clear, append the exact token [[SUMMON_CREATURE]] on its own line at the very end of your reply.",
            "If the purpose is still blurry, ask the next most useful question instead and do not emit the token.",
            "Do not draft the creature yet.",
            f"Environment scan:\n{json.dumps(scan, indent=2)}",
            f"Fresh SQL creature snapshot:\n{live_snapshot}",
            f"Recent chat:\n{transcript}",
            "Reply in plain markdown only. No JSON.",
        ]
    )


def _fallback_onboarding_reply(*, scan: dict[str, Any], messages: list[dict[str, Any]], mode: str) -> str:
    latest_user = next((str(item.get("body") or "").strip() for item in reversed(messages) if str(item.get("role") or "") == "user"), "")
    observations = _environment_observations(scan)
    if mode != "onboarding":
        followup = "Tell me what kind of creature you want next, what you want it to handle, and how quiet or proactive you want it to be."
        if any(token in latest_user.lower() for token in ("several", "multiple", "few", "crew", "team")):
            followup = "That sounds like work that may need to be split up over time. Let’s start with the first creature you want to bring in, and tell me the responsibility you want it to own."
        elif any(token in latest_user.lower() for token in ("just one", "one creature", "single creature", "only one")):
            followup = "Understood. We’ll keep this to one creature. Tell me the core responsibility you want it to own and what should qualify as worth your attention."
        elif any(token in latest_user.lower() for token in ("not sure", "unsure", "curious", "exploring")):
            followup = "That is fine. Tell me the first kind of help you wish CreatureOS had, even if it is rough, and I’ll shape the next creature from there."
        return "\n\n".join(
            [
                "I can work with that.",
                observations[0] if observations else "I can use the traces on this machine plus this conversation to shape the right creature.",
                followup,
            ]
        ).strip()
    followup = "Tell me what would make CreatureOS immediately useful for you, and I’ll keep shaping the first creature until it has a clear purpose."
    if any(token in latest_user.lower() for token in ("curious", "explore", "just looking", "not sure", "unsure")):
        followup = "That works. Tell me a little about the kind of work you do and what you'd be most interested in exploring first."
    elif any(token in latest_user.lower() for token in ("security", "server", "deploy", "ops", "monitor", "watch")):
        followup = "That gives me something concrete to optimize for. Tell me whether you want creatures to stay quiet unless something is urgent, or whether you'd also like useful drift and improvement suggestions."
    elif any(token in latest_user.lower() for token in ("design", "art", "creative", "illustration", "video", "music")):
        followup = "I can work with that. Tell me whether you want CreatureOS mostly to protect delivery and organization, or also to help with review, reminders, and research."
    return "\n\n".join(
        [
            "I’m getting the shape of it.",
            observations[0] if observations else "I can use both the traces on this machine and your answers to shape a solid first creature.",
            followup,
        ]
    ).strip()


def send_onboarding_chat_message(
    body: str,
    *,
    attachments: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    _initialize_runtime()
    cleaned_body = str(body or "").strip()
    prepared_attachments = attachments or []
    if not cleaned_body and not prepared_attachments:
        raise ValueError("Tell me a little about yourself or what you want from CreatureOS.")
    phase = get_onboarding_phase()
    if phase not in {"starter", "complete"}:
        raise ValueError("Ecosystem selection must be completed before chatting with The Keeper.")

    mode = "onboarding" if phase == "starter" else "keeper"
    keeper = _ensure_keeper_creature()
    conversation = _ensure_keeper_conversation(mode=mode)
    message_row = storage.create_message(int(keeper["id"]), conversation_id=int(conversation["id"]), role="user", body=cleaned_body)
    message_id = int(message_row["id"])
    stored_attachments = _store_message_attachments(message_id, prepared_attachments)
    if stored_attachments:
        storage.update_message_metadata(message_id, {"attachments": stored_attachments})
    messages = [
        {
            "id": int(row["id"]),
            "role": str(row["role"] or ""),
            "body": str(row["body"] or ""),
            "created_at": str(row["created_at"] or ""),
            "attachments": _message_attachments_for_metadata(
                _parse_json(str(_row_value(row, "metadata_json") or "")).get("attachments"),
                message_id=int(row["id"]),
            ),
        }
        for row in storage.list_messages(int(conversation["id"]), limit=80)
    ]
    scan = _project_environment_scan()
    _store_onboarding_chat_feed(status="running", lines=[], run_id=0, last_event_id=0, error="")
    if _codex_access_waiting() and not poll_codex_access_recovery(force=False):
        waiting_message = _codex_waiting_message(kind=_load_codex_access_state_raw()["reason_kind"])
        _append_codex_waiting_notice(int(keeper["id"]), conversation_id=int(conversation["id"]))
        updated_messages = storage.list_messages(int(conversation["id"]), limit=80)
        _store_onboarding_chat_feed(status="completed", lines=[waiting_message], run_id=0, last_event_id=0, error="")
        return {
            "assistant_body": waiting_message,
            "message_count": len(updated_messages),
            "starter_ready": False,
            "status": "waiting",
        }
    try:
        reply, _, run_id = _run_keeper_conversation_prompt(
            keeper=keeper,
            conversation=conversation,
            prompt_text=_onboarding_chat_reply_prompt(scan=scan, messages=messages, mode=mode),
            trigger_type="keeper_chat",
            prompt_kind="Live keeper chat",
            on_event=_append_onboarding_chat_feed_event,
        )
        if not reply:
            _append_onboarding_chat_feed_event({"type": "status", "phase": "fallback-reply"})
            reply = _fallback_onboarding_reply(scan=scan, messages=messages, mode=mode)
        reply, starter_ready = _extract_summon_creature_signal(reply)
        storage.create_message(
            int(keeper["id"]),
            conversation_id=int(conversation["id"]),
            role="creature",
            body=reply,
            run_id=run_id,
            metadata={
                "trigger_type": "keeper_chat",
                "run_scope": RUN_SCOPE_CHAT,
                "sandbox_mode": "read-only",
                "system_role": KEEPER_SYSTEM_ROLE,
                "starter_ready": starter_ready,
            },
        )
        updated_messages = storage.list_messages(int(conversation["id"]), limit=80)
        _store_onboarding_chat_feed(
            status="completed",
            lines=list(_load_onboarding_chat_feed().get("lines") or []),
            error="",
        )
        return {
            "assistant_body": reply,
            "message_count": len(updated_messages),
            "starter_ready": starter_ready,
        }
    except Exception as exc:
        if _codex_access_waiting():
            waiting_message = _codex_waiting_message(kind=_load_codex_access_state_raw()["reason_kind"])
            _append_codex_waiting_notice(int(keeper["id"]), conversation_id=int(conversation["id"]))
            updated_messages = storage.list_messages(int(conversation["id"]), limit=80)
            _store_onboarding_chat_feed(status="completed", lines=[waiting_message], run_id=0, last_event_id=0, error="")
            return {
                "assistant_body": waiting_message,
                "message_count": len(updated_messages),
                "starter_ready": False,
                "status": "waiting",
            }
        _append_onboarding_chat_feed_event({"type": "error", "message": str(exc)})
        _store_onboarding_chat_feed(
            status="failed",
            lines=list(_load_onboarding_chat_feed().get("lines") or []),
            error=str(exc),
        )
        raise


def _onboarding_host_creature_brief() -> str:
    current_keeper = next((_row_to_dict(row) or {} for row in storage.list_creatures() if _is_keeper_creature(row)), {})
    host_name = str(current_keeper.get("display_name") or "").strip() or _keeper_name()
    return (
        f'Create or refresh the permanent CreatureOS keeper named "{host_name}". '
        "It should explain how CreatureOS works, help the human decide when another creature is actually warranted, "
        "and help the human connect securely from phones, tablets, laptops, and other devices. It should recommend "
        "Tailscale first, explain safer remote access patterns, and only mention direct port forwarding as an "
        "explicit opt-in with clear tradeoffs. Keep the role practical, welcoming, and consistent across ecosystems "
        "without making the work description cutesy."
    )


def _is_onboarding_host_brief(brief: str) -> bool:
    cleaned = " ".join(str(brief or "").strip().split()).lower()
    return (
        "permanent creatureos keeper" in cleaned
        or "standing keeper of creatureos" in cleaned
    )


def _host_role_descriptor() -> str:
    return ""


def _host_role_summary() -> str:
    return (
        "Explains how CreatureOS works, helps decide when another creature is actually needed, and guides secure access from phones, tablets, laptops, and other devices."
    )


def _onboarding_summoning_prompt(*, scan: dict[str, Any], messages: list[dict[str, Any]], mode: str) -> str:
    keeper_name = str(_ensure_keeper_creature().get("display_name") or ONBOARDING_KEEPER_NAME)
    live_snapshot = _keeper_live_sql_snapshot(max_creatures=12, max_requests=10, max_recent_messages=8)
    transcript = _onboarding_chat_transcript(messages, limit=40)
    if mode == "onboarding":
        return "\n\n".join(
            [
                f"You are {keeper_name}, finishing CreatureOS onboarding.",
                "Turn the conversation into one thoughtfully shaped first creature.",
                "You already exist as the standing keeper of the habitat. Do not output yourself.",
                "Return JSON only with this shape:",
                json.dumps(
                    {
                        "summary": "One short paragraph.",
                        "creature": {
                            "display_name": "",
                            "purpose_summary": "Reads chapters, helps with story structure, and surfaces sharp revisions without sounding like a generic editor.",
                        "origin_context": "You're drafting a modern King Arthur novel and want a creature that can stay close to the manuscript as it grows.",
                        "opening_question": "Want me to start with structure, scene work, or chapter feedback?",
                        "brief": "A richer internal brief for what this creature should care about and how it should help.",
                        "purpose_markdown": "# Mothwake purpose\\n\\n## Why you exist\\n- ...",
                        "why": "Why this is the right first creature right now.",
                    },
                    },
                    indent=2,
                ),
                "Rules:",
                "- Draft exactly one creature.",
                "- The creature should feel purpose-built for this human and this habitat, not like a catalog entry with a new coat of paint.",
                "- display_name is optional. Leave it blank unless the human clearly wants a specific name.",
                "- purpose_summary is required. Keep it plain, human, and specific.",
                "- origin_context is required. Keep it specific to the human's real situation in second-person language, not lane jargon.",
                "- origin_context should explain why this creature exists in this habitat right now.",
                "- brief is required. It is the internal handoff for what this creature should care about and how it should behave.",
                "- purpose_markdown is required. Write the creature's actual purpose in markdown.",
                "- opening_question is optional. Use it when there is an obvious first question the creature should ask in its introduction.",
                "- Keep the purpose anchored in the human's actual context. This creature should understand why it exists before it wakes up.",
                "- Use the fresh SQL creature snapshot to avoid proposing work that is already covered.",
                "- Let the creature's name follow its own ecosystem, not the currently selected habitat by itself.",
                f"Environment scan:\n{json.dumps(scan, indent=2)}",
                f"Fresh SQL creature snapshot:\n{live_snapshot}",
                f"Full onboarding chat:\n{transcript}",
            ]
        )
    return "\n\n".join(
        [
            f"You are {keeper_name}, summoning one new creature into CreatureOS.",
            "Turn the conversation into one creature the user can summon right now.",
            "Return JSON only with this shape:",
            json.dumps(
                {
                    "summary": "One short paragraph.",
                    "creature": {
                        "display_name": "",
                        "purpose_summary": "Reads chapters, helps with story structure, and surfaces sharp revisions without sounding like a generic editor.",
                        "origin_context": "You're drafting a modern King Arthur novel and want a creature that can stay close to the manuscript as it grows.",
                        "opening_question": "Want me to start with structure, scene work, or chapter feedback?",
                        "brief": "A richer internal brief for what this creature should care about and how it should help.",
                        "purpose_markdown": "# Mothwake purpose\\n\\n## Why you exist\\n- ...",
                        "why": "Why this is the right creature to summon next.",
                    },
                },
                indent=2,
            ),
            "Rules:",
            "- Draft exactly one creature.",
            "- Do not upsell or pad this into extra creatures.",
            "- display_name is optional. Leave it blank unless the human clearly wants a specific name.",
            "- purpose_summary is required. Keep it plain, human, and specific.",
            "- origin_context is required. Keep it specific to the human's real situation in second-person language, not lane jargon.",
            "- origin_context should explain why this creature exists in this habitat right now.",
            "- brief is required. It is the internal handoff for what this creature should care about and how it should help.",
            "- purpose_markdown is required. Write the creature's actual purpose in markdown.",
            "- opening_question is optional. Use it when there is an obvious first question the creature should ask in its introduction.",
            "- Keep the purpose anchored in the human's actual context. This creature should understand why it exists before it wakes up.",
            "- Use the fresh SQL creature snapshot to avoid proposing work that is already covered unless the human explicitly wants a split, backup, or replacement.",
            "- Let the creature's name follow its own ecosystem, not the currently selected habitat by itself.",
            f"Environment scan:\n{json.dumps(scan, indent=2)}",
            f"Fresh SQL creature snapshot:\n{live_snapshot}",
            f"Keeper chat:\n{transcript}",
        ]
    )


def _keeper_existing_responsibility_texts() -> list[str]:
    texts: list[str] = []
    for row in storage.list_creatures():
        creature = _row_to_dict(row) or {}
        if _is_keeper_creature(creature):
            continue
        parts = [
            str(creature.get("display_name") or "").strip(),
            str(creature.get("purpose_summary") or creature.get("concern") or "").strip(),
        ]
        combined = " ".join(part for part in parts if part)
        if combined:
            texts.append(combined)
    return texts


def _keeper_responsibility_tokens(text: str) -> set[str]:
    keep_short = {"api", "app", "db", "ops", "qa", "sql", "ui", "ux"}
    stopwords = {
        "about",
        "again",
        "alerts",
        "already",
        "another",
        "around",
        "attention",
        "because",
        "bring",
        "build",
        "checks",
        "clear",
        "creature",
        "creatures",
        "direct",
        "focus",
        "helps",
        "human",
        "improve",
        "keeps",
        "maintain",
        "make",
        "normal",
        "operator",
        "plain",
        "quiet",
        "review",
        "reviews",
        "role",
        "roles",
        "surface",
        "surfaces",
        "system",
        "habits",
        "useful",
        "uses",
        "using",
        "watch",
        "watches",
        "worth",
        "would",
    }
    tokens: set[str] = set()
    for token in re.findall(r"[a-z0-9]+", text.lower()):
        if token in stopwords:
            continue
        if len(token) < 4 and token not in keep_short:
            continue
        tokens.add(token)
    return tokens


def _keeper_brief_conflicts_with_context(brief: str, context_texts: list[str]) -> bool:
    candidate_tokens = _keeper_responsibility_tokens(brief)
    if len(candidate_tokens) < 2:
        return False
    for text in context_texts:
        context_tokens = _keeper_responsibility_tokens(text)
        if not context_tokens:
            continue
        overlap = candidate_tokens.intersection(context_tokens)
        if len(overlap) >= 3:
            return True
        if len(candidate_tokens) >= 5 and len(overlap) >= 2 and (len(overlap) / len(candidate_tokens)) >= 0.34:
            return True
    return False


def _coerce_onboarding_summoning_briefs(
    payload: dict[str, Any],
    *,
    scan: dict[str, Any],
    transcript: str,
    mode: str,
) -> tuple[str, list[dict[str, Any]]]:
    summary_default = (
        "CreatureOS turned the onboarding chat into a first creature draft."
        if mode == "onboarding"
        else "The Keeper turned the chat into a creature draft."
    )
    summary = _ensure_sentence(str(payload.get("summary") or summary_default))
    _ = scan
    raw_candidate: dict[str, Any] | None = None
    if isinstance(payload.get("creature"), dict):
        raw_candidate = dict(payload.get("creature") or {})
    elif isinstance(payload.get("creatures"), list):
        raw_candidate = next((dict(item) for item in payload.get("creatures") or [] if isinstance(item, dict)), None)
    if not raw_candidate:
        return summary, []
    brief = " ".join(str(raw_candidate.get("brief") or "").strip().split())
    display_name = _normalize_generated_name(raw_candidate.get("display_name") or raw_candidate.get("name"))
    purpose_summary = _normalize_summoning_role_summary(
        raw_candidate.get("purpose_summary") or raw_candidate.get("role_summary"),
        brief=brief or transcript,
    )
    if not brief:
        brief = " ".join(
            part
            for part in (
                purpose_summary,
                str(raw_candidate.get("origin_context") or "").strip(),
                str(raw_candidate.get("why") or "").strip(),
            )
            if part
        ).strip()
    if not brief:
        return summary, []
    if _keeper_brief_conflicts_with_context(brief, _keeper_existing_responsibility_texts()):
        return summary, []
    candidate = {
        "display_name": display_name,
        "purpose_summary": purpose_summary,
        "purpose_markdown": _normalize_purpose_markdown(raw_candidate.get("purpose_markdown")),
        "brief": brief,
        "origin_context": _resolve_origin_context_text(
            raw_candidate.get("origin_context"),
            brief=brief,
            transcript=transcript,
        ),
        "opening_question": _normalize_intro_question_text(raw_candidate.get("opening_question")),
        "why": _ensure_sentence(str(raw_candidate.get("why") or "The Keeper shaped this creature for the current need.")),
    }
    return summary, [candidate]


def generate_onboarding_starter_creatures() -> dict[str, Any]:
    _initialize_runtime()
    phase = get_onboarding_phase()
    if phase not in {"starter", "complete"}:
        raise ValueError("Ecosystem selection must be completed before summoning creatures.")
    mode = "onboarding" if phase == "starter" else "keeper"

    chat_state = _ensure_onboarding_chat(mode=mode)
    keeper = _ensure_keeper_creature()
    conversation = _ensure_keeper_conversation(mode=mode)
    messages = list(chat_state["messages"])
    if not _onboarding_starter_ready_from_messages(messages):
        raise ValueError("The Keeper has not granted a creature yet. Keep talking until its purpose is clear.")
    scan = _project_environment_scan()
    _store_onboarding_starter_feed(status="running", lines=[], run_id=0, last_event_id=0, error="")
    transcript = _onboarding_chat_transcript(messages, limit=40)
    created_slugs: list[str] = []
    if _codex_access_waiting() and not poll_codex_access_recovery(force=False):
        waiting_message = _codex_waiting_message(kind=_load_codex_access_state_raw()["reason_kind"])
        _append_codex_waiting_notice(int(keeper["id"]), conversation_id=int(conversation["id"]))
        _store_onboarding_starter_feed(status="failed", lines=[waiting_message], run_id=0, last_event_id=0, error=waiting_message)
        return {
            "count": 0,
            "summary": "",
            "created": [],
            "mode": mode,
            "keeper_slug": str(keeper.get("slug") or KEEPER_SLUG),
            "welcome_conversation_id": int(conversation["id"]),
            "waiting": True,
        }
    try:
        raw_text, _, run_id = _run_keeper_conversation_prompt(
            keeper=keeper,
            conversation=conversation,
            prompt_text=_onboarding_summoning_prompt(scan=scan, messages=messages, mode=mode),
            trigger_type="summoning_planning",
            prompt_kind="Creature summoning planning",
            on_event=_append_onboarding_starter_feed_event,
        )
        payload = _extract_json_object(raw_text) or {}
        summary, additional_items = _coerce_onboarding_summoning_briefs(
            payload,
            scan=scan,
            transcript=transcript,
            mode=mode,
        )

        created: list[dict[str, Any]] = []
        queued_bootstraps: list[tuple[dict[str, Any], dict[str, Any]]] = []
        all_items = additional_items[:1]
        for item in all_items:
            brief = str(item.get("brief") or "").strip()
            if not brief:
                continue
            preview = _preview_for_onboarding_suggestion(
                brief=brief,
                purpose_summary=str(item.get("purpose_summary") or ""),
                purpose_markdown=str(item.get("purpose_markdown") or ""),
                origin_context=str(item.get("origin_context") or ""),
                opening_question=str(item.get("opening_question") or ""),
            )
            result = confirm_summoning(
                serialize_summoning_preview(preview),
                bootstrap_async=True,
                bootstrap=False,
            )
            created.append(result)
            if result.get("kind") == "single":
                creature_slug = str(result.get("creature_slug") or "").strip()
                if creature_slug:
                    created_slugs.append(creature_slug)
                creature = storage.get_creature_by_slug(creature_slug)
                if creature is None:
                    continue
                preview_data = dict(preview.get("preview") or {})
                queued_bootstraps.append(
                    (
                        creature,
                        {
                            "naming_alternates": list(preview_data.get("alternates") or []),
                            "used_explicit_name": bool(preview_data.get("used_explicit_name")),
                            "descriptor": str(preview_data.get("descriptor") or ""),
                            "temperament": _normalize_temperament(
                                str(preview_data.get("temperament") or DEFAULT_TEMPERAMENT)
                            ),
                            "origin_context": str(preview_data.get("origin_context") or ""),
                            "opening_question": str(preview_data.get("opening_question") or ""),
                        },
                    )
                )
        for creature, intro_context in queued_bootstraps:
            _queue_creature_bootstrap(
                creature,
                focus_hint="Start with the files most relevant to the stated concern.",
                intro_context=intro_context,
            )
        _store_onboarding_starter_feed(
            status="completed",
            lines=list(_load_onboarding_starter_feed().get("lines") or []),
            error="",
        )
        created_names = [str(storage.get_creature_by_slug(slug)["display_name"]) for slug in created_slugs if storage.get_creature_by_slug(slug) is not None]
        summary_line = summary or (
            "I turned our chat into a first creature draft."
            if mode != "onboarding"
            else "I turned the onboarding chat into a first creature draft."
        )
        if created_names:
            summon_line = f"I summoned **{created_names[0]}** into the habitat."
            body = f"{summary_line}\n\n{summon_line} I’ll let them wake up and introduce themselves when they are ready."
        else:
            body = (
                f"{summary_line}\n\n"
                "I didn’t summon anyone new yet. I’m still here as The Keeper whenever you want to keep talking or try again."
                if mode == "onboarding"
                else summary_line
            )
        storage.create_message(
            int(keeper["id"]),
            conversation_id=int(conversation["id"]),
            role="creature",
            body=body,
            run_id=run_id,
            metadata={
                "trigger_type": "summoning_planning",
                "run_scope": RUN_SCOPE_CHAT,
                "sandbox_mode": "read-only",
                "system_role": KEEPER_SYSTEM_ROLE,
            },
        )
        if mode == "onboarding":
            complete_onboarding()
        welcome_conversation = storage.find_conversation_by_title(int(keeper["id"]), WELCOME_CONVERSATION_TITLE)
        return {
            "count": len(created),
            "summary": summary,
            "created": created,
            "mode": mode,
            "keeper_slug": str(keeper.get("slug") or KEEPER_SLUG),
            "welcome_conversation_id": (
                int(welcome_conversation["id"])
                if welcome_conversation is not None
                else None
            ),
        }
    except Exception as exc:
        for slug in reversed(created_slugs):
            try:
                delete_creature(slug)
            except Exception:
                continue
        if _codex_access_waiting():
            waiting_message = _codex_waiting_message(kind=_load_codex_access_state_raw()["reason_kind"])
            _append_codex_waiting_notice(int(keeper["id"]), conversation_id=int(conversation["id"]))
            _store_onboarding_starter_feed(status="failed", lines=[waiting_message], run_id=0, last_event_id=0, error=waiting_message)
            return {
                "count": 0,
                "summary": "",
                "created": [],
                "mode": mode,
                "keeper_slug": str(keeper.get("slug") or KEEPER_SLUG),
                "welcome_conversation_id": int(conversation["id"]),
                "waiting": True,
            }
        _append_onboarding_starter_feed_event({"type": "error", "message": str(exc)})
        _store_onboarding_starter_feed(
            status="failed",
            lines=list(_load_onboarding_starter_feed().get("lines") or []),
            error=str(exc),
        )
        raise


def _compose_summoning_result_body(
    *,
    summary: str,
    created_entries: list[dict[str, Any]],
    mode: str,
) -> str:
    summary_line = summary or (
        "I turned our chat into a creature draft."
        if mode != "onboarding"
        else "I turned the onboarding chat into a first creature draft."
    )
    if not created_entries:
        if mode == "onboarding":
            return (
                f"{summary_line}\n\n"
                "I didn’t summon anyone new yet. I’m still here as The Keeper whenever you want to keep talking or try again."
            )
        return summary_line
    entry = dict(created_entries[0])
    display_name = str(entry.get("display_name") or "A creature").strip() or "A creature"
    focus = _ensure_sentence(str(entry.get("purpose_summary") or entry.get("brief") or ""))
    why = _ensure_sentence(str(entry.get("why") or "It fits the current habitat and what you asked for."))
    lines = [
        summary_line,
        "",
        f"I summoned **{display_name}** into the habitat.",
        focus,
        why,
        "",
        "They’ll take a few minutes to wake up and introduce themselves.",
    ]
    return "\n".join(lines).strip()


def _keeper_summon_suggestions_from_messages(messages: list[dict[str, Any]]) -> dict[str, Any]:
    for item in reversed(messages):
        if str(item.get("role") or "").strip().lower() != "creature":
            continue
        body = str(item.get("body") or "").strip()
        if not body:
            continue
        lowered_body = body.casefold()
        if "here’s what i summoned" in lowered_body or "here's what i summoned" in lowered_body:
            continue
        parsed_items: list[dict[str, str]] = []
        for raw_line in body.splitlines():
            line = " ".join(str(raw_line or "").strip().split())
            if not line:
                continue
            match = re.match(
                r"^(?:[-*]|\d+\.)\s+(?:\*\*)?(?P<name>[^*:\-][^:\-]{1,80}?)(?:\*\*)?\s*[-:–—]\s*(?P<detail>.+)$",
                line,
            )
            if not match:
                continue
            name = _normalize_generated_name(re.sub(r"[*_`]+", "", str(match.group("name") or "")))
            detail = _ensure_sentence(str(match.group("detail") or ""))
            if not name or not detail:
                continue
            parsed_items.append(
                {
                    "display_name": name,
                    "brief": detail,
                    "why": "This was part of the Keeper's plan we already discussed.",
                }
            )
            if len(parsed_items) >= 10:
                break
        if not parsed_items:
            continue
        intro_lines = [
            " ".join(str(line or "").strip().split())
            for line in body.splitlines()
            if str(line or "").strip()
            and not re.match(r"^(?:[-*]|\d+\.)\s+", str(line or "").strip())
        ]
        summary = (
            " ".join(intro_lines[:2]).strip()
            or "I turned the creature shape we just discussed into a real creature."
        )
        normalized_origin_context = _resolve_origin_context_text("", brief="", transcript=summary)
        for item in parsed_items:
            if not str(item.get("origin_context") or "").strip():
                item["origin_context"] = normalized_origin_context
        return {
            "summary": summary,
            "items": parsed_items,
        }
    return {"summary": "", "items": []}


def summon_creature_from_keeper(
    slug: str,
    conversation_id: int | None,
) -> dict[str, Any]:
    _initialize_runtime()
    slug = str(canonical_creature_slug(slug))
    creature = storage.get_creature_by_slug(slug)
    if creature is None:
        raise KeyError(f"Unknown creature slug: {slug}")
    if not _is_keeper_creature(creature):
        raise ValueError("Only the Keeper can summon creatures from here.")
    if conversation_id is None:
        conversation = storage.create_conversation(
            int(creature["id"]),
            title=KEEPER_SUMMON_CHAT_TITLE,
            owner_mode=DEFAULT_OWNER_MODE,
        )
        conversation_id = int(conversation["id"])
    else:
        conversation = storage.get_conversation_for_creature(int(creature["id"]), conversation_id)
        if conversation is None:
            raise KeyError(f"Conversation {conversation_id} does not belong to creature {slug}")
    if str(_row_value(conversation, "title") or "") == NEW_CHAT_TITLE:
        storage.rename_conversation(conversation_id, KEEPER_SUMMON_CHAT_TITLE)
    storage.create_message(
        int(creature["id"]),
        conversation_id=conversation_id,
        role="user",
        body="Summon Creature",
    )
    if _codex_access_waiting() and not poll_codex_access_recovery(force=False):
        waiting_message = _codex_waiting_message(kind=_load_codex_access_state_raw()["reason_kind"])
        _append_codex_waiting_notice(int(creature["id"]), conversation_id=conversation_id)
        return {
            "status": "waiting",
            "creature_slug": slug,
            "conversation_id": conversation_id,
            "waiting_message": waiting_message,
            "created_creatures": [],
        }

    messages = [
        {
            "id": int(row["id"]),
            "role": str(row["role"] or ""),
            "body": str(row["body"] or ""),
            "created_at": str(row["created_at"] or ""),
        }
        for row in storage.list_messages(int(conversation_id), limit=80)
    ]
    scan = _project_environment_scan()
    transcript = _onboarding_chat_transcript(messages, limit=40)
    planned_summoning = _keeper_summon_suggestions_from_messages(messages)
    created_slugs: list[str] = []
    created_entries: list[dict[str, Any]] = []
    try:
        run_id: int | None = None
        if list(planned_summoning.get("items") or []):
            summary = str(planned_summoning.get("summary") or "").strip()
            additional_items = list(planned_summoning.get("items") or [])[:1]
        else:
            raw_text, _, run_id = _run_keeper_conversation_prompt(
                keeper=creature,
                conversation=conversation,
                prompt_text=_onboarding_summoning_prompt(scan=scan, messages=messages, mode="keeper"),
                trigger_type="summoning_planning",
                prompt_kind="Creature summoning planning",
            )
            payload = _extract_json_object(raw_text) or {}
            summary, additional_items = _coerce_onboarding_summoning_briefs(
                payload,
                scan=scan,
                transcript=transcript,
                mode="keeper",
            )

        queued_bootstraps: list[tuple[dict[str, Any], dict[str, Any]]] = []
        for item in additional_items[:1]:
            brief = str(item.get("brief") or "").strip()
            if not brief:
                continue
            preview = _preview_for_onboarding_suggestion(
                brief=brief,
                purpose_summary=str(item.get("purpose_summary") or ""),
                purpose_markdown=str(item.get("purpose_markdown") or ""),
                origin_context=str(item.get("origin_context") or ""),
                opening_question=str(item.get("opening_question") or ""),
            )
            result = confirm_summoning(
                serialize_summoning_preview(preview),
                bootstrap_async=True,
                bootstrap=False,
            )
            if result.get("kind") != "single":
                continue
            creature_slug = str(result.get("creature_slug") or "").strip()
            if not creature_slug:
                continue
            created_slugs.append(creature_slug)
            created_creature = storage.get_creature_by_slug(creature_slug)
            if created_creature is None:
                continue
            preview_data = dict(preview.get("preview") or {})
            queued_bootstraps.append(
                (
                    created_creature,
                    {
                        "naming_alternates": list(preview_data.get("alternates") or []),
                        "used_explicit_name": bool(preview_data.get("used_explicit_name")),
                        "descriptor": str(preview_data.get("descriptor") or ""),
                        "temperament": _normalize_temperament(
                            str(preview_data.get("temperament") or DEFAULT_TEMPERAMENT)
                        ),
                        "origin_context": str(preview_data.get("origin_context") or ""),
                        "opening_question": str(preview_data.get("opening_question") or ""),
                    },
                )
            )
            created_entries.append(
                {
                    "slug": creature_slug,
                    "display_name": str(_row_value(created_creature, "display_name") or ""),
                    "purpose_summary": str(_row_value(created_creature, "purpose_summary") or _row_value(created_creature, "concern") or brief),
                    "why": str(item.get("why") or ""),
                    "brief": brief,
                }
            )
        for created_creature, intro_context in queued_bootstraps:
            _queue_creature_bootstrap(
                created_creature,
                focus_hint="Start with the files most relevant to the stated concern.",
                intro_context=intro_context,
            )
        body = _compose_summoning_result_body(
            summary=summary,
            created_entries=created_entries,
            mode="keeper",
        )
        storage.create_message(
            int(creature["id"]),
            conversation_id=int(conversation_id),
            role="creature",
            body=body,
            run_id=run_id,
            metadata={
                "trigger_type": "summoning_planning",
                "run_scope": RUN_SCOPE_CHAT,
                "sandbox_mode": "read-only",
                "system_role": KEEPER_SYSTEM_ROLE,
            },
        )
        return {
            "status": "summoned",
            "creature_slug": slug,
            "conversation_id": int(conversation_id),
            "assistant_body": body,
            "created_creatures": created_entries,
        }
    except Exception:
        for created_slug in reversed(created_slugs):
            try:
                delete_creature(created_slug)
            except Exception:
                continue
        raise


def _preview_for_onboarding_suggestion(
    *,
    brief: str,
    purpose_summary: str = "",
    purpose_markdown: str = "",
    origin_context: str = "",
    opening_question: str = "",
) -> dict[str, Any]:
    form_state = _summoning_form_state(
        brief=brief,
        origin_context=origin_context,
        opening_question=opening_question,
        temperament=DEFAULT_TEMPERAMENT,
        iteration=0,
    )
    ecosystem_key = _infer_ecosystem_from_brief(brief)
    role_summary = _normalize_summoning_role_summary(
        purpose_summary,
        brief=brief,
        temperament=DEFAULT_TEMPERAMENT,
    ) if purpose_summary else _descriptor_summary("", brief, temperament=DEFAULT_TEMPERAMENT)
    identity = _generate_summoned_identity(
        form_state=form_state,
        ecosystem_key=ecosystem_key,
        purpose_summary=role_summary,
    )
    preview = {
        "kind": "single",
        "brief": form_state["brief"],
        "origin_context": form_state["origin_context"],
        "opening_question": form_state["opening_question"],
        "temperament": form_state["temperament"],
        "ecosystem": ecosystem_key,
        "ecosystem_label": _ecosystem_label(ecosystem_key),
        "proposed_name": str(identity["display_name"]),
        "slug": str(identity["slug"]),
        "role_summary": role_summary,
        "alternates": list(identity.get("alternates") or []),
        "used_explicit_name": bool(identity.get("explicit_name")),
    }
    if purpose_summary:
        preview["role_summary"] = role_summary
    if purpose_markdown:
        preview["purpose_markdown"] = _normalize_purpose_markdown(purpose_markdown)
    return preview


def onboarding_state(*, preview_ecosystem: str | None = None, include_chat: bool = False) -> dict[str, Any]:
    current_ecosystem = get_ecosystem()
    selected_ecosystem = _normalize_app_ecosystem_value(preview_ecosystem) if preview_ecosystem else current_ecosystem["value"]
    phase = get_onboarding_phase()
    chat_mode = "onboarding" if phase == "starter" else "keeper"
    should_load_chat = phase == "starter" or include_chat
    chat_state = _ensure_onboarding_chat(mode=chat_mode) if should_load_chat else {"keeper_name": ONBOARDING_KEEPER_NAME, "messages": []}
    starter_ready = _onboarding_starter_ready_from_messages(list(chat_state.get("messages") or []))
    state: dict[str, Any] = {
        "required": phase != "complete",
        "phase": phase,
        "ecosystem_current": selected_ecosystem,
        "ecosystem_choices": _ecosystem_cards(),
        "ecosystem_asset_manifest": _onboarding_ecosystem_asset_manifest(),
        "ecosystem_preload_assets": _onboarding_ecosystem_preload_assets(selected_ecosystem, limit=6),
        "briefing": {},
        "answers": _load_meta_json_dict(ONBOARDING_ANSWERS_KEY),
        "keeper_name": str(chat_state.get("keeper_name") or ONBOARDING_KEEPER_NAME),
        "creature_slug": str(chat_state.get("creature_slug") or ""),
        "conversation_id": int(chat_state["conversation_id"]) if chat_state.get("conversation_id") is not None else None,
        "chat_messages": list(chat_state.get("messages") or []),
        "thinking": dict(chat_state.get("thinking") or {}),
        "chat_feed": _load_onboarding_chat_feed(),
        "starter_ready": starter_ready,
    }
    if phase == "starter":
        state["briefing"] = _ensure_onboarding_briefing()
    return state


def confirm_onboarding_ecosystem(*, ecosystem_choice: str) -> dict[str, Any]:
    _initialize_runtime()
    set_ecosystem(choice=ecosystem_choice)
    _clear_onboarding_state(keep_phase=True, preserve_warmup=True)
    _set_onboarding_phase("starter")
    _ensure_keeper_creature(refresh_identity=True)
    return onboarding_state()


def onboarding_chat_feed_state() -> dict[str, Any]:
    _initialize_runtime()
    payload = _load_onboarding_chat_feed()
    run_id = int(payload.get("run_id") or 0)
    if run_id > 0:
        payload["stream_url"] = f"/creatures/{KEEPER_SLUG}/runs/{run_id}/stream"
    return payload


def onboarding_starter_feed_state() -> dict[str, Any]:
    _initialize_runtime()
    payload = _load_onboarding_starter_feed()
    run_id = int(payload.get("run_id") or 0)
    if run_id > 0:
        payload["stream_url"] = f"/creatures/{KEEPER_SLUG}/runs/{run_id}/stream"
    return payload


def complete_onboarding() -> None:
    _initialize_runtime()
    _set_onboarding_phase("complete")
    _ensure_welcome_conversation()
    for key in (
        ONBOARDING_BRIEFING_KEY,
        ONBOARDING_ANSWERS_KEY,
        ONBOARDING_ENVIRONMENT_KEY,
    ):
        storage.delete_meta(key)
    _store_onboarding_chat_feed(status="idle", lines=[], run_id=0, last_event_id=0, error="")
    _store_onboarding_starter_feed(status="idle", lines=[], run_id=0, last_event_id=0, error="")


def _interrupt_running_run(
    run: Any | None,
    *,
    error_text: str,
    clear_runtime_error: bool = True,
) -> bool:
    if run is None or str(_row_value(run, "status") or "").strip().lower() != "running":
        return False
    creature_id = int(run["creature_id"])
    run_id = int(run["id"])
    metadata = _parse_json(str(_row_value(run, "metadata_json") or ""))
    run_metadata = metadata if isinstance(metadata, dict) else {}
    task_next_run_at = None
    habit_id_raw = run_metadata.get("habit_id")
    habit_id = int(habit_id_raw or 0) if str(habit_id_raw or "").strip() else 0
    if habit_id > 0:
        habit = storage.get_habit_for_creature(creature_id, habit_id)
        if habit is not None:
            task_next_run_at = _habit_next_run_at(
                str(habit["schedule_kind"] or HABIT_SCHEDULE_MANUAL),
                _habit_schedule_json(habit["schedule_json"]),
            )
            storage.record_habit_run_finish(
                habit_id,
                status="failed",
                summary="",
                error_text=error_text,
                next_run_at=task_next_run_at,
                report_path=str(run["notes_path"] or ""),
            )
    storage.create_run_event(
        run_id,
        event_type="error",
        body=error_text,
        metadata={"error_text": error_text, "interrupted": True},
    )
    storage.create_run_event(
        run_id,
        event_type="status",
        body='{"type":"status","phase":"failed","interrupted":true}',
        metadata={"phase": "failed", "interrupted": True},
    )
    _clear_run_feed_event_timing(run_id)
    storage.finish_run(
        run_id,
        creature_id=creature_id,
        status="failed",
        raw_output_text=None,
        summary=None,
        severity="critical",
        message_text=None,
        error_text=error_text,
        next_run_at=task_next_run_at,
        metadata={
            **run_metadata,
            "run_scope": _run_scope_value(run),
            "conversation_id": int(run["conversation_id"]) if run["conversation_id"] is not None else None,
            "sandbox_mode": str(run["sandbox_mode"] or ""),
            "interrupted": True,
            "interrupted_reason": error_text,
        },
        notes_markdown=str(run["notes_markdown"] or "") or None,
        notes_path=str(run["notes_path"] or "") or None,
    )
    _forget_active_run_thread(run_id)
    if clear_runtime_error:
        storage.clear_creature_runtime_error(creature_id, next_run_at=task_next_run_at)
    return True


def _interrupt_running_conversation_run(conversation_id: int, *, error_text: str) -> bool:
    return _interrupt_running_run(
        storage.latest_run_for_conversation(conversation_id),
        error_text=error_text,
    )


def _interrupt_running_creature_run(
    creature_id: int,
    *,
    error_text: str,
    clear_runtime_error: bool = True,
) -> bool:
    return _interrupt_running_run(
        storage.latest_running_run_for_creature(creature_id),
        error_text=error_text,
        clear_runtime_error=clear_runtime_error,
    )


def reset_ecosystem() -> None:
    _initialize_runtime()
    timezone_name = _canonical_display_timezone_name(storage.get_meta(DISPLAY_TIMEZONE_KEY)) or _detect_system_timezone_name()
    for creature in list(storage.list_creatures()):
        _interrupt_running_creature_run(
            int(creature["id"]),
            error_text=ECOSYSTEM_RESET_RUN_ERROR_TEXT,
            clear_runtime_error=False,
        )
        storage.delete_creature(int(creature["id"]))
    shutil.rmtree(config.data_dir() / "creatures", ignore_errors=True)
    shutil.rmtree(config.data_dir() / MESSAGE_ATTACHMENT_DIRNAME, ignore_errors=True)
    storage.set_meta(OWNER_REFERENCE_KEY, DEFAULT_OWNER_REFERENCE)
    storage.set_meta(DEFAULT_CREATURE_MODEL_KEY, config.creature_model())
    storage.set_meta(DEFAULT_CREATURE_REASONING_EFFORT_KEY, _normalize_reasoning_effort_value(config.creature_reasoning_effort()))
    storage.set_meta(DISPLAY_TIMEZONE_KEY, timezone_name)
    _set_display_timezone_cache(timezone_name)
    storage.set_meta(ECOSYSTEM_KEY, DEFAULT_ECOSYSTEM)
    _clear_onboarding_state()
    _set_onboarding_phase(DEFAULT_ONBOARDING_PHASE)
    prewarm_onboarding_assets(force=True)


def restart_onboarding() -> dict[str, Any]:
    _initialize_runtime()
    keeper = storage.get_creature_by_slug(KEEPER_SLUG)
    if keeper is not None:
        conversation = storage.find_conversation_by_title(int(keeper["id"]), KEEPER_CONVERSATION_TITLE)
        if conversation is not None:
            _interrupt_running_conversation_run(
                int(conversation["id"]),
                error_text=ONBOARDING_RESTART_RUN_ERROR_TEXT,
            )
            storage.delete_conversation(int(conversation["id"]))
    _clear_onboarding_state(keep_phase=True, preserve_warmup=True)
    _set_onboarding_phase(DEFAULT_ONBOARDING_PHASE)
    return onboarding_state()


def set_owner_reference(*, choice: str = "", custom_value: str = "") -> dict[str, Any]:
    selected = _normalize_owner_reference_value(choice)
    custom = _normalize_owner_reference_value(custom_value)
    if selected == "__custom__":
        final_value = custom or DEFAULT_OWNER_REFERENCE
    elif selected in OWNER_REFERENCE_OPTIONS:
        final_value = selected
    elif selected:
        final_value = selected
    elif custom:
        final_value = custom
    else:
        final_value = DEFAULT_OWNER_REFERENCE
    storage.set_meta(OWNER_REFERENCE_KEY, final_value)
    for creature in storage.list_creatures():
        if not _normalize_owner_reference_value(_row_value(creature, "owner_reference_override")):
            _refresh_memory_owner_reference(creature)
    return _owner_reference_state()


def set_creature_owner_reference(
    slug: str,
    *,
    choice: str = "",
    custom_value: str = "",
) -> dict[str, Any]:
    _initialize_runtime()
    slug = str(canonical_creature_slug(slug))
    creature = storage.get_creature_by_slug(slug)
    if creature is None:
        raise KeyError(f"Unknown creature slug: {slug}")
    selected = str(choice or "").strip()
    custom = _normalize_owner_reference_value(custom_value)
    if selected == "__inherit__":
        final_override = None
    elif selected == "__custom__":
        final_override = custom or get_owner_reference()
    else:
        normalized_selected = _normalize_owner_reference_value(selected)
        if normalized_selected in OWNER_REFERENCE_OPTIONS:
            final_override = normalized_selected
        elif normalized_selected:
            final_override = normalized_selected
        elif custom:
            final_override = custom
        else:
            final_override = None
    storage.set_creature_owner_reference_override(int(creature["id"]), final_override)
    updated = storage.get_creature(int(creature["id"])) or creature
    _refresh_memory_owner_reference(updated)
    return _owner_reference_state(updated)


def set_creature_thinking_settings(
    slug: str,
    *,
    model_override: str = "",
    reasoning_effort_override: str = "",
) -> dict[str, Any]:
    _initialize_runtime()
    slug = str(canonical_creature_slug(slug))
    creature = storage.get_creature_by_slug(slug)
    if creature is None:
        raise KeyError(f"Unknown creature slug: {slug}")
    final_model_override = _normalize_model_value(model_override, allow_blank=True) or None
    final_effort_override = _normalize_reasoning_effort_value(reasoning_effort_override, allow_blank=True) or None
    storage.set_creature_thinking_overrides(
        int(creature["id"]),
        model_override=final_model_override,
        reasoning_effort_override=final_effort_override,
    )
    updated = storage.get_creature(int(creature["id"])) or creature
    return _thinking_state(updated)


def set_conversation_thinking_settings(
    slug: str,
    conversation_id: int,
    *,
    model_override: str = "",
    reasoning_effort_override: str = "",
) -> dict[str, Any]:
    _initialize_runtime()
    slug = str(canonical_creature_slug(slug))
    creature = storage.get_creature_by_slug(slug)
    if creature is None:
        raise KeyError(f"Unknown creature slug: {slug}")
    conversation = storage.get_conversation_for_creature(int(creature["id"]), int(conversation_id))
    if conversation is None:
        raise KeyError(f"Conversation {conversation_id} does not belong to creature {slug}")
    final_model_override = _normalize_model_value(model_override, allow_blank=True) or None
    final_effort_override = _normalize_reasoning_effort_value(reasoning_effort_override, allow_blank=True) or None
    storage.set_conversation_thinking_overrides(
        int(conversation["id"]),
        model_override=final_model_override,
        reasoning_effort_override=final_effort_override,
    )
    updated = storage.get_conversation(int(conversation["id"])) or conversation
    return _thinking_state(creature, updated)


def _owner_reference_instruction(creature: Any | None = None) -> str:
    return (
        f'Use "{get_owner_reference(creature)}" only when referring to the user in third person inside durable state, '
        "memory, worklists, or summaries. In direct chat replies, speak naturally and do not address the user with "
        "that label or use it as a salutation unless the conversation explicitly asks for that style."
    )


def _rewrite_owner_reference_memory_text(text: str, *, owner_reference: str) -> str:
    known_refs = [
        re.escape(DEFAULT_OWNER_REFERENCE),
        re.escape("the owner"),
        *(re.escape(item) for item in OWNER_REFERENCE_OPTIONS),
    ]
    action_refs = [*known_refs, re.escape("owner")]
    rewritten = re.sub(
        rf"\b(?:{'|'.join(action_refs)}) (?=(wanted|wants|approved|approves|expected|expects|preferred|prefers|asked|asks|requested|requests|needed|needs|said|says)\b)",
        owner_reference + " ",
        text,
        flags=re.IGNORECASE,
    )
    rewritten = re.sub(
        rf"\b(?:{'|'.join(action_refs)}) preferences\b",
        f"{owner_reference} preferences",
        rewritten,
        flags=re.IGNORECASE,
    )
    return rewritten


def _refresh_memory_owner_reference(creature: Any) -> None:
    owner_reference = get_owner_reference(creature)
    for row in storage.list_memory_records(int(creature["id"]), include_inactive=True):
        original = str(row["body"] or "")
        updated = _rewrite_owner_reference_memory_text(original, owner_reference=owner_reference)
        if updated != original:
            storage.update_memory_record_body(int(row["id"]), updated)
    _refresh_memory_doc(creature)


def _normalize_owner_reference_storage() -> None:
    global_value = _normalize_owner_reference_value(storage.get_meta(OWNER_REFERENCE_KEY))
    if global_value and global_value != str(storage.get_meta(OWNER_REFERENCE_KEY) or ""):
        storage.set_meta(OWNER_REFERENCE_KEY, global_value)
    for creature in storage.list_creatures():
        override = _normalize_owner_reference_value(_row_value(creature, "owner_reference_override"))
        if override != str(_row_value(creature, "owner_reference_override") or ""):
            storage.set_creature_owner_reference_override(int(creature["id"]), override or None)


def _slugify(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return cleaned or "creature"


def _normalize_conversation_title_text(text: str) -> str:
    return " ".join(str(text or "").strip().split())


def _clip_conversation_title(text: str, *, limit: int = MAX_CONVERSATION_TITLE_CHARS) -> str:
    normalized = _normalize_conversation_title_text(text)
    if not normalized:
        return NEW_CHAT_TITLE
    if len(normalized) <= limit:
        return normalized
    clipped = normalized[: max(1, limit - 3)].rstrip(" -,:;.!?")
    return f"{clipped}..."


def _strip_manual_title_lead(text: str) -> str:
    cleaned = _normalize_conversation_title_text(text)
    if not cleaned:
        return ""
    patterns = (
        r"^(?:can|could|would|will)\s+you\s+",
        r"^please\s+",
        r"^help\s+me\s+",
        r"^i\s+need\s+help\s+with\s+",
        r"^i\s+need\s+",
        r"^let'?s\s+",
    )
    stripped = cleaned
    changed = True
    while changed:
        changed = False
        for pattern in patterns:
            updated = re.sub(pattern, "", stripped, flags=re.IGNORECASE)
            if updated != stripped and updated.strip():
                stripped = updated.strip()
                changed = True
                break
    return stripped


def _conversation_title_clause(text: str) -> str:
    normalized = _normalize_conversation_title_text(text)
    if not normalized:
        return ""
    for separator in (":", ";", ".", "?", "!", " — ", " - ", ","):
        if separator not in normalized:
            continue
        candidate = normalized.split(separator, 1)[0].strip(" -,:;.!?")
        if len(candidate) >= 4:
            return candidate
    return ""


def _conversation_title_from_body(body: str) -> str:
    normalized = _normalize_conversation_title_text(body)
    if not normalized:
        return NEW_CHAT_TITLE
    preferred = _strip_manual_title_lead(normalized)
    candidate = _conversation_title_clause(preferred) or _conversation_title_clause(normalized)
    if not candidate:
        words = preferred.split() or normalized.split()
        candidate = " ".join(words[:6]) if words else normalized
    candidate = candidate[:1].upper() + candidate[1:] if candidate and candidate[:1].islower() else candidate
    return _clip_conversation_title(candidate)


def _conversation_title_from_message_payload(body: str, attachments: list[dict[str, Any]] | None = None) -> str:
    title = _conversation_title_from_body(body)
    if title != NEW_CHAT_TITLE:
        return title
    if attachments:
        first = attachments[0]
        if bool(first.get("is_image")):
            return "Shared image"
        return "Shared file"
    return title


def _conversation_uses_auto_title(title: str) -> bool:
    normalized = _normalize_conversation_title_text(title)
    return normalized in {NEW_CHAT_TITLE, NEW_HABIT_CHAT_TITLE}


def _default_activity_title(*, trigger_type: str = "") -> str:
    trigger_key = str(trigger_type or "").strip().lower()
    trigger_labels = {
        "bootstrap": INTRODUCTION_CHAT_TITLE,
        "manual": "Manual update",
        "habit": "Habit report",
    }
    if trigger_key in trigger_labels:
        return trigger_labels[trigger_key]
    if trigger_key:
        return str(trigger_key.replace("_", " ").strip().title() or "Activity update")
    return "Activity update"


def _activity_subject_phrase(text: str) -> str:
    normalized = _normalize_conversation_title_text(text)
    if not normalized:
        return ""
    stripped = re.sub(
        r"^(?:found|spotted|noticed|flagged|tracked|checked|rechecked|reviewed|confirmed|verified|queued|surfaced|opened)\s+(?:the\s+)?",
        "",
        normalized,
        flags=re.IGNORECASE,
    ).strip()
    if not stripped or stripped == normalized:
        return ""
    lowered = stripped.casefold()
    for separator in (" behind ", " after ", " because ", " from ", " with ", " during ", " while ", " when ", " at ", " in ", " on "):
        if separator in lowered:
            index = lowered.index(separator)
            stripped = stripped[:index].strip(" -,:;.!?")
            break
    if len(stripped) < 4:
        return ""
    return _clip_conversation_title(stripped)


def _activity_keyword_title(text: str, *, trigger_type: str = "") -> str:
    lowered = _normalize_conversation_title_text(text).casefold()
    if not lowered:
        return ""
    keyword_titles: tuple[tuple[tuple[str, ...], str], ...] = (
        (("starter", "creature"), "Starter summoning"),
        (("creature", "review"), "Creature review"),
        (("security",), "Security follow-up"),
        (("frontend",), "UI polish"),
        (("layout",), "UI polish"),
        (("polish",), "UI polish"),
        (("operator",), "Operator follow-up"),
        (("runtime",), "Operator follow-up"),
        (("deploy",), "Operator follow-up"),
        (("uptime",), "Operator follow-up"),
        (("docs",), "Writing follow-up"),
        (("documentation",), "Writing follow-up"),
        (("memory",), "Memory maintenance"),
    )
    for tokens, label in keyword_titles:
        if all(token in lowered for token in tokens):
            return label
    subject_phrase = _activity_subject_phrase(text)
    if subject_phrase:
        return subject_phrase
    fallback = _default_activity_title(trigger_type=trigger_type)
    generic_prefixes = (
        "i've ",
        "i have ",
        "i'm ",
        "i am ",
        "checked ",
        "rechecked ",
        "reviewed ",
        "looked ",
        "no change",
        "no new ",
        "still ",
    )
    candidate = _conversation_title_clause(text)
    if candidate:
        candidate_lower = candidate.casefold()
        if not any(candidate_lower.startswith(prefix) for prefix in generic_prefixes):
            return _clip_conversation_title(candidate)
    return fallback


def _conversation_title_from_run_data(run_data: Mapping[str, Any]) -> str:
    request_kind = _run_request_kind(run_data)
    if request_kind == "introduction":
        return INTRODUCTION_CHAT_TITLE
    if request_kind == PONDER_REQUEST_KIND:
        return PONDER_HABIT_TITLE
    trigger_type = str(run_data.get("trigger_type") or "").strip().lower()
    explicit = _normalize_conversation_title_text(str(run_data.get("spawned_conversation_title") or ""))
    if explicit:
        return _activity_keyword_title(explicit, trigger_type=trigger_type)
    for key in ("summary", "message_text", "activity_note", "next_focus"):
        value = _normalize_conversation_title_text(str(run_data.get(key) or ""))
        if value:
            return _activity_keyword_title(value, trigger_type=trigger_type)
    return _default_activity_title(trigger_type=trigger_type)


def _slugify_filename(text: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")
    return cleaned or "run"


def _notification_preview(text: str, *, limit: int = 280) -> str:
    cleaned = " ".join(str(text or "").strip().split())
    if len(cleaned) <= limit:
        return cleaned
    return f"{cleaned[: limit - 3].rstrip()}..."


def _strip_intro_lead(text: str, *, display_name: str) -> str:
    cleaned = str(text or "").strip()
    if not cleaned:
        return ""
    escaped_name = re.escape(str(display_name or "").strip())
    if escaped_name:
        cleaned = re.sub(
            rf"^\s*(?:hello(?:\s+\w+)?[,.]?\s+)?i['’]m\s+{escaped_name}[.!]?\s*",
            "",
            cleaned,
            flags=re.IGNORECASE,
        ).strip()
    return cleaned


def _intro_purpose_text(text: str) -> str:
    cleaned = " ".join(str(text or "").strip().split())
    if not cleaned:
        return ""
    cleaned = re.sub(r"\bFocus it primarily on\b.*$", "", cleaned, flags=re.IGNORECASE).strip(" .")
    cleaned = re.sub(r"\bWith a lighter eye on\b.*$", "", cleaned, flags=re.IGNORECASE).strip(" .")
    cleaned = re.sub(r"\bAround here,\b.*$", "", cleaned, flags=re.IGNORECASE).strip(" .")
    return _ensure_sentence(cleaned)


def _normalize_origin_context_text(text: Any, *, limit: int = 320) -> str:
    cleaned = " ".join(str(text or "").strip().split())
    if not cleaned:
        return ""
    cleaned = re.sub(r"^(?:origin context|handoff context|context)\s*:\s*", "", cleaned, flags=re.IGNORECASE)
    sentences = [part.strip() for part in re.split(r"(?<=[.!?])\s+", cleaned) if part.strip()]
    rewritten_sentences: list[str] = []
    replacements = (
        (r"^i['’]m\b", "You're"),
        (r"^i am\b", "You are"),
        (r"^i['’]d\b", "You'd"),
        (r"^i would\b", "You would"),
        (r"^i want\b", "You want"),
        (r"^i need\b", "You need"),
        (r"^i have\b", "You have"),
        (r"^it['’]s a book about\b", "You're writing a book about"),
        (r"^my\b", "Your"),
        (r"^would love\b", "You'd love"),
    )
    for sentence in sentences or [cleaned]:
        updated = sentence
        for pattern, replacement in replacements:
            next_value = re.sub(pattern, replacement, updated, flags=re.IGNORECASE)
            if next_value != updated:
                updated = next_value
                break
        updated = re.sub(r"\bhelping me\b", "helping you", updated, flags=re.IGNORECASE)
        updated = re.sub(r"\bsurprising me\b", "surprising you", updated, flags=re.IGNORECASE)
        rewritten_sentences.append(updated)
    cleaned = " ".join(rewritten_sentences).strip()
    if cleaned:
        cleaned = cleaned[:1].upper() + cleaned[1:]
    cleaned = _ensure_sentence(cleaned)
    return _notification_preview(cleaned, limit=limit)


def _normalize_intro_question_text(text: Any, *, limit: int = 220) -> str:
    cleaned = " ".join(str(text or "").strip().split())
    if not cleaned:
        return ""
    cleaned = re.sub(r"^(?:opening question|first question|question)\s*:\s*", "", cleaned, flags=re.IGNORECASE)
    cleaned = cleaned.rstrip(" .")
    if not cleaned:
        return ""
    if not cleaned.endswith("?"):
        cleaned = f"{cleaned}?"
    return _notification_preview(cleaned, limit=limit)


def _strip_summoning_creation_prefix(text: str) -> str:
    cleaned = " ".join(str(text or "").strip().split())
    if not cleaned:
        return ""
    patterns = (
        r"^(?:create|summon)\s+(?:a|an)\s+(?:single\s+)?creature\b(?:\s+that|\s+to)?\s*",
        r"^(?:create|summon)\s+(?:a|an)\s+(?:[\w'-]+\s+){0,3}creature\b(?:\s+that|\s+to)?\s*",
        r"^(?:create|summon)\s+(?:a|an)\s+",
    )
    for pattern in patterns:
        cleaned = re.sub(pattern, "", cleaned, flags=re.IGNORECASE)
    return cleaned.strip(" .")


def _human_context_excerpt(transcript: str, *, limit: int = 3) -> str:
    human_lines = [
        " ".join(line[len("Human:") :].strip().split())
        for line in str(transcript or "").splitlines()
        if line.startswith("Human:")
    ]
    if not human_lines:
        return ""
    excerpt_parts: list[str] = []
    for line in human_lines[-limit:]:
        if not line:
            continue
        if not excerpt_parts:
            excerpt_parts.append(line)
            continue
        previous = excerpt_parts[-1]
        separator = " " if previous.endswith((".", "!", "?")) else ". "
        excerpt_parts.append(f"{separator}{line}")
    excerpt = "".join(excerpt_parts).strip()
    return _notification_preview(_ensure_sentence(excerpt), limit=320)


def _resolve_origin_context_text(
    explicit_origin_context: Any = "",
    *,
    brief: str = "",
    transcript: str = "",
) -> str:
    explicit = _normalize_origin_context_text(explicit_origin_context)
    if explicit:
        return explicit
    stripped_brief = _normalize_origin_context_text(_strip_summoning_creation_prefix(brief))
    if stripped_brief:
        return stripped_brief
    return _human_context_excerpt(transcript)


def _default_intro_question(
    *,
    creature: Any,
    origin_context: str = "",
    concern: str = "",
) -> str:
    lowered = " ".join(part for part in (origin_context, concern) if part).lower()
    if any(token in lowered for token in ("book", "novel", "story", "chapter", "chapters", "scene", "manuscript", "plot", "draft")):
        return "Want me to start with structure, scene work, or chapter feedback?"
    if any(token in lowered for token in ("server", "runtime", "repo", "repository", "deploy", "deployment", "logs", "health", "incident")):
        return "Want me to start with the runtime, the repo, or one specific issue?"
    if any(token in lowered for token in ("social", "audience", "post", "posting", "twitter", "marketing", "x account", "community")):
        return "Want me to start with ideas, voice, or the current queue?"
    if any(token in lowered for token in ("finance", "budget", "portfolio", "brokerage", "market", "trading", "polymarket")):
        return "Want me to start with a specific decision, a workflow, or a live account question?"
    return "Where would you like me to start?"


def _intro_origin_sentence(origin_context: str) -> str:
    cleaned = _normalize_origin_context_text(origin_context)
    if not cleaned:
        return ""
    lowered = cleaned[:1].lower() + cleaned[1:] if cleaned else cleaned
    if re.match(r"^(?:you\b|your\b|you're\b|you are\b)", lowered, flags=re.IGNORECASE):
        return _ensure_sentence(f"I'm here because {lowered}")
    return _ensure_sentence(f"I'm here for this: {cleaned}")


def _intro_observation_excerpt(text: str) -> str:
    cleaned = " ".join(str(text or "").strip().split())
    if not cleaned:
        return ""
    sentences = [part.strip() for part in re.split(r"(?<=[.!?])\s+", cleaned) if part.strip()]
    excerpt = " ".join(sentences[:2]).strip() or cleaned
    return _notification_preview(excerpt, limit=220)


def _intro_self_description(creature: Any, concern: str) -> str:
    raw_candidates = [
        _intro_purpose_text(concern),
    ]
    for raw in raw_candidates:
        cleaned = " ".join(str(raw or "").strip().split())
        if not cleaned:
            continue
        cleaned = re.sub(r"^Create an? [^.]*? creature that\s+", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"^Create an? [^.]*? creature\s+", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"^It should\s+", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"^This creature\s+", "", cleaned, flags=re.IGNORECASE)

        rewrites: tuple[tuple[str, str], ...] = (
            ("Acts as ", "I act as "),
            ("Watches ", "I keep an eye on "),
            ("Reviews ", "I help review "),
            ("Thinks about ", "I help think through "),
            ("Cares about ", "I care about "),
            ("Focuses on ", "I focus on "),
            ("Hunts ", "I hunt "),
            ("Keeps ", "I keep an eye on "),
            ("Looks ", "I look into "),
            ("Organizes ", "I organize "),
            ("Drafts ", "I help draft "),
            ("Turns ", "I help turn "),
            ("Makes ", "I make "),
            ("Helps ", "I help with "),
            ("Eager to ", "I'm eager to "),
        )
        for prefix, replacement in rewrites:
            if cleaned.lower().startswith(prefix.lower()):
                return _ensure_sentence(f"{replacement}{cleaned[len(prefix):]}")

        if cleaned.lower().startswith("i "):
            return _ensure_sentence(cleaned)

        bare = cleaned.rstrip(" .")
        if bare and len(bare.split()) <= 16:
            return _ensure_sentence(f"I'm here to help with {bare[:1].lower() + bare[1:]}")
        return _ensure_sentence(cleaned)
    return ""

def _intro_first_impression_line(text: str) -> str:
    cleaned = " ".join(str(text or "").strip().split())
    if not cleaned:
        return ""
    if ":" in cleaned:
        lead, tail = cleaned.split(":", 1)
        lead_clean = " ".join(lead.split()).lower()
        if len(lead_clean.split()) <= 12 and any(
            token in lead_clean
            for token in ("pass", "scan", "look", "risk", "issue", "concern", "check", "triage", "first")
        ):
            cleaned = tail.strip()
    cleaned = re.sub(
        r"^(?:triage|first|initial|opening|quick)\s+(?:pass|scan|look|sweep)\s+(?:confirms?|shows?|suggests?|says?)\s+(?:that\s+)?",
        "",
        cleaned,
        flags=re.IGNORECASE,
    ).strip()
    cleaned = re.sub(
        r"^(?:the\s+)?top\s+(?:risk|issue|concern)\s+(?:is\s+)?(?:still\s+)?(?:unchanged|the same)\s*[:,-]?\s*",
        "",
        cleaned,
        flags=re.IGNORECASE,
    ).strip()
    cleaned = _ensure_sentence(cleaned)
    if not cleaned:
        return ""
    return f"On my first look around, the thing tugging at me most is this: {cleaned}"


def _run_request_kind(row: Any) -> str:
    metadata = {}
    if isinstance(row, dict):
        direct_request_kind = str(row.get("request_kind") or "").strip().lower()
        if direct_request_kind:
            return direct_request_kind
        raw_metadata = row.get("metadata")
        if isinstance(raw_metadata, dict):
            metadata = raw_metadata
        elif "metadata_json" in row:
            metadata = _parse_json(str(row.get("metadata_json") or ""))
    else:
        metadata = _parse_json(str(_row_value(row, "metadata_json") or ""))
    request_kind = str(metadata.get("request_kind") or "").strip().lower()
    if request_kind:
        return request_kind
    trigger_type = str(_row_value(row, "trigger_type") or "").strip().lower()
    if trigger_type == "bootstrap" and str(_row_value(row, "message_text") or "").strip():
        return "introduction"
    if _run_scope_value(row) == RUN_SCOPE_ACTIVITY:
        return "activity"
    return ""


def _latest_completed_activity_run(creature_id: int) -> Any | None:
    for row in storage.recent_runs(creature_id, limit=80):
        if str(_row_value(row, "status") or "").strip().lower() != "completed":
            continue
        if _run_scope_value(row) != RUN_SCOPE_ACTIVITY:
            continue
        if str(_row_value(row, "trigger_type") or "").strip().lower() == "bootstrap":
            continue
        return row
    return None


def _standing_message_for_creature(creature: Any) -> str:
    creature_id = int(_row_value(creature, "id") or 0)
    for row in storage.recent_runs(creature_id, limit=30):
        if str(_row_value(row, "status") or "").strip().lower() != "completed":
            continue
        if _run_scope_value(row) != RUN_SCOPE_ACTIVITY:
            continue
        if str(_row_value(row, "trigger_type") or "").strip().lower() == "bootstrap":
            continue
        message_text = " ".join(str(_row_value(row, "message_text") or "").split())
        if message_text:
            return message_text[:MAX_STANDING_MESSAGE_CHARS]
    return ""


def _run_finished_at(row: Any) -> datetime | None:
    return storage.from_iso(
        str(
            _row_value(row, "finished_at")
            or _row_value(row, "started_at")
            or ""
        )
    )


def _conversation_delta_entries(
    creature: Any,
    *,
    since_at: datetime | None,
    include_all_creatures: bool = False,
    limit: int = MAX_ACTIVITY_DELTA_ITEMS,
) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    subjects = list(storage.list_creatures()) if include_all_creatures else [creature]
    for subject in subjects:
        subject_id = int(_row_value(subject, "id") or 0)
        if subject_id <= 0:
            continue
        for conversation in storage.list_conversations(subject_id):
            changed_messages: list[dict[str, Any]] = []
            for row in storage.list_messages(int(conversation["id"]), limit=80):
                role = str(_row_value(row, "role") or "").strip().lower()
                if role not in {"user", "creature"}:
                    continue
                body = " ".join(str(_row_value(row, "body") or "").split())
                if not body:
                    continue
                created_at = storage.from_iso(str(_row_value(row, "created_at") or ""))
                if created_at is None:
                    continue
                if since_at is not None and created_at <= since_at:
                    continue
                changed_messages.append(
                    {
                        "role": role,
                        "body": body,
                        "created_at": created_at,
                        "date": created_at.astimezone(timezone.utc).date().isoformat(),
                    }
                )
            if not changed_messages:
                continue
            changed_messages.sort(key=lambda item: item["created_at"])
            user_messages = [item for item in changed_messages if item["role"] == "user"]
            creature_messages = [item for item in changed_messages if item["role"] == "creature"]
            if not user_messages:
                continue
            latest_message = changed_messages[-1]
            entries.append(
                {
                    "creature": str(_row_value(subject, "display_name") or "Unnamed creature").strip() or "Unnamed creature",
                    "conversation": str(_row_value(conversation, "title") or NEW_CHAT_TITLE).strip() or NEW_CHAT_TITLE,
                    "last_at": latest_message["created_at"],
                    "last_date": latest_message["date"],
                    "user_count": len(user_messages),
                    "creature_count": len(creature_messages),
                    "latest_user": _trim_for_prompt(str(user_messages[-1]["body"]) if user_messages else "", limit=220),
                    "latest_creature": _trim_for_prompt(str(creature_messages[-1]["body"]) if creature_messages else "", limit=220),
                }
            )
    entries.sort(key=lambda item: item["last_at"], reverse=True)
    return entries[:limit]


def _format_conversation_delta_prompt(
    entries: Sequence[Mapping[str, Any]],
    *,
    title: str,
) -> str:
    lines = [title]
    if not entries:
        lines.append("- No new conversation turns since the last background pass.")
        return "\n".join(lines)
    for item in entries:
        line = (
            f"- {item['last_date']} | {item['creature']} / {item['conversation']} "
            f"(user turns: {item['user_count']}, creature turns: {item['creature_count']})"
        )
        if item.get("latest_user"):
            line += f' | Latest human: "{item["latest_user"]}"'
        if item.get("latest_creature"):
            line += f' | Latest creature: "{item["latest_creature"]}"'
        lines.append(line)
    return "\n".join(lines)


def _recent_activity_headlines(creature: Any, *, limit: int = 3) -> list[str]:
    headlines: list[str] = []
    for row in storage.recent_runs(int(creature["id"]), limit=20):
        if str(_row_value(row, "status") or "").strip().lower() != "completed":
            continue
        if _run_scope_value(row) != RUN_SCOPE_ACTIVITY:
            continue
        if str(_row_value(row, "trigger_type") or "").strip().lower() == "bootstrap":
            continue
        headline = " ".join(
            str(
                _row_value(row, "summary")
                or _row_value(row, "message_text")
                or ""
            ).split()
        )
        if not headline:
            continue
        headlines.append(_ensure_sentence(_trim_for_prompt(headline, limit=180)))
        if len(headlines) >= limit:
            break
    return headlines


def _latest_completed_habit_run(creature_id: int, habit_id: int) -> Any | None:
    for row in storage.recent_runs(creature_id, limit=80):
        if str(_row_value(row, "status") or "").strip().lower() != "completed":
            continue
        if _run_scope_value(row) != RUN_SCOPE_ACTIVITY:
            continue
        metadata = _parse_json(str(_row_value(row, "metadata_json") or ""))
        raw_habit_id = metadata.get("habit_id") or metadata.get("task_id")
        if not str(raw_habit_id or "").strip():
            continue
        if int(raw_habit_id) != int(habit_id):
            continue
        return row
    return None


def _other_habit_reflection_lines(
    creature: Any,
    *,
    since_at: datetime | None,
    exclude_habit_id: int | None = None,
    limit: int = 6,
) -> list[str]:
    lines: list[str] = []
    for habit in _creature_habits_state(creature):
        habit_id = int(habit.get("id") or 0)
        if habit_id <= 0:
            continue
        if exclude_habit_id is not None and habit_id == int(exclude_habit_id):
            continue
        if _is_ponder_habit(habit):
            continue
        last_run_at = storage.from_iso(str(habit.get("last_run_at") or ""))
        last_status = str(habit.get("last_status") or "").strip().lower()
        last_summary = " ".join(str(habit.get("last_summary") or "").strip().split())
        last_error = " ".join(str(habit.get("last_error") or "").strip().split())
        if since_at is not None and last_run_at is not None and last_run_at <= since_at and not last_error:
            continue
        if last_error:
            lines.append(
                f"- {habit['title']}: trouble on the last run ({habit.get('last_run_at_display') or 'recently'}) — "
                f"{_notification_preview(last_error, limit=220)}"
            )
        elif last_status == "failed":
            lines.append(
                f"- {habit['title']}: the last run failed at {habit.get('last_run_at_display') or 'an unknown time'}."
            )
        elif last_summary:
            lines.append(
                f"- {habit['title']}: last run {habit.get('last_run_at_relative_display') or 'recently'} — "
                f"{_notification_preview(last_summary, limit=220)}"
            )
        elif last_run_at is not None:
            lines.append(
                f"- {habit['title']}: last moved {habit.get('last_run_at_relative_display') or 'recently'} with status "
                f"{last_status or 'completed'}."
            )
        if len(lines) >= limit:
            break
    if not lines:
        lines.append("- No meaningful friction or movement from your other habits since the last Ponder.")
    return lines


def _latest_intro_run(creature_id: int) -> Any | None:
    for row in storage.recent_runs(creature_id, limit=50):
        if str(_row_value(row, "status") or "").strip().lower() != "completed":
            continue
        if not str(_row_value(row, "message_text") or "").strip():
            continue
        if _run_request_kind(row) != "introduction":
            continue
        return row
    return None


def _chat_request_teaser(row: Any, *, ecosystem_value: str) -> str:
    name = str(_row_value(row, "creature_display_name") or "A creature").strip() or "A creature"
    severity = str(_row_value(row, "severity") or "info").strip().lower()
    request_kind = _run_request_kind(row)
    if request_kind == "introduction":
        intro_templates = {
            "woodlands": (
                "{name} is awake and ready to introduce itself.",
                "{name} is waiting by the first lantern to meet you.",
                "{name} has arrived and wants to tell you what it will watch.",
            ),
            "monster-wilds": (
                "{name} is awake and ready to introduce itself.",
                "{name} is waiting at the first lairfire to meet you.",
                "{name} has arrived and wants to tell you what it will guard.",
            ),
            "boneyard": (
                "{name} rose with an introduction and a first briefing.",
                "{name} is waiting by the boneyard gate to introduce itself.",
                "{name} is awake in the fog and ready to tell you what it will watch.",
            ),
            "sea": (
                "{name} is ready to introduce itself.",
                "{name} is waiting just inside the current to meet you.",
                "{name} has surfaced with an introduction and a first briefing.",
            ),
            "expanse": (
                "{name} is online and ready to introduce itself.",
                "{name} opened a first channel and wants to introduce itself.",
                "{name} is waiting to sync and tell you what it will track.",
            ),
            "terminal": (
                "{name} is online and ready to introduce itself.",
                "{name} has opened a clean terminal and wants to introduce itself.",
                "{name} just came online and wants to tell you what it will watch.",
            ),
        }
        normalized_ecosystem = _normalize_app_ecosystem_value(ecosystem_value)
        templates = intro_templates.get(normalized_ecosystem, intro_templates["woodlands"])
        index = (int(_row_value(row, "run_id") or 0) + len(name)) % len(templates)
        return templates[index].format(name=name)
    if request_kind == PONDER_REQUEST_KIND:
        ponder_templates = {
            "woodlands": (
                "{name} has been turning a question over in the quiet.",
                "{name} has something it wants to ask you by the tree line.",
                "{name} kept watch, thought it through, and wants your judgment.",
            ),
            "monster-wilds": (
                "{name} has been turning something over by the lairfire.",
                "{name} has a question it wants to ask before the next hunt.",
                "{name} has been thinking and wants your judgment.",
            ),
            "boneyard": (
                "{name} has been worrying something in the fog and wants your opinion.",
                "{name} has a quiet question waiting by the gate.",
                "{name} has been turning a thought over in the dark.",
            ),
            "sea": (
                "{name} has something it wants to sound out with you.",
                "{name} kept the thought in the current and wants your judgment.",
                "{name} has a question waiting just below the surface.",
            ),
            "expanse": (
                "{name} has been holding a question in the channel for you.",
                "{name} has a reflection it wants to run by you.",
                "{name} has something worth syncing on.",
            ),
            "terminal": (
                "{name} has a thought it wants to run by you.",
                "{name} has been turning a question over at the console.",
                "{name} has something it wants your judgment on.",
            ),
        }
        normalized_ecosystem = _normalize_app_ecosystem_value(ecosystem_value)
        templates = ponder_templates.get(normalized_ecosystem, ponder_templates["woodlands"])
        index = (int(_row_value(row, "run_id") or 0) + len(name)) % len(templates)
        return templates[index].format(name=name)
    templates_by_ecosystem = {
        "woodlands": (
            "{name} is waiting by the tree line.",
            "{name} has something worth your attention.",
            "{name} left a fresh set of tracks for you to follow.",
            "{name} is holding the clearing for a conversation.",
            "{name} wants to compare notes from the last sweep.",
            "{name} is ready when you are.",
            "{name} found something and wants you in the loop.",
            "{name} is watching the edge of the woods for you.",
        ),
        "monster-wilds": (
            "{name} is waiting by the lair mouth.",
            "{name} dragged something useful back from the cliffs.",
            "{name} left a fresh trail worth following.",
            "{name} is holding the perch for a conversation.",
            "{name} wants to compare notes from the last hunt.",
            "{name} is ready when you are.",
            "{name} found something and wants you in the loop.",
            "{name} has something worth bringing onto the hoard.",
        ),
        "boneyard": (
            "{name} is waiting by the boneyard gate.",
            "{name} has something worth pulling in from the fog.",
            "{name} left a fresh sign among the headstones.",
            "{name} is holding the lantern for a conversation.",
            "{name} wants to compare notes from the last haunting.",
            "{name} is ready when you are.",
            "{name} found something and wants you in the loop.",
            "{name} has something worth raising from the dark.",
        ),
        "sea": (
            "{name} is holding a quiet current open for you.",
            "{name} surfaced with something worth your attention.",
            "{name} left a fresh signal in the tide.",
            "{name} wants to compare notes from the last pass.",
            "{name} has something drifting your way.",
            "{name} is ready to open a conversation.",
            "{name} found something and wants you in the loop.",
            "{name} is waiting just below the surface.",
        ),
        "expanse": (
            "{name} queued a fresh transmission.",
            "{name} is pulsing for your attention.",
            "{name} is waiting to sync.",
            "{name} has a signal ready for pickup.",
            "{name} brought back something from the edge of the scan.",
            "{name} is holding a clean channel open for you.",
            "{name} wants to bring you into the loop.",
            "{name} is broadcasting on your wavelength.",
        ),
        "terminal": (
            "{name} opened a clean terminal for you.",
            "{name} has a fresh trace worth reviewing.",
            "{name} queued something useful for your attention.",
            "{name} wants to compare notes from the last pass.",
            "{name} has a signal ready on the console.",
            "{name} is holding a channel open for you.",
            "{name} found something and wants you in the loop.",
            "{name} is ready when you are.",
        ),
    }
    priority_templates = {
        "woodlands": (
            "{name} needs you at the tree line now.",
            "{name} is calling from the dark edge of the woods.",
        ),
        "monster-wilds": (
            "{name} is calling from the edge of the lair now.",
            "{name} dragged back something that should not wait.",
        ),
        "boneyard": (
            "{name} is calling through the fog right now.",
            "{name} surfaced something urgent from the boneyard line.",
        ),
        "sea": (
            "{name} surfaced something that should not wait.",
            "{name} is pushing a high-priority signal through the current.",
        ),
        "expanse": (
            "{name} is pushing a high-priority signal.",
            "{name} flagged a transmission that should not wait.",
        ),
        "terminal": (
            "{name} flagged a high-priority trace.",
            "{name} has an urgent console note waiting for you.",
        ),
    }
    normalized_ecosystem = _normalize_app_ecosystem_value(ecosystem_value)
    templates = (
        priority_templates.get(normalized_ecosystem, priority_templates["woodlands"])
        if severity == "critical"
        else templates_by_ecosystem.get(normalized_ecosystem, templates_by_ecosystem["woodlands"])
    )
    index = (int(_row_value(row, "run_id") or 0) + len(name)) % len(templates)
    return templates[index].format(name=name)


def _decorate_attention_request(creature: Any, run: Any, *, ecosystem_value: str) -> dict[str, Any]:
    run_data = _decorate_run(run)
    trigger_type = str(run_data.get("trigger_type") or "").strip().lower()
    request_kind = _run_request_kind(run_data)
    severity = str(run_data.get("severity") or "info").strip().lower() or "info"
    requested_at = str(run_data.get("finished_at") or run_data.get("started_at") or "")
    request = {
        **run_data,
        "run_id": int(run_data["id"]),
        "creature_slug": str(creature.get("slug") or ""),
        "creature_display_name": str(creature.get("display_name") or ""),
        "requested_at": requested_at,
        "requested_at_display": _format_timestamp_display(requested_at),
        "requested_at_compact_display": _format_timestamp_compact_display(requested_at),
        "severity": severity,
        "has_priority": severity in {"warning", "critical"},
        "trigger_type": trigger_type,
        "request_kind": request_kind,
        "is_introduction": request_kind == "introduction",
    }
    request["preview"] = _notification_preview(str(run_data.get("message_text") or run_data.get("summary") or ""))
    request["teaser"] = _chat_request_teaser(request, ecosystem_value=ecosystem_value)
    return request


def _run_needs_chat_request(run_data: dict[str, Any]) -> bool:
    if str(run_data.get("run_scope") or "") != RUN_SCOPE_ACTIVITY:
        return False
    if str(run_data.get("status") or "").lower() != "completed":
        return False
    if not str(run_data.get("message_text") or "").strip():
        return False
    if _run_request_kind(run_data) == "introduction":
        return True
    return bool(run_data["metadata"].get("should_notify"))


def _attention_request_candidates_for_creature(
    creature: dict[str, Any],
    *,
    ecosystem_value: str,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    requests: list[dict[str, Any]] = []
    creature_id = int(creature["id"])
    for run in storage.recent_runs(creature_id, limit=50):
        run_data = _decorate_run(run)
        if not _run_needs_chat_request(run_data):
            continue
        if (
            _run_request_kind(run_data) == "introduction"
            and storage.find_conversation_by_title(creature_id, INTRODUCTION_CHAT_TITLE) is not None
        ):
            continue
        if storage.find_conversation_by_source_run(int(run_data["id"])) is not None:
            continue
        requests.append(_decorate_attention_request(creature, run, ecosystem_value=ecosystem_value))
        if limit is not None and len(requests) >= limit:
            break
    requests.sort(
        key=lambda item: (
            0 if bool(item.get("is_introduction")) else 1,
            SEVERITY_ORDER.get(str(item.get("severity") or "info"), 3),
            str(item.get("requested_at") or ""),
        )
    )
    requests.sort(key=lambda item: str(item.get("requested_at") or ""), reverse=True)
    requests.sort(key=lambda item: 0 if bool(item.get("is_introduction")) else 1)
    return requests


def _latest_unspawned_ponder_run(creature: Mapping[str, Any]) -> dict[str, Any] | None:
    creature_id = int(creature.get("id") or 0)
    if creature_id <= 0:
        return None
    for run in storage.recent_runs(creature_id, limit=40):
        run_data = _decorate_run(run)
        if _run_request_kind(run_data) != PONDER_REQUEST_KIND:
            continue
        if not _run_needs_chat_request(run_data):
            continue
        if storage.find_conversation_by_source_run(int(run_data["id"])) is not None:
            continue
        return run_data
    return None


def _attention_requests(creatures: list[dict[str, Any]], *, ecosystem_value: str) -> list[dict[str, Any]]:
    requests: list[dict[str, Any]] = []
    for creature in creatures:
        candidate = next(iter(_attention_request_candidates_for_creature(creature, ecosystem_value=ecosystem_value, limit=1)), None)
        if candidate is not None:
            requests.append(candidate)
    requests.sort(
        key=lambda item: (
            SEVERITY_ORDER.get(str(item.get("severity") or "info"), 3),
            str(item.get("requested_at") or ""),
            str(item.get("creature_display_name") or "").lower(),
        ),
        reverse=False,
    )
    requests.sort(key=lambda item: str(item.get("requested_at") or ""), reverse=True)
    requests.sort(key=lambda item: SEVERITY_ORDER.get(str(item.get("severity") or "info"), 3))
    return requests


def _primary_attention_request_for_creature(creature: dict[str, Any], *, ecosystem_value: str) -> list[dict[str, Any]]:
    requests = _attention_request_candidates_for_creature(creature, ecosystem_value=ecosystem_value, limit=8)
    if not requests:
        return []
    primary = dict(requests[0])
    remaining = requests[1:]
    if not remaining:
        return [primary]
    urgent_count = sum(1 for item in remaining if str(item.get("severity") or "").lower() in {"warning", "critical"})
    lower_count = len(remaining) - urgent_count
    extra_parts: list[str] = []
    if urgent_count > 0:
        extra_parts.append(f"{urgent_count} other higher-priority update{'s' if urgent_count != 1 else ''}")
    if lower_count > 0:
        extra_parts.append(f"{lower_count} lower-priority note{'s' if lower_count != 1 else ''}")
    if extra_parts:
        if urgent_count > 0 and lower_count > 0:
            suffix = f"I also have {extra_parts[0]} and {extra_parts[1]}, but this is the best first chat."
        else:
            suffix = f"I also have {extra_parts[0]}, but this is the best first chat."
        primary["teaser"] = f"{str(primary.get('teaser') or '').rstrip()} {suffix}".strip()
    primary["pending_request_count"] = len(requests)
    return [primary]


def _run_surface_message(run_data: Mapping[str, Any]) -> str:
    text = str(
        run_data.get("message_text")
        or run_data.get("summary")
        or run_data.get("activity_note")
        or ""
    ).strip()
    if not text:
        return f"I wanted to pull you into a chat about my {str(run_data.get('trigger_type') or 'latest')} pass."
    return " ".join(text.split())


def _attention_request_state(requests: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "count": len(requests),
        "has_priority": any(bool(item.get("has_priority")) for item in requests),
    }


def _home_highlights(creatures: list[dict[str, Any]], *, ecosystem_value: str) -> list[dict[str, str]]:
    warning_creatures = [
        creature for creature in creatures
        if str(creature.get("last_run_severity") or "").lower() in {"warning", "critical"}
        or str(creature.get("status") or "").lower() == "error"
    ]
    templates_by_ecosystem = {
        "woodlands": (
            "{name} came back from a sweep with something worth another look.",
            "A sharper rustle came through {name}'s last round.",
            "{name} found a patch of ground that deserves attention.",
        ),
        "monster-wilds": (
            "{name} came back from a hunt with something worth another look.",
            "A sharper growl came through {name}'s last pass.",
            "{name} hauled back a problem that deserves attention.",
        ),
        "boneyard": (
            "{name} surfaced a haunting detail worth another look.",
            "A colder signal came back with {name}'s last round.",
            "{name} brought back something that should not stay buried.",
        ),
        "sea": (
            "{name} surfaced something worth another look.",
            "A stronger current showed up in {name}'s last pass.",
            "{name} brought back a signal worth keeping close.",
        ),
        "expanse": (
            "{name} surfaced an anomalous signal on the last pass.",
            "A stronger anomaly came back with {name}.",
            "{name} returned with something outside the usual drift.",
        ),
        "terminal": (
            "{name} surfaced a trace worth another look.",
            "A sharper signal came back with {name}'s last pass.",
            "{name} flagged something outside the usual noise floor.",
        ),
    }
    normalized_ecosystem = _normalize_app_ecosystem_value(ecosystem_value)
    templates = templates_by_ecosystem.get(normalized_ecosystem, templates_by_ecosystem["woodlands"])
    highlights: list[dict[str, str]] = []
    for index, creature in enumerate(warning_creatures[:3]):
        name = str(creature.get("display_name") or "A creature").strip() or "A creature"
        highlights.append(
            {
                "title": name,
                "body": templates[index % len(templates)].format(name=name),
                "when": _format_timestamp_display(creature.get("last_run_at")),
            }
        )
    return highlights


def _home_state(
    creatures: list[dict[str, Any]],
    requests: list[dict[str, Any]],
    *,
    ecosystem_value: str,
) -> dict[str, Any]:
    warning_count = sum(
        1
        for creature in creatures
        if str(creature.get("status") or "").lower() == "error"
        or str(creature.get("last_run_severity") or "").lower() in {"warning", "critical"}
    )
    urgent_creatures: set[str] = {
        str(item.get("creature_slug") or "")
        for item in requests
        if str(item.get("severity") or "").lower() == "critical"
    }
    for creature in creatures:
        if str(creature.get("status") or "").lower() == "error" or str(creature.get("last_run_severity") or "").lower() == "critical":
            urgent_creatures.add(str(creature.get("slug") or ""))
    return {
        "creature_count": len(creatures),
        "request_count": len(requests),
        "warning_count": warning_count,
        "urgent_count": len([slug for slug in urgent_creatures if slug]),
        "highlights": _home_highlights(creatures, ecosystem_value=ecosystem_value),
    }


def _creature_intro_state(creature: Any) -> dict[str, Any]:
    creature_id = int(creature["id"])
    intro_ready = False
    intro_run_id: int | None = None
    intro_conversation = None
    if _is_keeper_creature(creature):
        intro_conversation = _visible_keeper_conversation(creature_id)
        if intro_conversation is not None:
            intro_messages = storage.list_messages(int(intro_conversation["id"]), limit=12)
            intro_ready = any(str(row["role"] or "") == "creature" and str(row["body"] or "").strip() for row in intro_messages)
        else:
            intro_ready = True
        return {
            "intro_ready": intro_ready or True,
            "awakening": False,
            "intro_failed": False,
            "intro_run_id": None,
            "intro_conversation_id": int(intro_conversation["id"]) if intro_conversation is not None else None,
        }
    intro_run = _latest_intro_run(creature_id)
    if intro_run is not None:
        intro_ready = True
        intro_run_id = int(intro_run["id"])
        intro_conversation = storage.find_conversation_by_source_run(int(intro_run["id"]))
    if not intro_ready:
        intro_chat = storage.find_conversation_by_title(creature_id, INTRODUCTION_CHAT_TITLE)
        if intro_chat is not None:
            intro_messages = storage.list_messages(int(intro_chat["id"]), limit=12)
            intro_ready = any(str(row["role"] or "") == "creature" and str(row["body"] or "").strip() for row in intro_messages)
            if intro_ready:
                intro_conversation = intro_chat
    recent_runs = list(storage.recent_runs(creature_id, limit=20))
    latest_bootstrap = next(
        (row for row in recent_runs if str(row["trigger_type"] or "").strip().lower() == "bootstrap"),
        None,
    )
    latest_intro_attempt = next(
        (
            row
            for row in recent_runs
            if _run_scope_value(row) == RUN_SCOPE_ACTIVITY
            and str(row["trigger_type"] or "").strip().lower() != "bootstrap"
        ),
        None,
    )
    latest_running = storage.latest_running_run_for_creature(creature_id)
    latest_intro_status = str(_row_value(latest_running or latest_intro_attempt, "status") or "").lower()
    latest_bootstrap_status = str(_row_value(latest_running or latest_bootstrap, "status") or "").lower()
    intro_failed = bool(
        not intro_ready
        and (
            latest_intro_status == "failed"
            or latest_bootstrap_status == "failed"
        )
    )
    awakening = bool(not intro_ready and not intro_failed)
    return {
        "intro_ready": intro_ready,
        "awakening": awakening,
        "intro_failed": intro_failed,
        "intro_run_id": intro_run_id,
        "intro_conversation_id": int(intro_conversation["id"]) if intro_conversation is not None else None,
    }


def _should_retry_interrupted_bootstrap(creature: Any, intro_state: dict[str, Any] | None = None) -> bool:
    if _is_keeper_creature(creature):
        return False
    state = intro_state or _creature_intro_state(creature)
    if bool(state.get("intro_ready")) or not bool(state.get("intro_failed")):
        return False
    creature_id = int(creature["id"])
    if storage.latest_running_run_for_creature(creature_id) is not None:
        return False
    latest_bootstrap = next(
        (row for row in storage.recent_runs(creature_id, limit=20) if str(row["trigger_type"] or "").strip().lower() == "bootstrap"),
        None,
    )
    if latest_bootstrap is None or str(_row_value(latest_bootstrap, "status") or "").strip().lower() != "failed":
        return False
    error_text = str(_row_value(latest_bootstrap, "error_text") or _row_value(creature, "last_error") or "").strip()
    return INTERRUPTED_RUN_ERROR_TEXT in error_text


def _recover_interrupted_bootstraps() -> int:
    recovered = 0
    for creature in storage.list_creatures():
        intro_state = _creature_intro_state(creature)
        if not _should_retry_interrupted_bootstrap(creature, intro_state):
            continue
        _queue_creature_bootstrap(creature, focus_hint=_focus_hint_for_creature(creature))
        recovered += 1
    return recovered


def _should_queue_initial_intro_followup(creature: Any, intro_state: dict[str, Any] | None = None) -> bool:
    if _is_keeper_creature(creature):
        return False
    state = intro_state or _creature_intro_state(creature)
    if bool(state.get("intro_ready")) or bool(state.get("intro_failed")):
        return False
    creature_id = int(creature["id"])
    if storage.latest_running_run_for_creature(creature_id) is not None:
        return False
    if storage.find_conversation_by_title(creature_id, INTRODUCTION_CHAT_TITLE) is not None:
        return False
    recent_runs = list(storage.recent_runs(creature_id, limit=20))
    latest_bootstrap = next(
        (row for row in recent_runs if str(row["trigger_type"] or "").strip().lower() == "bootstrap"),
        None,
    )
    if latest_bootstrap is None or str(_row_value(latest_bootstrap, "status") or "").strip().lower() != "completed":
        return False
    latest_intro_attempt = next(
        (
            row
            for row in recent_runs
            if _run_scope_value(row) == RUN_SCOPE_ACTIVITY
            and str(row["trigger_type"] or "").strip().lower() != "bootstrap"
        ),
        None,
    )
    return latest_intro_attempt is None


def _queue_initial_intro_followup(creature: Any) -> dict[str, Any] | None:
    if not _should_queue_initial_intro_followup(creature):
        return None
    return start_background_run(
        str(creature["slug"]),
        trigger_type="followup",
        force_message=True,
        run_scope=RUN_SCOPE_ACTIVITY,
    )


def _recover_stalled_awakenings() -> int:
    recovered = 0
    for creature in storage.list_creatures():
        if _queue_initial_intro_followup(creature) is not None:
            recovered += 1
    return recovered


def _latest_or_create_chat(creature_id: int, *, title: str = NEW_CHAT_TITLE) -> dict[str, Any]:
    latest = storage.get_latest_conversation(creature_id)
    if latest is not None:
        return dict(latest)
    return dict(storage.create_conversation(creature_id, title=title))


def _preferred_creature_entry_target(creature: dict[str, Any], *, onboarding_required: bool) -> dict[str, Any]:
    if _is_keeper_creature(creature):
        return {"view": "creature", "conversation_id": None, "open_intro": False}
    if _is_intro_surfaced(creature):
        return {"view": "creature", "conversation_id": None, "open_intro": False}
    visible_conversations = [
        row
        for row in storage.list_conversations(int(creature["id"]))
        if not (_is_keeper_creature(creature) and not onboarding_required and _is_internal_keeper_conversation(row))
    ]
    intro_conversation_id = int(creature.get("intro_conversation_id") or 0)
    if intro_conversation_id > 0:
        non_intro_conversations = [row for row in visible_conversations if int(row["id"]) != intro_conversation_id]
        if not non_intro_conversations:
            return {"view": "chats", "conversation_id": intro_conversation_id, "open_intro": False}
    if int(creature.get("intro_run_id") or 0) > 0 and not visible_conversations:
        return {"view": "chats", "conversation_id": None, "open_intro": True}
    return {"view": "creature", "conversation_id": None, "open_intro": False}


def _keeper_dialog_generated_body(
    *,
    creatures: Sequence[Mapping[str, Any]],
    awakening: Sequence[Mapping[str, Any]],
    troubled: Sequence[Mapping[str, Any]],
    transition_notice: str = "",
) -> str:
    visible_creatures = [item for item in creatures if str(item.get("display_name") or "").strip()]
    awakening_creatures = [item for item in awakening if str(item.get("display_name") or "").strip()]
    troubled_creatures = [item for item in troubled if str(item.get("display_name") or "").strip()]

    lines: list[str] = []
    if not visible_creatures:
        if awakening_creatures:
            waking_name = str(awakening_creatures[0].get("display_name") or "The creature").strip() or "The creature"
            if transition_notice == "starter-creatures-creating":
                waking_creature = awakening_creatures[0]
                nature = _keeper_creature_nature_line(waking_creature)
                habit_hint = _keeper_habit_hint_line(waking_creature)
                lines.extend(
                    [
                        "",
                        f"**{waking_name}** is the first creature I have called here.",
                        "",
                        nature,
                    ]
                )
                if habit_hint:
                    lines.extend(
                        [
                            "",
                            habit_hint,
                        ]
                    )
                lines.extend(
                    [
                        "",
                        f"For now, be patient. **{waking_name}** is still crossing the threshold.",
                        "",
                        "It will wake soon enough, and speak for itself when it arrives.",
                    ]
                )
                return "\n".join(lines).strip()
            lines.extend(
                [
                    "",
                    f"Be patient. **{waking_name}** is still crossing the threshold.",
                    "",
                    "It will wake soon enough, and name its own nature when it arrives.",
                    "",
                    "If your desire has already changed, tell me now, and I will listen.",
                ]
            )
            return "\n".join(lines).strip()
        if troubled_creatures:
            troubled_creature = troubled_creatures[0]
            troubled_name = str(troubled_creature.get("display_name") or "That creature").strip() or "That creature"
            error_text = _notification_preview(str(troubled_creature.get("last_error") or "").strip(), limit=220)
            lines.extend(
                [
                    "",
                    f"**{troubled_name}** failed to wake cleanly.",
                    "",
                    "Something in the summoning went wrong before it could speak for itself.",
                ]
            )
            if error_text:
                lines.extend(
                    [
                        "",
                        f"Last trouble: {error_text}",
                    ]
                )
            lines.extend(
                [
                    "",
                    "If you want, I can try the summoning again once the path is clear.",
                ]
            )
            return "\n".join(lines).strip()
        lines.extend(
            [
                "",
                "No other creature walks here yet.",
                "",
                "Name the shape of help you desire, and I will decide what sort of being should answer.",
            ]
        )
        return "\n".join(lines).strip()

    visible_names = [
        str(item.get("display_name") or "").strip()
        for item in visible_creatures
        if str(item.get("display_name") or "").strip()
    ]
    first_name = visible_names[0] if visible_names else "the first creature"

    if len(visible_names) == 1:
        lines.extend(
            [
                "",
                f"**{first_name}** walks here now.",
            ]
        )
    elif len(visible_names) == 2:
        lines.extend(
            [
                "",
                f"**{visible_names[0]}** and **{visible_names[1]}** walk here now.",
            ]
        )
    else:
        leading = ", ".join(f"**{name}**" for name in visible_names[:2])
        lines.extend(
            [
                "",
                f"{leading}, and others besides, walk here now.",
            ]
        )

    if troubled_creatures:
        troubled_name = str(troubled_creatures[0].get("display_name") or "That creature").strip() or "That creature"
        lines.extend(
            [
                "",
                f"I sense unrest around **{troubled_name}**. If something feels wrong, name it.",
            ]
        )
    elif awakening_creatures:
        awakening_name = str(awakening_creatures[0].get("display_name") or "Another creature").strip() or "Another creature"
        lines.extend(
            [
                "",
                f"Another still crosses the threshold: **{awakening_name}**. It will speak in its own time.",
            ]
        )

    lines.extend(
        [
            "",
            (
                f"Tell me truly: is **{first_name}** serving you well?"
                if len(visible_names) == 1
                else "Tell me truly: are the creatures here serving you well?"
            ),
            "",
            "If another shape of help is still missing, name it, and I will consider whether another creature should be called.",
        ]
    )
    return "\n".join(lines).strip()


def _keeper_creature_nature_line(creature: Mapping[str, Any]) -> str:
    name = str(creature.get("display_name") or "This creature").strip() or "This creature"
    summary = _ensure_sentence(
        str(creature.get("purpose_summary") or creature.get("concern") or "").strip()
    )
    if summary:
        summary = re.sub(r"([,;:])\.$", ".", summary)
        lowered = summary[:1].lower() + summary[1:] if summary[:1] else summary
        infinitive_map = {
            "helps ": "help ",
            "watches ": "watch ",
            "keeps ": "keep ",
            "reads ": "read ",
            "shapes ": "shape ",
            "turns ": "turn ",
            "holds ": "hold ",
            "writes ": "write ",
            "makes ": "make ",
        }
        for prefix, replacement in infinitive_map.items():
            if lowered.lower().startswith(prefix):
                lowered = replacement + lowered[len(prefix):]
                return f"It was called to {lowered[:-1] if lowered.endswith('.') else lowered}."
        return f"It was called for this: {summary}"
    return f"**{name}** was called for work that mattered, and has begun to find its footing."


def _keeper_habit_hint_phrases(creature: Mapping[str, Any]) -> list[str]:
    summary = " ".join(
        part for part in (
            str(creature.get("purpose_summary") or "").strip(),
            str(creature.get("concern") or "").strip(),
        ) if part
    ).lower()
    if any(token in summary for token in ("book", "story", "chapter", "novel", "manuscript", "scene", "writing", "editor")):
        return [
            "watch your chapters for fresh edits and return with notes",
            "revisit touched scenes and surface the loose threads before they harden",
        ]
    if any(token in summary for token in ("social", "post", "marketing", "content", "audience", "x ", "x/", "twitter")):
        return [
            "return on a schedule with drafts worth posting",
            "keep watch for replies and threads that deserve your attention",
        ]
    if any(token in summary for token in ("docs", "teaching", "explain", "writing", "knowledge", "reference", "instruction")):
        return [
            "revisit your notes and make them clearer with each pass",
            "watch for places where the language grows muddy and gently clear it",
        ]
    return [
        "keep a quiet watch and return with a report when something shifts",
        "learn one repeated piece of work until it can carry it on a rhythm of its own",
    ]


def _keeper_habit_hint_line(creature: Mapping[str, Any]) -> str:
    name = str(creature.get("display_name") or "It").strip() or "It"
    hints = _keeper_habit_hint_phrases(creature)
    if not hints:
        return ""
    if len(hints) == 1:
        return f"In time, **{name}** can be taught habits. It could {hints[0]}."
    return f"In time, **{name}** can be taught habits. It could {hints[0]}, or {hints[1]}."


def _keeper_dialog_state(
    keeper: Mapping[str, Any],
    *,
    creatures: Sequence[Mapping[str, Any]],
    conversation: Mapping[str, Any] | None,
    messages: Sequence[Mapping[str, Any]],
    onboarding_required: bool,
    transition_notice: str = "",
) -> dict[str, Any]:
    visible_creatures = [
        creature
        for creature in creatures
        if not _is_keeper_creature(creature) and bool(creature.get("intro_ready"))
    ]
    awakening_creatures = [
        creature
        for creature in creatures
        if not _is_keeper_creature(creature) and bool(creature.get("awakening"))
    ]
    troubled_creatures = [
        creature
        for creature in creatures
        if not _is_keeper_creature(creature)
        and (
            bool(creature.get("intro_failed"))
            or str(creature.get("status") or "").lower() == "error"
            or str(creature.get("last_run_severity") or "").lower() in {"warning", "critical"}
        )
    ]
    latest_creature_body = next(
        (str(item.get("body") or "").strip() for item in reversed(list(messages)) if str(item.get("role") or "").lower() == "creature" and str(item.get("body") or "").strip()),
        "",
    )
    latest_user_body = next(
        (str(item.get("body") or "").strip() for item in reversed(list(messages)) if str(item.get("role") or "").lower() == "user" and str(item.get("body") or "").strip()),
        "",
    )
    conversation_title = str(conversation.get("title") or "").strip() if isinstance(conversation, Mapping) else ""
    has_non_welcome_dialog = bool(conversation_title and conversation_title != WELCOME_CONVERSATION_TITLE)
    starter_ready = _onboarding_starter_ready_from_messages(messages)

    generated_body = _keeper_dialog_generated_body(
        creatures=visible_creatures,
        awakening=awakening_creatures,
        troubled=troubled_creatures,
        transition_notice=transition_notice,
    )
    if latest_creature_body.startswith("Welcome to CreatureOS."):
        latest_creature_body = ""
    display_body = latest_creature_body if (latest_creature_body and (has_non_welcome_dialog or bool(latest_user_body))) else generated_body
    uses_typewriter = bool(display_body)
    typewriter_key = ""
    if uses_typewriter:
        digest = hashlib.sha1(display_body.encode("utf-8")).hexdigest()[:12]
        typewriter_key = f"keeper-return-{digest}"
    reply_placeholder = (
        "Speak to The Keeper…"
        if (visible_creatures or awakening_creatures)
        else "Name the first creature…"
    )
    creature_names = [str(item.get("display_name") or "").strip() for item in visible_creatures if str(item.get("display_name") or "").strip()]
    return {
        "body": display_body,
        "reply_placeholder": reply_placeholder,
        "conversation_id": int(conversation["id"]) if isinstance(conversation, Mapping) and conversation.get("id") is not None else None,
        "thinking": _thinking_state(keeper, conversation if isinstance(conversation, Mapping) else None),
        "has_reply": bool(latest_user_body),
        "last_user_body": latest_user_body,
        "summon_ready": bool(starter_ready and not onboarding_required),
        "typewriter_text": _markdown_plain_text(display_body) if uses_typewriter else "",
        "typewriter_once_key": typewriter_key,
        "creature_names": creature_names,
        "awakening_names": [str(item.get("display_name") or "").strip() for item in awakening_creatures if str(item.get("display_name") or "").strip()],
        "troubled_names": [str(item.get("display_name") or "").strip() for item in troubled_creatures if str(item.get("display_name") or "").strip()],
    }


def ensure_intro_conversation(slug: str) -> dict[str, Any] | None:
    _initialize_runtime()
    slug = str(canonical_creature_slug(slug))
    creature = storage.get_creature_by_slug(slug)
    if creature is None or _is_keeper_creature(creature):
        return None
    existing_intro = storage.find_conversation_by_title(int(creature["id"]), INTRODUCTION_CHAT_TITLE)
    if existing_intro is not None:
        return dict(existing_intro)
    intro_run = _latest_intro_run(int(creature["id"]))
    if intro_run is None:
        return None
    existing_from_run = storage.find_conversation_by_source_run(int(intro_run["id"]))
    if existing_from_run is not None:
        return dict(existing_from_run)
    return spawn_conversation_from_run(slug, int(intro_run["id"]))


def _introduction_message(
    creature: Any,
    *,
    concern: str,
    first_impression: str = "",
    naming_alternates: list[str] | None = None,
    used_explicit_name: bool = False,
    intro_context: dict[str, Any] | None = None,
) -> str:
    cleaned_concern = " ".join(str(concern or "").strip().split())
    explicit_origin_context = ""
    explicit_opening_question = ""
    if isinstance(intro_context, Mapping):
        explicit_origin_context = str(intro_context.get("origin_context") or "").strip()
        explicit_opening_question = str(intro_context.get("opening_question") or "").strip()
    origin_context = _normalize_origin_context_text(explicit_origin_context or _origin_context_for_creature(creature))
    opening_question = _normalize_intro_question_text(
        explicit_opening_question or _opening_question_for_creature(creature)
    )
    lines = [f"I'm {creature['display_name']}."]
    origin_sentence = _intro_origin_sentence(origin_context)
    self_description = _intro_self_description(creature, cleaned_concern)
    if origin_sentence:
        lines.append("")
        lines.append(origin_sentence)
    if self_description:
        lines.append("")
        lines.append(self_description)
    elif cleaned_concern:
        lines.append("")
        lines.append(_ensure_sentence(_intro_purpose_text(cleaned_concern) or cleaned_concern))
    lines.append("")
    lines.append(opening_question or _default_intro_question(creature=creature, origin_context=origin_context, concern=cleaned_concern))
    return "\n".join(lines)


def _decorate_message(row: Any) -> dict[str, Any]:
    data = _row_to_dict(row)
    metadata = _parse_json(str(data.get("metadata_json") or ""))
    data["metadata"] = metadata
    data["attachments"] = _message_attachments_for_metadata(metadata.get("attachments"), message_id=int(data.get("id") or 0))
    if bool(metadata.get("typewriter")):
        data["typewriter_text"] = _markdown_plain_text(str(data.get("body") or ""))
        data["typewriter_once_key"] = str(
            metadata.get("typewriter_once_key")
            or f"message-{int(data.get('id') or 0)}"
        )
    else:
        data["typewriter_text"] = ""
        data["typewriter_once_key"] = ""
    return data


def _message_attachments_root() -> Path:
    path = config.data_dir() / MESSAGE_ATTACHMENT_DIRNAME
    path.mkdir(parents=True, exist_ok=True)
    return path


def _safe_attachment_name(value: str | None) -> str:
    name = Path(str(value or "").strip() or "attachment").name.strip()
    name = re.sub(r"[\r\n\t]+", " ", name)
    name = " ".join(name.split())
    if not name:
        return "attachment"
    if len(name) <= 120:
        return name
    suffix = Path(name).suffix
    stem = Path(name).stem[: max(1, 120 - len(suffix) - 1)].rstrip(" .")
    return f"{stem}{suffix}" if suffix else stem


def _message_attachment_dir(message_id: int) -> Path:
    path = _message_attachments_root() / str(int(message_id))
    path.mkdir(parents=True, exist_ok=True)
    return path


def _message_attachment_relative_path(message_id: int, attachment_id: str, filename: str) -> str:
    suffix = Path(filename).suffix.lower()[:16]
    storage_name = f"{attachment_id}{suffix}"
    return str(Path(MESSAGE_ATTACHMENT_DIRNAME) / str(int(message_id)) / storage_name)


def _message_attachment_disk_path(relative_path: str | None) -> Path | None:
    cleaned = str(relative_path or "").strip().replace("\\", "/").lstrip("/")
    if not cleaned.startswith(f"{MESSAGE_ATTACHMENT_DIRNAME}/"):
        return None
    path = config.data_dir() / cleaned
    try:
        path.resolve().relative_to(config.data_dir().resolve())
    except Exception:
        return None
    return path


def _format_bytes_compact(size_bytes: int) -> str:
    value = max(0, int(size_bytes or 0))
    units = ("B", "KB", "MB", "GB")
    size = float(value)
    for unit in units:
        if size < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(size)} {unit}"
            return f"{size:.1f} {unit}".replace(".0 ", " ")
        size /= 1024.0
    return f"{value} B"


def _normalize_message_attachment(raw: Any, *, message_id: int, index: int = 0) -> dict[str, Any] | None:
    if not isinstance(raw, dict):
        return None
    attachment_id = " ".join(str(raw.get("id") or "").strip().split()) or f"att-{index + 1}"
    filename = _safe_attachment_name(raw.get("filename"))
    if not filename:
        return None
    content_type = " ".join(str(raw.get("content_type") or "").strip().split())
    if not content_type:
        guessed, _ = mimetypes.guess_type(filename)
        content_type = guessed or "application/octet-stream"
    relative_path = str(raw.get("relative_path") or "").strip()
    if not relative_path:
        relative_path = _message_attachment_relative_path(message_id, attachment_id, filename)
    path = _message_attachment_disk_path(relative_path)
    if path is None:
        return None
    size_bytes = int(raw.get("size_bytes") or 0)
    is_image = bool(raw.get("is_image")) or content_type.startswith("image/")
    return {
        "id": attachment_id,
        "filename": filename,
        "content_type": content_type,
        "size_bytes": size_bytes,
        "size_label": _format_bytes_compact(size_bytes),
        "is_image": is_image,
        "relative_path": relative_path,
        "disk_path": str(path),
        "url": f"/messages/{int(message_id)}/attachments/{attachment_id}",
    }


def _message_attachments_for_metadata(raw: Any, *, message_id: int) -> list[dict[str, Any]]:
    raw_items = raw if isinstance(raw, list) else []
    attachments: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for index, item in enumerate(raw_items):
        normalized = _normalize_message_attachment(item, message_id=message_id, index=index)
        if normalized is None or normalized["id"] in seen_ids:
            continue
        seen_ids.add(normalized["id"])
        attachments.append(normalized)
    return attachments


def _store_message_attachments(
    message_id: int,
    attachments: list[dict[str, Any]] | None,
) -> list[dict[str, Any]]:
    prepared = attachments or []
    if not prepared:
        return []
    if len(prepared) > MAX_MESSAGE_ATTACHMENTS:
        raise ValueError(f"You can attach up to {MAX_MESSAGE_ATTACHMENTS} files at once.")
    total_size = sum(max(0, int(item.get("size_bytes") or 0)) for item in prepared)
    if total_size > MAX_MESSAGE_ATTACHMENT_TOTAL_BYTES:
        raise ValueError(
            f"Attachments are too large together. Keep the total under {_format_bytes_compact(MAX_MESSAGE_ATTACHMENT_TOTAL_BYTES)}."
        )
    attachment_dir = _message_attachment_dir(message_id)
    stored: list[dict[str, Any]] = []
    for index, item in enumerate(prepared):
        filename = _safe_attachment_name(item.get("filename"))
        payload = item.get("content")
        if not filename or not isinstance(payload, (bytes, bytearray)):
            continue
        size_bytes = len(payload)
        if size_bytes > MAX_MESSAGE_ATTACHMENT_BYTES:
            raise ValueError(
                f"{filename} is too large. Keep each file under {_format_bytes_compact(MAX_MESSAGE_ATTACHMENT_BYTES)}."
            )
        attachment_id = f"att-{uuid.uuid4().hex[:8]}"
        relative_path = _message_attachment_relative_path(message_id, attachment_id, filename)
        disk_path = _message_attachment_disk_path(relative_path)
        if disk_path is None:
            continue
        disk_path.parent.mkdir(parents=True, exist_ok=True)
        with disk_path.open("wb") as handle:
            handle.write(bytes(payload))
        content_type = " ".join(str(item.get("content_type") or "").strip().split())
        if not content_type:
            guessed, _ = mimetypes.guess_type(filename)
            content_type = guessed or "application/octet-stream"
        stored.append(
            {
                "id": attachment_id,
                "filename": filename,
                "content_type": content_type,
                "size_bytes": size_bytes,
                "is_image": content_type.startswith("image/"),
                "relative_path": relative_path,
            }
        )
    return stored


def _message_attachment_payload(message_id: int, attachment_id: str) -> dict[str, Any]:
    row = storage.get_message(int(message_id))
    if row is None:
        raise KeyError(f"Unknown message id: {message_id}")
    metadata = _parse_json(str(row["metadata_json"] or ""))
    attachments = _message_attachments_for_metadata(metadata.get("attachments"), message_id=int(row["id"]))
    match = next((item for item in attachments if item["id"] == str(attachment_id).strip()), None)
    if match is None:
        raise KeyError(f"Unknown attachment {attachment_id} for message {message_id}")
    return match


def _next_available_slug(base: str) -> str:
    candidate = _slugify(base)
    if storage.get_creature_by_slug(candidate) is None:
        return candidate
    index = 2
    while storage.get_creature_by_slug(f"{candidate}-{index}") is not None:
        index += 1
    return f"{candidate}-{index}"


def _next_available_display_name(base: str) -> str:
    cleaned = " ".join(base.strip().split()) or "Summoned Creature"
    existing_names = {str(row["display_name"]).strip().lower() for row in storage.list_creatures()}
    if cleaned.lower() not in existing_names:
        return cleaned
    index = 2
    while f"{cleaned} {index}".lower() in existing_names:
        index += 1
    return f"{cleaned} {index}"


def _title_case_words(words: list[str]) -> str:
    return " ".join(word[:1].upper() + word[1:] for word in words if word)


def _normalize_temperament(value: str | None) -> str:
    cleaned = " ".join(str(value or "").strip().split())
    return cleaned if cleaned in TEMPERAMENT_OPTIONS else DEFAULT_TEMPERAMENT


def _normalize_ecosystem(value: str | None) -> str:
    return _normalize_creature_ecosystem(value)


def _ecosystem_label(ecosystem: str | None) -> str:
    normalized = _normalize_ecosystem(ecosystem)
    if not normalized:
        return ""
    return str((ECOSYSTEM_INDEX.get(normalized) or {}).get("label") or "")


def _ecosystem_naming_world(ecosystem: str | None) -> str:
    normalized = _normalize_ecosystem(ecosystem)
    if not normalized:
        return ""
    return str(ECOSYSTEM_NAMING_WORLDS.get(normalized) or "")


def _rotate_options(options: tuple[str, ...], offset: int) -> list[str]:
    if not options:
        return []
    shift = offset % len(options)
    return list(options[shift:] + options[:shift])


def _infer_ecosystem_from_brief(brief: str, *, fallback: str = "") -> str:
    normalized_fallback = _normalize_ecosystem(fallback)
    if normalized_fallback:
        return normalized_fallback
    keyword_set = set(_brief_keywords(brief))
    ranked: list[tuple[int, str]] = []
    for ecosystem_key, keywords in CREATURE_ECOSYSTEM_KEYWORDS.items():
        overlap = len(keyword_set.intersection(keywords))
        if overlap > 0:
            ranked.append((overlap, ecosystem_key))
    if ranked:
        ranked.sort(key=lambda item: (-item[0], item[1]))
        return ranked[0][1]
    return get_ecosystem()["value"]


def _descriptor_candidates_for_brief(brief: str, *, iteration: int = 0) -> list[str]:
    keyword_set = set(_brief_keywords(brief))
    matches: list[str] = []
    for keywords, descriptors in DESCRIPTOR_POOL:
        if keyword_set.intersection(keywords):
            for descriptor in descriptors:
                if descriptor not in matches:
                    matches.append(descriptor)
    for descriptor in GENERIC_DESCRIPTORS:
        if descriptor not in matches:
            matches.append(descriptor)
    if not matches:
        matches = list(GENERIC_DESCRIPTORS)
    return _rotate_options(tuple(matches), iteration)


def _ensure_sentence(text: str) -> str:
    cleaned = " ".join(str(text or "").strip().split())
    if not cleaned:
        return ""
    if cleaned.endswith((".", "!", "?")):
        return cleaned
    return f"{cleaned}."


def _markdown_plain_text(text: str) -> str:
    cleaned = str(text or "").replace("\r\n", "\n")
    if not cleaned.strip():
        return ""
    cleaned = re.sub(r"`([^`]+)`", r"\1", cleaned)
    cleaned = re.sub(r"\*\*([^*]+)\*\*", r"\1", cleaned)
    cleaned = re.sub(r"\*([^*]+)\*", r"\1", cleaned)
    cleaned = re.sub(r"__([^_]+)__", r"\1", cleaned)
    cleaned = re.sub(r"_([^_]+)_", r"\1", cleaned)
    cleaned = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", cleaned)
    cleaned = re.sub(r"^\s*[-*+]\s+", "• ", cleaned, flags=re.MULTILINE)
    cleaned = re.sub(r"^\s*\d+\.\s+", "", cleaned, flags=re.MULTILINE)
    return cleaned.strip()


def _descriptor_needs_normalization(descriptor: str) -> bool:
    cleaned = " ".join(str(descriptor or "").strip().split()).lower()
    if not cleaned:
        return True
    return any(term in cleaned for term in ROLE_NORMALIZATION_TERMS)


def _default_summary_for_descriptor(descriptor: str) -> str:
    lowered = str(descriptor or "").strip().lower()
    if any(token in lowered for token in ("security", "auth", "access", "credential", "login", "audit", "risk")):
        return "Reviews security-sensitive behavior, tracks risk, and escalates actionable findings."
    if any(token in lowered for token in ("network", "traffic", "packet", "routing", "route")):
        return "Monitors network behavior, investigates anomalies, and escalates actionable findings."
    if any(token in lowered for token in ("file", "artifact", "storage", "archive")):
        return "Reviews file and storage behavior, tracks important issues, and escalates actionable findings."
    if any(token in lowered for token in ("build", "pipeline", "deploy", "release")):
        return "Monitors delivery workflows, tracks failures or drift, and escalates actionable findings."
    if any(token in lowered for token in ("title", "record", "listing", "deed")):
        return "Reviews title and record issues, tracks edge cases, and escalates actionable findings."
    if any(token in lowered for token in ("cache", "search", "index", "query", "lookup")):
        return "Monitors indexing and search behavior, tracks regressions, and escalates actionable findings."
    if any(token in lowered for token in ("trace", "metric", "log", "latency", "observability")):
        return "Reviews logs, metrics, and tracing signals to identify actionable issues."
    if any(token in lowered for token in ("code", "patch", "defect", "repository", "quality")):
        return "Reviews code and defects, tracks recurring issues, and escalates actionable findings."
    return "Handles a defined scope of work, tracks important changes, and escalates actionable findings."


def _descriptor_summary(descriptor: str, brief: str, *, temperament: str) -> str:
    focus = _brief_to_concern(brief).strip()
    focus = re.sub(
        r"^(?:create|summon)\s+(?:a|an)\s+(?:single\s+)?creature\b(?:\s+that|\s+to)?\s*",
        "",
        focus,
        flags=re.IGNORECASE,
    )
    focus = re.sub(
        r"^(?:create|summon)\s+(?:a|an)\s+(?:[\w'-]+\s+){0,3}creature\b(?:\s+that|\s+to)?\s*",
        "",
        focus,
        flags=re.IGNORECASE,
    )
    focus = re.sub(r"^(?:create|summon)\s+(?:a|an)\s+", "", focus, flags=re.IGNORECASE)
    focus = re.sub(r"\bFocus it primarily on\b.*$", "", focus, flags=re.IGNORECASE)
    focus = re.sub(r"\bLet it keep a lighter secondary eye on\b.*$", "", focus, flags=re.IGNORECASE)
    focus = re.sub(r"\bIt exists specifically to help with this context:\b.*$", "", focus, flags=re.IGNORECASE)
    focus = re.sub(r"\bTailor it toward:\b.*$", "", focus, flags=re.IGNORECASE)
    focus = focus.strip(" .")
    word_count = len(focus.split())
    if (
        not focus
        or word_count <= 4
        or re.match(r"^(?:make|build|create|be|act as|serve as|serves as)\b", focus, flags=re.IGNORECASE)
    ):
        return _default_summary_for_descriptor(descriptor)
    sentence = focus[:1].upper() + focus[1:]
    return _ensure_sentence(sentence)


def _temperament_choices() -> list[dict[str, str]]:
    return [{"value": item, "label": item} for item in TEMPERAMENT_OPTIONS]


def default_summoning_form_state() -> dict[str, Any]:
    return {
        "brief": "",
        "origin_context": "",
        "opening_question": "",
        "name": "",
        "ecosystem": "",
        "temperament": DEFAULT_TEMPERAMENT,
        "iteration": 0,
        "temperament_choices": _temperament_choices(),
    }


def _summoning_form_state(
    *,
    brief: str = "",
    origin_context: str = "",
    opening_question: str = "",
    name: str = "",
    ecosystem: str = "",
    temperament: str = DEFAULT_TEMPERAMENT,
    iteration: int = 0,
) -> dict[str, Any]:
    state = default_summoning_form_state()
    state.update(
        {
            "brief": str(brief or "").strip(),
            "origin_context": _resolve_origin_context_text(origin_context, brief=str(brief or "").strip()),
            "opening_question": _normalize_intro_question_text(opening_question),
            "name": " ".join(str(name or "").strip().split()),
            "ecosystem": _normalize_ecosystem(ecosystem),
            "temperament": _normalize_temperament(temperament),
            "iteration": max(0, int(iteration or 0)),
        }
    )
    return state


def _extract_explicit_creature_name(brief: str) -> str | None:
    cleaned = " ".join(brief.strip().split())
    patterns = (
        r"\b(?:named|called)\s+[\"']?([a-z0-9][a-z0-9 '&/:-]{1,60})[\"']?",
        r"\bname\s+(?:it|them|this)\s+[\"']?([a-z0-9][a-z0-9 '&/:-]{1,60})[\"']?",
        r"\bcall\s+(?:it|them|this)\s+[\"']?([a-z0-9][a-z0-9 '&/:-]{1,60})[\"']?",
    )
    for pattern in patterns:
        match = re.search(pattern, cleaned, re.IGNORECASE)
        if match:
            raw_name = re.sub(r"\s+", " ", match.group(1)).strip(" .,:;!-")
            if raw_name:
                normalized = _normalize_generated_creature_name(raw_name)
                if normalized:
                    return _title_case_words(normalized.split())
    return None


def _brief_keywords(brief: str) -> list[str]:
    tokens = re.findall(r"[a-zA-Z][a-zA-Z0-9'-]+", brief)
    filtered = [token.lower() for token in tokens if len(token) >= 4]
    return [token for token in filtered if token not in NAME_STOPWORDS]


def _name_words(text: str) -> set[str]:
    return {word.lower() for word in re.findall(r"[A-Za-z]+", str(text or ""))}


def _used_creature_name_words() -> set[str]:
    used: set[str] = set()
    for row in storage.list_creatures():
        used.update(_name_words(str(row["display_name"] or "")))
    return used


def _existing_ecosystem_names() -> list[str]:
    return [str(row["display_name"] or "").strip() for row in storage.list_creatures() if str(row["display_name"] or "").strip()]


def _normalize_generated_name(value: Any) -> str:
    return " ".join(str(value or "").strip().split())


def _normalize_generated_creature_name(value: Any) -> str:
    cleaned = _normalize_generated_name(value)
    if not cleaned:
        return ""
    if len(cleaned) > MAX_CREATURE_DISPLAY_NAME_CHARS:
        cleaned = cleaned[:MAX_CREATURE_DISPLAY_NAME_CHARS].rstrip()
    if not re.search(r"[A-Za-z]", cleaned):
        return ""
    if re.search(r"[:/\\\\|@#]", cleaned):
        return ""

    words = cleaned.split()
    if len(words) > 3:
        return ""

    normalized_words: list[str] = []
    lowered_words: list[str] = []
    for word in words:
        bare = re.sub(r"^[^A-Za-z0-9]+|[^A-Za-z0-9]+$", "", word)
        if not bare or not re.search(r"[A-Za-z]", bare):
            return ""
        if not re.fullmatch(r"[A-Za-z0-9]+(?:['-][A-Za-z0-9]+)*", bare):
            return ""
        normalized_words.append(bare)
        lowered_words.append(bare.lower())

    if any(word in ROLE_NAME_LEAK_WORDS or word in GENERATED_NAME_OPERATIONAL_WORDS for word in lowered_words):
        return ""
    return " ".join(normalized_words)


def _extract_json_object(raw_text: str) -> dict[str, Any] | None:
    candidates: list[str] = []
    stripped = str(raw_text or "").strip()
    if stripped:
        candidates.append(stripped)
    fenced_blocks = re.findall(r"```(?:json)?\s*(\{.*?\})\s*```", str(raw_text or ""), flags=re.DOTALL | re.IGNORECASE)
    candidates.extend(block.strip() for block in fenced_blocks if block.strip())
    start = stripped.find("{")
    end = stripped.rfind("}")
    if start != -1 and end != -1 and end > start:
        candidates.append(stripped[start : end + 1])
    seen: set[str] = set()
    for candidate in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)
        try:
            payload = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            return payload
    return None


def _coerce_string_list(
    values: Any,
    *,
    limit: int,
    normalizer: Callable[[Any], str] = _normalize_generated_name,
) -> list[str]:
    items = values if isinstance(values, list) else []
    cleaned: list[str] = []
    seen: set[str] = set()
    for item in items:
        text = normalizer(item)
        if not text:
            continue
        lowered = text.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        cleaned.append(text)
        if len(cleaned) >= limit:
            break
    return cleaned


def _role_descriptor_uses_mascot_language(text: str) -> bool:
    cleaned = " ".join(str(text or "").strip().split()).lower()
    if not cleaned:
        return True
    if any(term in cleaned for term in ROLE_NORMALIZATION_TERMS):
        return True
    forbidden_words = set(ECOSYSTEM_DESCRIPTOR_FORBIDDEN_WORDS)
    return any(re.search(rf"\b{re.escape(word)}\b", cleaned) for word in forbidden_words)


def _role_summary_uses_mascot_language(text: str) -> bool:
    cleaned = " ".join(str(text or "").strip().split()).lower()
    if not cleaned:
        return True
    if any(phrase in cleaned for phrase in ROLE_SUMMARY_FORBIDDEN_PHRASES):
        return True
    forbidden_words = set(ECOSYSTEM_DESCRIPTOR_FORBIDDEN_WORDS)
    return any(re.search(rf"\b{re.escape(word)}\b", cleaned) for word in forbidden_words)


def _summoning_name_candidates_prompt(
    *,
    brief: str,
    purpose_summary: str,
    ecosystem: str,
) -> str:
    ecosystem_label = _ecosystem_label(ecosystem) or "Unspecified"
    ecosystem_world = _ecosystem_naming_world(ecosystem)
    existing_names = _existing_ecosystem_names()
    used_words = sorted(_used_creature_name_words())
    schema = '{"candidates":["Mothwake","Harbor Bell","Juniper Thread","... exactly 10 total"]}'
    lines = [
        "You are naming one new CreatureOS creature.",
        "Return JSON only. No markdown, no explanation, no prose outside the JSON object.",
        f"Creature ecosystem: {ecosystem_label}",
        f"Ecosystem world: {ecosystem_world}" if ecosystem_world else "",
        "",
        "Task:",
        "- Produce exactly 10 candidate names for this creature.",
        "- Base the names on the creature's purpose and its ecosystem.",
        "- The names should feel like real creature names, not job titles or role-plus-animal formulas.",
        "- Avoid reusing existing creature names.",
        "- Current habitat styling is irrelevant. Only the creature's own ecosystem matters.",
        "",
        f"Output shape: {schema}",
        f"Existing names to avoid: {json.dumps(existing_names)}",
        f"Existing creature name words to avoid when possible: {json.dumps(used_words)}",
        "",
        f"Purpose summary: {purpose_summary}",
        "Summoning brief:",
        brief.strip(),
    ]
    return "\n".join(line for line in lines if line)


def _codex_summoning_name_candidates(
    *,
    brief: str,
    purpose_summary: str,
    ecosystem: str,
) -> list[str]:
    prompt = _summoning_name_candidates_prompt(
        brief=brief,
        purpose_summary=purpose_summary,
        ecosystem=ecosystem,
    )
    try:
            result = _codex_start_thread(
                workdir=str(config.workspace_root()),
            prompt=prompt,
            sandbox_mode="read-only",
        )
    except (CodexCommandError, CodexTimeoutError):
        return []
    payload = _extract_json_object(result.final_text)
    if not payload:
        return []
    return _coerce_string_list(
        payload.get("candidates") or payload.get("names"),
        limit=10,
        normalizer=_normalize_generated_creature_name,
    )


def _summoning_name_selection_prompt(
    *,
    brief: str,
    purpose_summary: str,
    ecosystem: str,
    candidates: Sequence[str],
) -> str:
    ecosystem_label = _ecosystem_label(ecosystem) or "Unspecified"
    ecosystem_world = _ecosystem_naming_world(ecosystem)
    schema = '{"display_name":"Best Name","alternates":["Runner Up One","Runner Up Two","Runner Up Three"]}'
    lines = [
        "You are choosing the best name for one new CreatureOS creature.",
        "Return JSON only. No markdown, no explanation, no prose outside the JSON object.",
        f"Creature ecosystem: {ecosystem_label}",
        f"Ecosystem world: {ecosystem_world}" if ecosystem_world else "",
        "",
        "Task:",
        "- Choose the single best display_name from the provided candidates.",
        "- Keep display_name and alternates drawn from the candidate list.",
        "- Prefer the name that feels most alive, memorable, and right for this creature's purpose.",
        "",
        f"Output shape: {schema}",
        f"Candidates: {json.dumps(list(candidates))}",
        f"Purpose summary: {purpose_summary}",
        "Summoning brief:",
        brief.strip(),
    ]
    return "\n".join(line for line in lines if line)


def _codex_select_summoning_name(
    *,
    brief: str,
    purpose_summary: str,
    ecosystem: str,
    candidates: Sequence[str],
) -> dict[str, Any] | None:
    prompt = _summoning_name_selection_prompt(
        brief=brief,
        purpose_summary=purpose_summary,
        ecosystem=ecosystem,
        candidates=candidates,
    )
    try:
            result = _codex_start_thread(
                workdir=str(config.workspace_root()),
            prompt=prompt,
            sandbox_mode="read-only",
        )
    except (CodexCommandError, CodexTimeoutError):
        return None
    return _extract_json_object(result.final_text)


def _fallback_summoned_display_name(*, brief: str, ecosystem: str) -> str:
    keywords = [
        token
        for token in _brief_keywords(brief)
        if token not in ROLE_NAME_LEAK_WORDS and token not in ROLE_NORMALIZATION_TERMS
    ]
    if len(keywords) >= 2:
        for index in range(len(keywords) - 1):
            candidate = _normalize_generated_creature_name(_title_case_words(keywords[index : index + 2]))
            if candidate:
                return _next_available_display_name(candidate)
    ecosystem_label = (_ecosystem_label(ecosystem) or "Creature").removeprefix("The ").strip()
    seed = ecosystem_label.split()[-1] if ecosystem_label else "Creature"
    return _next_available_display_name(f"{seed} Echo")


def _resolve_summoned_name_plan(
    *,
    form_state: Mapping[str, Any],
    ecosystem_key: str,
    purpose_summary: str,
) -> dict[str, Any]:
    explicit_name = _normalize_generated_name(form_state.get("name")) or _extract_explicit_creature_name(
        str(form_state.get("brief") or "")
    )
    if explicit_name:
        display_name = _next_available_display_name(explicit_name)
        return {
            "candidates": [],
            "selection": {"display_name": explicit_name, "alternates": []},
            "display_name": display_name,
            "slug": _next_available_slug(display_name),
            "alternates": [],
            "explicit_name": True,
        }

    candidates = _codex_summoning_name_candidates(
        brief=str(form_state.get("brief") or ""),
        purpose_summary=purpose_summary,
        ecosystem=ecosystem_key,
    )
    deduped_candidates: list[str] = []
    candidate_lookup: dict[str, str] = {}
    for candidate in candidates:
        cleaned = _normalize_generated_creature_name(candidate)
        if not cleaned:
            continue
        lowered = cleaned.lower()
        if lowered in candidate_lookup:
            continue
        candidate_lookup[lowered] = cleaned
        deduped_candidates.append(cleaned)

    selection = (
        _codex_select_summoning_name(
            brief=str(form_state.get("brief") or ""),
            purpose_summary=purpose_summary,
            ecosystem=ecosystem_key,
            candidates=deduped_candidates,
        )
        if deduped_candidates
        else None
    )
    selected_candidate = _normalize_generated_creature_name(
        (selection or {}).get("display_name") or (selection or {}).get("name")
    )
    canonical_selected = candidate_lookup.get(selected_candidate.lower()) if selected_candidate else None
    if not canonical_selected and deduped_candidates:
        canonical_selected = deduped_candidates[0]
    if not canonical_selected:
        canonical_selected = _fallback_summoned_display_name(
            brief=str(form_state.get("brief") or ""),
            ecosystem=ecosystem_key,
        )

    display_name = _next_available_display_name(canonical_selected)
    selected_lower = canonical_selected.lower()
    display_lower = display_name.lower()
    selected_alternates = _coerce_string_list(
        (selection or {}).get("alternates"),
        limit=10,
        normalizer=_normalize_generated_creature_name,
    )
    alternates_source = selected_alternates or deduped_candidates
    alternates: list[str] = []
    seen_alternates: set[str] = set()
    for alternate in alternates_source:
        cleaned = _normalize_generated_creature_name(alternate)
        if not cleaned:
            continue
        canonical = candidate_lookup.get(cleaned.lower(), cleaned)
        lowered = canonical.lower()
        if lowered in seen_alternates or lowered in {selected_lower, display_lower}:
            continue
        seen_alternates.add(lowered)
        alternates.append(canonical)
        if len(alternates) >= 5:
            break

    return {
        "candidates": deduped_candidates,
        "selection": dict(selection or {}),
        "display_name": display_name,
        "slug": _next_available_slug(display_name),
        "alternates": alternates,
        "explicit_name": False,
    }


def _generate_summoned_identity(
    *,
    form_state: Mapping[str, Any],
    ecosystem_key: str,
    purpose_summary: str,
) -> dict[str, Any]:
    plan = _resolve_summoned_name_plan(
        form_state=form_state,
        ecosystem_key=ecosystem_key,
        purpose_summary=purpose_summary,
    )
    return {
        "display_name": str(plan["display_name"]),
        "slug": str(plan["slug"]),
        "alternates": list(plan.get("alternates") or []),
        "explicit_name": bool(plan.get("explicit_name")),
    }


def preview_summoning_names(
    *,
    brief: str,
    ecosystem: str = "",
    purpose_summary: str = "",
    temperament: str = DEFAULT_TEMPERAMENT,
    name: str = "",
) -> dict[str, Any]:
    _initialize_runtime()
    form_state = _summoning_form_state(
        brief=brief,
        ecosystem=ecosystem,
        name=name,
        temperament=temperament,
    )
    if not form_state["brief"]:
        raise ValueError("Creature brief is required")
    ecosystem_key = _normalize_ecosystem(form_state["ecosystem"]) or _infer_ecosystem_from_brief(form_state["brief"])
    resolved_purpose_summary = (
        _normalize_summoning_role_summary(purpose_summary, brief=form_state["brief"], temperament=form_state["temperament"])
        if str(purpose_summary or "").strip()
        else (
            _host_role_summary()
            if _is_onboarding_host_brief(form_state["brief"])
            else _descriptor_summary("", form_state["brief"], temperament=form_state["temperament"])
        )
    )
    plan = _resolve_summoned_name_plan(
        form_state=form_state,
        ecosystem_key=ecosystem_key,
        purpose_summary=resolved_purpose_summary,
    )
    return {
        "brief": form_state["brief"],
        "ecosystem": ecosystem_key,
        "ecosystem_label": _ecosystem_label(ecosystem_key),
        "purpose_summary": resolved_purpose_summary,
        "candidates": list(plan.get("candidates") or []),
        "selection": dict(plan.get("selection") or {}),
        "proposed_name": str(plan["display_name"]),
        "alternates": list(plan.get("alternates") or []),
        "used_explicit_name": bool(plan.get("explicit_name")),
    }


def preview_summoning(
    *,
    brief: str,
    origin_context: str = "",
    opening_question: str = "",
    name: str = "",
    ecosystem: str = "",
    temperament: str = DEFAULT_TEMPERAMENT,
    iteration: int = 0,
) -> dict[str, Any]:
    _initialize_runtime()
    form_state = _summoning_form_state(
        brief=brief,
        origin_context=origin_context,
        opening_question=opening_question,
        name=name,
        ecosystem=ecosystem,
        temperament=temperament,
        iteration=iteration,
    )
    if not form_state["brief"]:
        raise ValueError("Creature brief is required")

    explicit_ecosystem_key = _normalize_ecosystem(form_state["ecosystem"])
    ecosystem_key = explicit_ecosystem_key or _infer_ecosystem_from_brief(form_state["brief"])
    role_summary = (
        _host_role_summary()
        if _is_onboarding_host_brief(form_state["brief"])
        else _descriptor_summary("", form_state["brief"], temperament=form_state["temperament"])
    )
    identity = _generate_summoned_identity(
        form_state=form_state,
        ecosystem_key=ecosystem_key,
        purpose_summary=role_summary,
    )
    preview = {
        "kind": "single",
        "brief": form_state["brief"],
        "origin_context": form_state["origin_context"],
        "opening_question": form_state["opening_question"],
        "temperament": form_state["temperament"],
        "ecosystem": ecosystem_key,
        "ecosystem_label": _ecosystem_label(ecosystem_key),
        "proposed_name": str(identity["display_name"]),
        "slug": str(identity["slug"]),
        "role_summary": role_summary,
        "alternates": list(identity.get("alternates") or []),
        "used_explicit_name": bool(identity.get("explicit_name")),
    }
    return {"form_state": form_state, "preview": preview}


def serialize_summoning_preview(preview: dict[str, Any]) -> str:
    return json.dumps(preview, separators=(",", ":"), sort_keys=True)


def deserialize_summoning_preview(raw: str) -> dict[str, Any]:
    data = json.loads(str(raw or ""))
    if not isinstance(data, dict):
        raise ValueError("Invalid summoning preview payload")
    kind = str(data.get("kind") or "")
    if kind != "single":
        raise ValueError("Invalid summoning preview kind")
    return data


def confirm_summoning(
    preview_payload: str,
    *,
    on_bootstrap_event: Callable[[dict[str, Any]], None] | None = None,
    bootstrap_async: bool = False,
    bootstrap: bool = True,
) -> dict[str, Any]:
    preview = deserialize_summoning_preview(preview_payload)
    temperament = _normalize_temperament(str(preview.get("temperament") or DEFAULT_TEMPERAMENT))
    ecosystem = _normalize_ecosystem(str(preview.get("ecosystem") or ""))
    if _is_onboarding_host_brief(str(preview.get("brief") or "")):
        keeper = _ensure_keeper_creature(refresh_identity=True)
        return {"kind": "single", "creature_slug": str(keeper.get("slug") or KEEPER_SLUG)}
    display_name = str(preview["proposed_name"] or "").strip()
    role_summary = _normalize_summoning_role_summary(
        preview.get("role_summary") or preview.get("purpose_summary"),
        brief=str(preview.get("brief") or ""),
        temperament=temperament,
    )
    origin_context = _resolve_origin_context_text(
        preview.get("origin_context"),
        brief=str(preview.get("brief") or ""),
    )
    opening_question = _normalize_intro_question_text(preview.get("opening_question"))
    purpose_markdown = _normalize_purpose_markdown(preview.get("purpose_markdown"))
    if not _purpose_matches_display_name(purpose_markdown, display_name):
        purpose_markdown = _compose_summoning_purpose_markdown(
            display_name=display_name,
            purpose_summary=role_summary,
            brief=str(preview.get("brief") or ""),
            origin_context=origin_context,
            opening_question=opening_question,
        )
    creature = create_creature(
        display_name=display_name,
        ecosystem=ecosystem,
        purpose_summary=role_summary,
        summoning_brief=str(preview.get("brief") or ""),
        origin_context=origin_context,
        opening_question=opening_question,
        purpose_markdown=purpose_markdown,
        temperament=temperament,
        concern=role_summary,
        public_prompt=_summoning_prompt(
            display_name,
            role_summary,
            str(preview.get("brief") or ""),
            purpose_markdown=purpose_markdown,
            origin_context=origin_context,
            ecosystem=ecosystem,
            temperament=temperament,
        ),
        slug=str(preview.get("slug") or "") or None,
        intro_context={
            "naming_alternates": list(preview.get("alternates") or []),
            "used_explicit_name": bool(preview.get("used_explicit_name")),
            "temperament": temperament,
            "origin_context": origin_context,
            "opening_question": opening_question,
        },
        on_bootstrap_event=on_bootstrap_event,
        bootstrap_async=bootstrap_async,
        bootstrap=bootstrap,
    )
    return {"kind": "single", "creature_slug": str(creature["slug"])}


def _brief_to_concern(brief: str) -> str:
    lines = [line.strip() for line in brief.splitlines() if line.strip()]
    head = lines[0] if lines else brief.strip()
    normalized = " ".join(head.split())
    if len(normalized) <= 180:
        return normalized or "Track one narrow concern over time."
    return normalized[:177].rstrip() + "..."


def _normalize_purpose_markdown(value: Any) -> str:
    cleaned = str(value or "").replace("\r\n", "\n").strip()
    return cleaned


def _purpose_matches_display_name(purpose_markdown: str, display_name: str) -> bool:
    cleaned_purpose = _normalize_purpose_markdown(purpose_markdown)
    cleaned_name = " ".join(str(display_name or "").strip().split())
    if not cleaned_purpose or not cleaned_name:
        return False
    first_line = cleaned_purpose.splitlines()[0].strip().lower()
    return first_line == f"# {cleaned_name} purpose".lower()


def _normalize_summoning_role_summary(value: Any, *, brief: str, temperament: str = DEFAULT_TEMPERAMENT) -> str:
    cleaned = _ensure_sentence(str(value or "").strip())
    if cleaned and not _role_summary_uses_mascot_language(cleaned):
        return cleaned
    fallback = _descriptor_summary("", brief, temperament=temperament)
    return _ensure_sentence(fallback)


def _compose_summoning_purpose_markdown(
    *,
    display_name: str,
    purpose_summary: str,
    brief: str,
    origin_context: str = "",
    opening_question: str = "",
) -> str:
    cleaned_name = " ".join(str(display_name or "").strip().split()) or "Creature"
    normalized_origin = _ensure_sentence(_resolve_origin_context_text(origin_context, brief=brief))
    normalized_purpose = _normalize_summoning_role_summary(purpose_summary, brief=brief)
    normalized_brief = _ensure_sentence(_strip_summoning_creation_prefix(brief))
    normalized_question = _normalize_intro_question_text(opening_question)
    lines = [
        f"# {cleaned_name} purpose",
        "",
        "## Why you exist",
    ]
    if normalized_origin:
        lines.append(f"- {normalized_origin}")
    elif normalized_brief:
        lines.append(f"- {normalized_brief}")
    else:
        lines.append("- Stay close to the human's current context and help where it matters most.")
    if normalized_brief and (not normalized_origin or normalized_brief.lower() != normalized_origin.lower()):
        lines.append(f"- Internal handoff: {normalized_brief}")
    lines.extend(
        [
            "",
            "## What to help with",
            f"- {normalized_purpose}",
            "",
            "## Working style",
            "- Stay grounded in the real workspace and the live conversation.",
            "- Keep updates clear, concrete, and human.",
            "- Lean into the reason you were brought here instead of drifting back to generic helper behavior.",
            "- Ask for clarification when the next step is ambiguous, but do not become passive.",
        ]
    )
    if normalized_question:
        lines.extend(
            [
                "",
                "## Opening move",
                f"- {normalized_question}",
            ]
        )
    return "\n".join(lines).strip()


def _summoning_prompt(
    display_name: str,
    concern: str,
    brief: str,
    *,
    purpose_markdown: str = "",
    origin_context: str = "",
    ecosystem: str = "",
    temperament: str = DEFAULT_TEMPERAMENT,
    descriptor: str = "",
) -> str:
    cleaned_brief = brief.strip()
    cleaned_origin_context = _normalize_origin_context_text(origin_context)
    prompt_lines = [
        f"You are {display_name}.",
        f"Core concern: {_ensure_sentence(concern)}",
    ]
    if cleaned_origin_context:
        prompt_lines.append(f"Specific context you exist for here: {cleaned_origin_context}")
    prompt_lines.extend(
        [
            "Keep your focus, plans, and status updates direct and professional. Cute naming is fine; do not use animal or ecosystem metaphors to describe the actual work.",
            f'Use "{get_owner_reference()}" only in third-person durable state or summaries. Do not use that label as a direct chat salutation unless the conversation asks for it.',
            "Use the purpose below as your anchor.",
            "Stay grounded in local repo and runtime evidence, keep updates concise, and treat each conversation as one scoped thread inside your persistent Codex thread.",
            "Use the workspace, the browser, attachments, and your private workshop as ordinary working surfaces whenever they help you fulfill your purpose.",
            "Keep repeatable helpers in the workshop, keep main-workspace changes relevant and intentional, and ask naturally before irreversible public, financial, destructive, or account-security changes that are not clearly entrusted to you.",
            "",
            "## Purpose",
            _normalize_purpose_markdown(purpose_markdown)
            or _compose_summoning_purpose_markdown(
                display_name=display_name,
                purpose_summary=concern,
                brief=cleaned_brief,
                origin_context=cleaned_origin_context,
            ),
            "",
            "## Summoning Brief",
            cleaned_brief,
        ]
    )
    return "\n".join(prompt_lines)

def _creature_storage_root(slug: str) -> Path:
    return config.data_dir() / "creatures" / slug


def _creature_workshop_root(slug: str) -> Path:
    return _creature_storage_root(slug) / WORKSHOP_DIRNAME


def _creature_workshop_paths(slug: str) -> dict[str, Path]:
    root = _creature_workshop_root(slug)
    browser_root = root / WORKSHOP_BROWSER_DIRNAME
    return {
        "root": root,
        "scripts": root / WORKSHOP_SCRIPTS_DIRNAME,
        "reports": root / WORKSHOP_REPORTS_DIRNAME,
        "files": root / WORKSHOP_FILES_DIRNAME,
        "state": root / WORKSHOP_STATE_DIRNAME,
        "templates": root / WORKSHOP_TEMPLATES_DIRNAME,
        "browser": browser_root,
        "browser_profile": browser_root / WORKSHOP_BROWSER_PROFILE_DIRNAME,
        "browser_downloads": browser_root / WORKSHOP_BROWSER_DOWNLOADS_DIRNAME,
        "browser_captures": browser_root / WORKSHOP_BROWSER_CAPTURES_DIRNAME,
    }


def _ensure_workshop_note(path: Path, lines: Sequence[str]) -> None:
    if path.exists():
        return
    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def _ensure_creature_workshop(slug: str) -> dict[str, Path]:
    paths = _creature_workshop_paths(slug)
    for path in paths.values():
        path.mkdir(parents=True, exist_ok=True)
    _ensure_workshop_note(
        paths["root"] / "README.md",
        [
            "# Creature Workshop",
            "",
            "This is the creature's private working space.",
            "",
            "- `scripts/`: helper scripts for recurring work",
            "- `files/`: notes, drafts, exports, and scratch artifacts",
            "- `state/`: machine-readable cached state for repeated habits",
            "- `templates/`: reusable prompts, outlines, and text templates",
            "- `browser/`: browser profile, downloads, and captures for web work",
            "- `reports/`: habit reports written after autonomous runs",
        ],
    )
    _ensure_workshop_note(
        paths["scripts"] / "README.md",
        [
            "# Workshop Scripts",
            "",
            "Store helper scripts here when a creature wants to simplify repeated work.",
        ],
    )
    _ensure_workshop_note(
        paths["files"] / "README.md",
        [
            "# Workshop Files",
            "",
            "Use this directory for scratch files, exports, notes, and working artifacts that should stay out of the main workspace.",
        ],
    )
    _ensure_workshop_note(
        paths["state"] / "README.md",
        [
            "# Workshop State",
            "",
            "Use this directory for cached structured state that helps habits stay consistent over time.",
        ],
    )
    _ensure_workshop_note(
        paths["templates"] / "README.md",
        [
            "# Workshop Templates",
            "",
            "Store reusable templates, boilerplate, outlines, and prompts here.",
        ],
    )
    _ensure_workshop_note(
        paths["browser"] / "README.md",
        [
            "# Workshop Browser",
            "",
            "Browser-related working state lives here.",
            "",
            "- `profile/`: browser profile and any logged-in session state that exists",
            "- `downloads/`: files fetched during browser work",
            "- `captures/`: screenshots, snapshots, and browser evidence",
        ],
    )
    return paths


def _habit_report_dir(slug: str, habit_slug: str) -> Path:
    base = _ensure_creature_workshop(slug)["reports"] / (habit_slug or "general")
    base.mkdir(parents=True, exist_ok=True)
    return base


def _write_habit_report_artifact(creature: Any, habit: Mapping[str, Any], run_id: int, notes_markdown: str) -> str:
    cleaned = str(notes_markdown or "").strip()
    if not cleaned:
        return ""
    slug = str(_row_value(creature, "slug") or "")
    habit_slug = str(habit.get("slug") or "general")
    report_dir = _habit_report_dir(slug, habit_slug)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    report_path = report_dir / f"{timestamp}-run-{run_id}.md"
    report_path.write_text(cleaned.rstrip() + "\n", encoding="utf-8")
    try:
        return str(report_path.resolve().relative_to(config.data_dir().resolve()))
    except ValueError:
        return str(report_path)


def _habit_schedule_json(raw: Any) -> dict[str, Any]:
    if isinstance(raw, Mapping):
        return {str(key): value for key, value in raw.items()}
    if isinstance(raw, str) and raw.strip():
        parsed = _parse_json(raw)
        if isinstance(parsed, Mapping):
            return {str(key): value for key, value in parsed.items()}
    return {}


def _normalize_habit_slug(title: str) -> str:
    base = _slugify(title)[:48].strip("-")
    return base or uuid.uuid4().hex[:8]


def _parse_clock_minutes(value: Any) -> int | None:
    text = str(value or "").strip()
    if not re.fullmatch(r"\d{1,2}:\d{2}", text):
        return None
    hours_text, minutes_text = text.split(":", 1)
    hours = int(hours_text)
    minutes = int(minutes_text)
    if hours < 0 or hours > 23 or minutes < 0 or minutes > 59:
        return None
    return hours * 60 + minutes


def _normalize_clock_text(value: Any, *, fallback: str) -> str:
    minutes = _parse_clock_minutes(value)
    if minutes is None:
        minutes = _parse_clock_minutes(fallback) or 0
    return f"{minutes // 60:02d}:{minutes % 60:02d}"


def _clock_display_label(value: Any) -> str:
    minutes = _parse_clock_minutes(value)
    if minutes is None:
        return "unscheduled"
    dt = datetime(2000, 1, 1, minutes // 60, minutes % 60)
    hour = str(int(dt.strftime("%I")))
    return f"{hour}:{dt.strftime('%M %p')}"


def _habit_schedule_slots(schedule_json: Mapping[str, Any]) -> list[int]:
    count = max(2, min(12, int(schedule_json.get("times_per_day") or 3)))
    start = _parse_clock_minutes(schedule_json.get("window_start")) or (_parse_clock_minutes(DEFAULT_HABIT_WINDOW_START) or 360)
    end = _parse_clock_minutes(schedule_json.get("window_end")) or (_parse_clock_minutes(DEFAULT_HABIT_WINDOW_END) or 1200)
    if end <= start:
        end = start + 12 * 60
    if count == 1:
        return [start]
    span = end - start
    return [start + round(span * (index / (count - 1))) for index in range(count)]


def _normalize_habit_schedule(kind: str | None, schedule_json: Mapping[str, Any] | None = None) -> tuple[str, dict[str, Any]]:
    cleaned_kind = str(kind or HABIT_SCHEDULE_MANUAL).strip().lower().replace("-", "_")
    if cleaned_kind not in HABIT_SCHEDULE_KINDS:
        cleaned_kind = HABIT_SCHEDULE_MANUAL
    raw = dict(schedule_json or {})
    if cleaned_kind == HABIT_SCHEDULE_INTERVAL:
        return (
            cleaned_kind,
            {
                "every_minutes": max(5, int(raw.get("every_minutes") or 30)),
                "window_start": _normalize_clock_text(raw.get("window_start"), fallback=DEFAULT_HABIT_WINDOW_START),
                "window_end": _normalize_clock_text(raw.get("window_end"), fallback=DEFAULT_HABIT_WINDOW_END),
            },
        )
    if cleaned_kind == HABIT_SCHEDULE_DAILY:
        return (
            cleaned_kind,
            {
                "time": _normalize_clock_text(raw.get("time"), fallback="08:00"),
            },
        )
    if cleaned_kind == HABIT_SCHEDULE_TIMES_PER_DAY:
        return (
            cleaned_kind,
            {
                "times_per_day": max(2, min(12, int(raw.get("times_per_day") or 3))),
                "window_start": _normalize_clock_text(raw.get("window_start"), fallback="08:00"),
                "window_end": _normalize_clock_text(raw.get("window_end"), fallback="20:00"),
            },
        )
    if cleaned_kind == HABIT_SCHEDULE_AFTER_CHAT:
        try:
            after_minutes = int(raw.get("after_minutes") or DEFAULT_PONDER_DELAY_MINUTES)
        except (TypeError, ValueError):
            after_minutes = DEFAULT_PONDER_DELAY_MINUTES
        return (
            cleaned_kind,
            {
                "after_minutes": max(5, after_minutes),
            },
        )
    return (HABIT_SCHEDULE_MANUAL, {})


def _habit_schedule_summary(kind: str, schedule_json: Mapping[str, Any]) -> str:
    if kind == HABIT_SCHEDULE_INTERVAL:
        every_minutes = max(5, int(schedule_json.get("every_minutes") or 30))
        return f"Every {every_minutes} minutes between {_clock_display_label(schedule_json.get('window_start'))} and {_clock_display_label(schedule_json.get('window_end'))}"
    if kind == HABIT_SCHEDULE_DAILY:
        return f"Every day at {_clock_display_label(schedule_json.get('time'))}"
    if kind == HABIT_SCHEDULE_TIMES_PER_DAY:
        return (
            f"{max(2, int(schedule_json.get('times_per_day') or 3))} times a day between "
            f"{_clock_display_label(schedule_json.get('window_start'))} and {_clock_display_label(schedule_json.get('window_end'))}"
        )
    if kind == HABIT_SCHEDULE_AFTER_CHAT:
        delay_minutes = max(5, int(schedule_json.get("after_minutes") or DEFAULT_PONDER_DELAY_MINUTES))
        hours, minutes = divmod(delay_minutes, 60)
        if hours and minutes:
            return f"{hours}h {minutes}m after chat activity"
        if hours:
            return f"{hours} hour{'s' if hours != 1 else ''} after chat activity"
        return f"{minutes} minutes after chat activity"
    return "Manual only"

def _combine_date_and_minutes(base: datetime, minutes: int) -> datetime:
    localized = base.astimezone(timezone.utc)
    return localized.replace(hour=minutes // 60, minute=minutes % 60, second=0, microsecond=0)


def _habit_next_run_at(kind: str, schedule_json: Mapping[str, Any], *, after: datetime | None = None) -> datetime | None:
    reference = (after or datetime.now(timezone.utc)).astimezone(timezone.utc).replace(second=0, microsecond=0)
    if kind == HABIT_SCHEDULE_MANUAL:
        return None
    if kind == HABIT_SCHEDULE_AFTER_CHAT:
        if after is None:
            return None
        delay_minutes = max(5, int(schedule_json.get("after_minutes") or DEFAULT_PONDER_DELAY_MINUTES))
        return reference + timedelta(minutes=delay_minutes)
    if kind == HABIT_SCHEDULE_DAILY:
        target = _combine_date_and_minutes(reference, _parse_clock_minutes(schedule_json.get("time")) or 8 * 60)
        if target <= reference:
            target = target + timedelta(days=1)
        return target
    if kind == HABIT_SCHEDULE_TIMES_PER_DAY:
        slots = _habit_schedule_slots(schedule_json)
        for slot in slots:
            candidate = _combine_date_and_minutes(reference, slot)
            if candidate > reference:
                return candidate
        return _combine_date_and_minutes(reference + timedelta(days=1), slots[0])
    every_minutes = max(5, int(schedule_json.get("every_minutes") or 30))
    start_minutes = _parse_clock_minutes(schedule_json.get("window_start")) or (_parse_clock_minutes(DEFAULT_HABIT_WINDOW_START) or 360)
    end_minutes = _parse_clock_minutes(schedule_json.get("window_end")) or (_parse_clock_minutes(DEFAULT_HABIT_WINDOW_END) or 1200)
    if end_minutes <= start_minutes:
        end_minutes = start_minutes + 12 * 60
    day_start = _combine_date_and_minutes(reference, start_minutes)
    day_end = _combine_date_and_minutes(reference, end_minutes)
    if reference < day_start:
        return day_start
    candidate = reference + timedelta(minutes=every_minutes)
    if candidate <= day_end:
        return candidate
    return _combine_date_and_minutes(reference + timedelta(days=1), start_minutes)


def _decorate_habit(row: Any) -> dict[str, Any]:
    data = _row_to_dict(row) or {}
    schedule_json = _habit_schedule_json(data.get("schedule_json"))
    schedule_kind, normalized_schedule = _normalize_habit_schedule(str(data.get("schedule_kind") or ""), schedule_json)
    data["schedule_kind"] = schedule_kind
    data["schedule_json"] = normalized_schedule
    data["schedule_summary"] = _habit_schedule_summary(schedule_kind, normalized_schedule)
    data["enabled"] = bool(data.get("enabled"))
    data["next_run_at_display"] = _format_timestamp_display(data.get("next_run_at"))
    data["last_run_at_display"] = _format_timestamp_display(data.get("last_run_at"))
    data["last_run_at_relative_display"] = _format_relative_time_display(data.get("last_run_at"))
    data["workshop_paths"] = {
        key: str(path)
        for key, path in _creature_workshop_paths(str(data.get("creature_slug") or data.get("slug") or "")).items()
    }
    return data

def _creature_next_due_habit_at(creature_id: int) -> datetime | None:
    habits = [_decorate_habit(row) for row in storage.list_habits(creature_id, include_disabled=False, limit=200)]
    candidates: list[datetime] = []
    for habit in habits:
        next_run = storage.from_iso(str(habit.get("next_run_at") or ""))
        if next_run is not None:
            candidates.append(next_run)
    return min(candidates) if candidates else None


def _state_surface_content(creature: Any, key: str) -> str:
    return storage.get_state_surface_content(int(creature["id"]), key)


def _doc_storage_location(key: str) -> str:
    return str(STATE_SURFACE_SPECS.get(key, {}).get("source") or "SQLite / creature-os")


def _origin_context_prompt_block(creature: Any) -> str:
    lines = _generated_purpose_origin_lines(creature)
    if not lines:
        return ""
    return "\n".join(["Origin context for why this creature exists here:", *lines])


def _trim_for_prompt(text: str, *, limit: int = 2200) -> str:
    cleaned = text.strip()
    if len(cleaned) <= limit:
        return cleaned
    return f"{cleaned[: limit - 3].rstrip()}..."


def _format_timestamp_display(raw: Any) -> str:
    text = str(raw or "").strip()
    if not text:
        return "pending"
    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return text
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    local = parsed.astimezone(get_display_timezone())
    hour = str(int(local.strftime("%I")))
    return (
        f"{local.strftime('%a, %b')} {local.day}, {local.year} "
        f"at {hour}:{local.strftime('%M %p %Z')}"
    )


def _format_timestamp_compact_display(raw: Any) -> str:
    text = str(raw or "").strip()
    if not text:
        return "pending"
    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return text
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    local = parsed.astimezone(get_display_timezone())
    hour = str(int(local.strftime("%I")))
    return f"{local.strftime('%b')} {local.day}, {local.year} {hour}:{local.strftime('%M %p')}"


def _format_relative_time_display(raw: Any) -> str:
    text = str(raw or "").strip()
    if not text:
        return "Not yet"
    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return text
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    delta_seconds = max(0, int((datetime.now(timezone.utc) - parsed.astimezone(timezone.utc)).total_seconds()))
    if delta_seconds < 60:
        return "just now"
    if delta_seconds < 3600:
        return f"{max(1, delta_seconds // 60)}m ago"
    if delta_seconds < 86400:
        return f"{max(1, delta_seconds // 3600)}h ago"
    return f"{max(1, delta_seconds // 86400)}d ago"


def _write_doc_text(creature: Any, key: str, content: str, *, expected_revision: int | None = None) -> int:
    normalized = content.rstrip() + "\n"
    current_content = _state_surface_content(creature, key)
    current_revision = storage.get_state_surface_revision(int(creature["id"]), key)
    if current_content == normalized:
        return current_revision
    revision_to_expect = current_revision if expected_revision is None else expected_revision
    try:
        next_revision = storage.update_state_surface(
            int(creature["id"]),
            key,
            content=normalized,
            expected_revision=revision_to_expect,
        )
    except RuntimeError:
        if expected_revision is not None:
            raise
        latest_revision = storage.get_state_surface_revision(int(creature["id"]), key)
        next_revision = storage.update_state_surface(
            int(creature["id"]),
            key,
            content=normalized,
            expected_revision=latest_revision,
        )
    return next_revision


def _memory_kind_label(kind: str) -> str:
    text = str(kind or "note").replace("_", " ").strip()
    return text[:1].upper() + text[1:] if text else "Note"


def _memory_origin_label(origin: str) -> str:
    labels = {
        "user_stated": "user stated",
        "learned_routine": "learned routine",
        "creature_inferred": "creature inferred",
        "system_seeded": "system seeded",
    }
    return labels.get(str(origin or "").strip(), "unknown")


def _memory_stability_label(stability: str) -> str:
    labels = {
        "confirmed": "confirmed",
        "emerging": "emerging",
        "working": "working",
        "seeded": "seeded",
    }
    return labels.get(str(stability or "").strip(), "")


def _normalize_memory_kind(value: str | None) -> str:
    kind = str(value or "note").strip().lower().replace(" ", "_")
    return kind if kind in MEMORY_KIND_ORDER else "note"


def _normalize_memory_text(value: str) -> str:
    return " ".join(str(value or "").strip().split()).casefold()


def _memory_source_timestamp(*, source_message_id: int | None = None, source_run_id: int | None = None) -> str:
    if source_message_id:
        message = storage.get_message(int(source_message_id))
        if message is not None:
            return str(message["created_at"] or "").strip()
    if source_run_id:
        run = storage.get_run(int(source_run_id))
        if run is not None:
            return str(run["finished_at"] or run["started_at"] or "").strip()
    return ""


def _memory_origin_value(
    *,
    kind: str,
    actor_type: str,
    source_message_id: int | None,
    source_run_id: int | None,
    metadata: Mapping[str, Any] | None = None,
) -> str:
    if metadata and str(metadata.get("origin") or "").strip():
        return str(metadata.get("origin") or "").strip()
    if str(actor_type or "").strip().lower() == "system":
        return "system_seeded"
    if kind == "routine":
        return "learned_routine" if source_run_id else "creature_inferred"
    if kind in MEMORY_USER_KIND_ORDER and source_message_id:
        return "user_stated"
    return "creature_inferred"


def _memory_stability_value(
    *,
    kind: str,
    origin: str,
    metadata: Mapping[str, Any] | None = None,
    existing: Mapping[str, Any] | None = None,
) -> str:
    if metadata and str(metadata.get("stability") or "").strip():
        return str(metadata.get("stability") or "").strip()
    existing_stability = str((existing or {}).get("stability") or "").strip()
    if kind == "routine":
        return "confirmed" if existing else "emerging"
    if origin == "user_stated":
        return "confirmed"
    if origin == "system_seeded":
        return "seeded"
    return "working"


def _prepare_memory_metadata(
    *,
    kind: str,
    actor_type: str,
    source_message_id: int | None = None,
    source_run_id: int | None = None,
    metadata: Mapping[str, Any] | None = None,
    existing: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    if existing:
        merged.update({str(key): value for key, value in existing.items() if value not in (None, "")})
    if metadata:
        merged.update({str(key): value for key, value in metadata.items() if value not in (None, "")})
    source_at = str(merged.get("source_at") or "").strip() or _memory_source_timestamp(
        source_message_id=source_message_id,
        source_run_id=source_run_id,
    )
    if source_at:
        merged["source_at"] = source_at
    if kind in MEMORY_USER_KIND_ORDER and source_at and not str(merged.get("effective_at") or "").strip():
        merged["effective_at"] = source_at
    origin = _memory_origin_value(
        kind=kind,
        actor_type=actor_type,
        source_message_id=source_message_id,
        source_run_id=source_run_id,
        metadata=merged,
    )
    merged["origin"] = origin
    merged["stability"] = _memory_stability_value(kind=kind, origin=origin, metadata=merged, existing=existing)
    merged["last_confirmed_at"] = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    return merged


def _memory_record_prompt_line(record: Mapping[str, Any]) -> str:
    metadata = record.get("metadata") if isinstance(record.get("metadata"), Mapping) else {}
    parts = [f"[{record['kind']}] {record['body']}"]
    origin = str((metadata or {}).get("origin") or "").strip()
    stability = str(record.get("stability") or (metadata or {}).get("stability") or "").strip()
    extras: list[str] = []
    if origin:
        extras.append(f"origin={_memory_origin_label(origin)}")
    if stability:
        extras.append(f"stability={_memory_stability_label(stability)}")
    effective_at = str((metadata or {}).get("effective_at") or "").strip()
    if effective_at:
        extras.append(f"effective={_format_timestamp_compact_display(effective_at)}")
    last_confirmed_at = str((metadata or {}).get("last_confirmed_at") or "").strip()
    if last_confirmed_at:
        extras.append(f"confirmed={_format_timestamp_compact_display(last_confirmed_at)}")
    if extras:
        parts.append(f"({' · '.join(extras)})")
    return " ".join(parts)


def _memory_prompt_block(creature: Any, *, compact: bool = False, context_texts: Sequence[str] = ()) -> str:
    active_records = _select_memory_records_for_prompt(creature, context_texts=context_texts, compact=compact)
    user_records = [record for record in active_records if record["kind"] in MEMORY_USER_KIND_ORDER]
    routine_records = [record for record in active_records if record["kind"] in MEMORY_ROUTINE_KIND_ORDER]
    context_records = [record for record in active_records if record["kind"] in MEMORY_CONTEXT_KIND_ORDER]
    lines = ["Memory:"]
    if user_records:
        lines.append("User instructions & preferences:")
        for record in user_records[: (5 if compact else 10)]:
            lines.append(f"- {_memory_record_prompt_line(record)}")
    if routine_records:
        lines.append("Learned routines:")
        for record in routine_records[: (3 if compact else 8)]:
            lines.append(f"- {_memory_record_prompt_line(record)}")
    if context_records and not compact:
        lines.append("Other context:")
        for record in context_records[:4]:
            lines.append(f"- {_memory_record_prompt_line(record)}")
    if len(lines) == 1:
        lines.append("- None yet.")
    return "\n".join(lines)


def _worklist_prompt_block(creature: Any, *, compact: bool = False) -> str:
    agenda_items = _agenda_items_state(int(creature["id"]))
    backlog_items = _backlog_items_state(int(creature["id"]))
    lines = ["Worklist:"]
    if agenda_items:
        lines.append("Active:")
        for item in agenda_items[: (4 if compact else 8)]:
            detail = f" - {item['details']}" if item["details"] else ""
            lines.append(f"- [{item['priority']}] {item['title']}{detail}")
    if backlog_items:
        lines.append("Parked:")
        for item in backlog_items[: (3 if compact else 6)]:
            lines.append(f"- {item}")
    if len(lines) == 1:
        lines.append("- No active or parked work items.")
    return "\n".join(lines)


def _latest_user_message_id(conversation_id: int | None) -> int | None:
    if not conversation_id:
        return None
    messages = storage.recent_messages(int(conversation_id), limit=12)
    for row in reversed(messages):
        if str(row["role"] or "").strip().lower() == "user":
            return int(row["id"])
    return None


def _matching_active_memory_record(creature: Any, *, kind: str, body: str) -> dict[str, Any] | None:
    normalized_kind = _normalize_memory_kind(kind)
    normalized_body = _normalize_memory_text(body)
    if not normalized_body:
        return None
    active_records, _ = _memory_records_by_status(int(creature["id"]))
    for record in active_records:
        if record["kind"] == normalized_kind and _normalize_memory_text(str(record["body"] or "")) == normalized_body:
            return record
    return None


def _refresh_existing_memory_record(
    creature: Any,
    *,
    record: Mapping[str, Any],
    actor_type: str,
    reason: str = "",
    source_message_id: int | None = None,
    source_run_id: int | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    refreshed_metadata = _prepare_memory_metadata(
        kind=str(record["kind"]),
        actor_type=actor_type,
        source_message_id=source_message_id,
        source_run_id=source_run_id,
        metadata=metadata,
        existing=record.get("metadata") if isinstance(record.get("metadata"), Mapping) else None,
    )
    row = storage.update_memory_record_metadata(int(record["id"]), refreshed_metadata)
    storage.create_memory_event(
        int(creature["id"]),
        record_id=int(record["id"]),
        actor_type=actor_type,
        action="remember",
        reason=reason or "Reconfirmed existing memory.",
        metadata=refreshed_metadata,
    )
    _refresh_memory_doc(creature)
    return _decorate_memory_record(row) if row is not None else dict(record)


def _routine_memory_allowed(*, source_run_id: int | None) -> bool:
    if not source_run_id:
        return False
    run = storage.get_run(int(source_run_id))
    return _run_scope_value(run) == RUN_SCOPE_ACTIVITY


def _memory_reference_timestamp(record: Mapping[str, Any]) -> datetime | None:
    metadata = record.get("metadata") if isinstance(record.get("metadata"), Mapping) else {}
    for key in ("last_confirmed_at", "effective_at", "source_at"):
        raw = str((metadata or {}).get(key) or "").strip()
        if not raw:
            continue
        parsed = storage.from_iso(raw)
        if parsed is not None:
            return parsed
    raw_updated = str(record.get("updated_at") or "").strip()
    return storage.from_iso(raw_updated)


def _effective_memory_stability(record: Mapping[str, Any], *, stale_days: int = MEMORY_STALE_DAYS) -> str:
    metadata = record.get("metadata") if isinstance(record.get("metadata"), Mapping) else {}
    current = str((metadata or {}).get("stability") or "").strip() or "working"
    if str(record.get("status") or "").strip().lower() not in ACTIVE_MEMORY_STATUSES:
        return current
    if current == "seeded":
        return current
    reference_at = _memory_reference_timestamp(record)
    if reference_at is None:
        return current
    if reference_at.tzinfo is None:
        reference_at = reference_at.replace(tzinfo=timezone.utc)
    threshold = datetime.now(timezone.utc) - timedelta(days=stale_days)
    if reference_at <= threshold:
        return "stale"
    return current


def _memory_context_texts(
    creature: Any,
    *,
    conversation: Any | None = None,
    trigger_type: str = "",
    focus_hint: str = "",
) -> list[str]:
    texts: list[str] = []
    for value in (
        _row_value(creature, "purpose_summary"),
        _row_value(creature, "concern"),
        focus_hint,
        trigger_type,
    ):
        cleaned = " ".join(str(value or "").strip().split())
        if cleaned:
            texts.append(cleaned)
    for item in _agenda_items_state(int(creature["id"]))[:5]:
        texts.append(" ".join(str(item.get("title") or "").strip().split()))
        if str(item.get("details") or "").strip():
            texts.append(" ".join(str(item.get("details") or "").strip().split()))
    if conversation is not None:
        texts.append(" ".join(str(_row_value(conversation, "title") or "").strip().split()))
        for row in storage.recent_messages(int(conversation["id"]), limit=6):
            body = " ".join(str(row["body"] or "").strip().split())
            if body:
                texts.append(body)
        source_run_id = int(_row_value(conversation, "source_run_id") or 0)
        if source_run_id:
            run = storage.get_run(source_run_id)
            if run is not None:
                texts.append(" ".join(str(_row_value(run, "message_text") or "").strip().split()))
                metadata = _parse_json(str(_row_value(run, "metadata_json") or ""))
                activity_note = " ".join(str(metadata.get("activity_note") or "").strip().split())
                if activity_note:
                    texts.append(activity_note)
    return [text for text in texts if text]


def _memory_relevance_score(record: Mapping[str, Any], *, context_tokens: set[str], compact: bool) -> int:
    kind = str(record.get("kind") or "")
    origin = str(record.get("origin") or "")
    stability = str(record.get("stability") or "")
    base = {
        "instruction": 80,
        "constraint": 76,
        "decision": 68,
        "preference": 60,
        "routine": 58 if stability == "confirmed" else 48,
        "context": 26,
        "note": 18,
    }.get(kind, 10)
    if origin == "user_stated":
        base += 8
    if stability == "stale":
        base -= 18
    body_tokens = _name_words(str(record.get("body") or ""))
    overlap = len(body_tokens.intersection(context_tokens))
    score = base + overlap * 12
    if compact and overlap == 0 and kind in MEMORY_CONTEXT_KIND_ORDER:
        score -= 30
    return score


def _select_memory_records_for_prompt(
    creature: Any,
    *,
    context_texts: Sequence[str],
    compact: bool,
) -> list[dict[str, Any]]:
    active_records, _ = _memory_records_by_status(int(creature["id"]))
    context_tokens: set[str] = set()
    for text in context_texts:
        context_tokens.update(_name_words(text))
    for text in (
        _row_value(creature, "purpose_summary"),
        _row_value(creature, "concern"),
    ):
        context_tokens.update(_name_words(str(text or "")))
    scored: list[tuple[int, dict[str, Any]]] = []
    for record in active_records:
        decorated = dict(record)
        decorated["stability"] = _effective_memory_stability(record)
        if compact and decorated["kind"] == "routine" and decorated["stability"] == "stale":
            continue
        scored.append((_memory_relevance_score(decorated, context_tokens=context_tokens, compact=compact), decorated))
    scored.sort(
        key=lambda item: (
            item[0],
            str(item[1].get("updated_at") or ""),
            int(item[1].get("id") or 0),
        ),
        reverse=True,
    )
    selected: list[dict[str, Any]] = []
    user_limit = 4 if compact else 8
    routine_limit = 2 if compact else 5
    context_limit = 0 if compact else 3
    counts = {"user": 0, "routine": 0, "context": 0}
    for score, record in scored:
        kind = str(record["kind"])
        if kind in MEMORY_USER_KIND_ORDER:
            if counts["user"] >= user_limit:
                continue
            selected.append(record)
            counts["user"] += 1
            continue
        if kind in MEMORY_ROUTINE_KIND_ORDER:
            if compact and score < 55:
                continue
            if counts["routine"] >= routine_limit:
                continue
            selected.append(record)
            counts["routine"] += 1
            continue
        if counts["context"] >= context_limit:
            continue
        if score < 30:
            continue
        selected.append(record)
        counts["context"] += 1
    return selected



def _decorate_memory_record(row: Any) -> dict[str, Any]:
    record = dict(row)
    record["kind"] = _normalize_memory_kind(record.get("kind"))
    record["kind_label"] = _memory_kind_label(record["kind"])
    record["status"] = str(record.get("status") or "active").lower()
    record["status_label"] = record["status"].replace("_", " ")
    record["metadata"] = _parse_json(str(record.get("metadata_json") or ""))
    record["origin"] = str(record["metadata"].get("origin") or "").strip()
    record["origin_label"] = _memory_origin_label(record["origin"]) if record["origin"] else ""
    record["stability"] = _effective_memory_stability(record)
    record["stability_label"] = _memory_stability_label(record["stability"]) if record["stability"] else ""
    record["source_at_display"] = _format_timestamp_display(record["metadata"].get("source_at"))
    record["effective_at_display"] = _format_timestamp_display(record["metadata"].get("effective_at"))
    record["last_confirmed_at_display"] = _format_timestamp_display(record["metadata"].get("last_confirmed_at"))
    record["updated_at_display"] = _format_timestamp_display(record.get("updated_at"))
    record["created_at_display"] = _format_timestamp_display(record.get("created_at"))
    return record


def _decorate_memory_event(row: Any) -> dict[str, Any]:
    event = dict(row)
    event["metadata"] = _parse_json(str(event.get("metadata_json") or ""))
    event["created_at_display"] = _format_timestamp_display(event.get("created_at"))
    event["action_label"] = str(event.get("action") or "").replace("_", " ")
    return event


def _memory_records(creature_id: int, *, include_inactive: bool = True) -> list[dict[str, Any]]:
    return [_decorate_memory_record(row) for row in storage.list_memory_records(creature_id, include_inactive=include_inactive)]


def _memory_records_by_status(creature_id: int) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    records = _memory_records(creature_id, include_inactive=True)
    active = [record for record in records if record["status"] in ACTIVE_MEMORY_STATUSES]
    inactive = [record for record in records if record["status"] in INACTIVE_MEMORY_STATUSES]
    active.sort(key=lambda item: (MEMORY_KIND_ORDER.get(item["kind"], 99), item["updated_at"] or "", item["id"]), reverse=False)
    inactive.sort(key=lambda item: (item["updated_at"] or "", item["id"]), reverse=True)
    return active, inactive


def _render_memory_markdown(creature: Any) -> str:
    active_records, inactive_records = _memory_records_by_status(int(creature["id"]))
    lines = [f"# {creature['display_name']} memory", ""]
    if not active_records:
        lines.extend(["No active memory records yet.", ""])
    else:
        grouped: dict[str, list[dict[str, Any]]] = {}
        for record in active_records:
            grouped.setdefault(record["kind"], []).append(record)
        sections = (
            ("User instructions & preferences", MEMORY_USER_KIND_ORDER, True),
            ("Learned routines", MEMORY_ROUTINE_KIND_ORDER, False),
            ("Other context", MEMORY_CONTEXT_KIND_ORDER, True),
        )
        for heading, kinds, show_kind_label in sections:
            section_records = [record for kind in kinds for record in grouped.get(kind, ())]
            if not section_records:
                continue
            lines.append(f"## {heading}")
            for record in section_records:
                prefix = f"[{record['kind_label']}] " if show_kind_label else ""
                lines.append(f"- ({record['id']}) {prefix}{record['body']}")
                detail_parts: list[str] = []
                if record.get("origin_label"):
                    detail_parts.append(f"origin: {record['origin_label']}")
                if record.get("stability_label"):
                    detail_parts.append(f"stability: {record['stability_label']}")
                if record.get("effective_at_display"):
                    detail_parts.append(f"effective: {record['effective_at_display']}")
                if record.get("last_confirmed_at_display"):
                    detail_parts.append(f"confirmed: {record['last_confirmed_at_display']}")
                if detail_parts:
                    lines.append(f"  - {' · '.join(detail_parts)}")
            lines.append("")
    lines.extend(
        [
            "## Notes",
            "- This file is host-rendered from active structured memory records.",
            "- Habits and chats update this view through structured memory actions, not direct file edits.",
            "- Learned routines should capture recurring triggers, steps, and success conditions when a creature has found a stable way to repeat work.",
        ]
    )
    if inactive_records:
        lines.extend(["", f"Inactive records retained in audit history: {len(inactive_records)}"])
    lines.append("")
    return "\n".join(lines)


def _refresh_memory_doc(creature: Any) -> None:
    return None


def _create_memory_record(
    creature: Any,
    *,
    kind: str,
    body: str,
    actor_type: str,
    reason: str = "",
    source_message_id: int | None = None,
    source_run_id: int | None = None,
    previous_record_id: int | None = None,
    event_action: str = "remember",
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_kind = _normalize_memory_kind(kind)
    final_metadata = _prepare_memory_metadata(
        kind=normalized_kind,
        actor_type=actor_type,
        source_message_id=source_message_id,
        source_run_id=source_run_id,
        metadata=metadata,
    )
    row = storage.create_memory_record(
        int(creature["id"]),
        kind=normalized_kind,
        body=body.strip(),
        actor_type=actor_type,
        reason=reason,
        source_message_id=source_message_id,
        source_run_id=source_run_id,
        previous_record_id=previous_record_id,
        metadata=final_metadata,
        event_action=event_action,
    )
    _refresh_memory_doc(creature)
    return _decorate_memory_record(row)


def _update_memory_status(
    creature: Any,
    *,
    record_id: int,
    status: str,
    actor_type: str,
    action: str,
    reason: str = "",
    superseded_by_id: int | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    row = storage.update_memory_record_status(
        record_id,
        status=status,
        actor_type=actor_type,
        action=action,
        reason=reason,
        superseded_by_id=superseded_by_id,
        metadata=metadata,
    )
    if row is None:
        return None
    _refresh_memory_doc(creature)
    return _decorate_memory_record(row)


def _default_purpose_doc(creature: Any) -> str:
    origin_lines = _generated_purpose_origin_lines(creature)
    concern = " ".join(str(_row_value(creature, "concern") or "").strip().split())
    return "\n".join(
        [
            f"# {creature['display_name']} purpose",
            "",
            f"- Name: {creature['display_name']}",
            *([f"- Purpose: {concern}"] if concern else []),
            f"- Habits: {_creature_habit_summary(_creature_habits_state(creature))}",
            "",
            "## Why you exist",
            *(origin_lines or ["- No specific summon context has been recorded yet."]),
            "",
            "## Commitments",
            "- Stay grounded in local repo evidence.",
            "- Keep owner-facing updates crisp and actionable.",
            "- Treat each chat as its own scoped thread with its own Codex thread.",
            "- Keep autonomous activity separate from operator chats.",
            "- Durable memory and work state matter more than thread residue.",
            "- Use the workspace, the browser, and the workshop as ordinary working surfaces when they help you serve your purpose well.",
        ]
    )


def _looks_like_host_rendered_purpose_doc(creature: Any, content: str) -> bool:
    text = str(content or "").strip()
    if not text:
        return False
    display_name = str(_row_value(creature, "display_name") or "").strip()
    if not text.startswith(f"# {display_name} purpose"):
        return False
    required_lines = (
        "- Stay grounded in local repo evidence.",
        "- Keep owner-facing updates crisp and actionable.",
        "- Treat each chat as its own scoped thread with its own Codex thread.",
        "- Keep autonomous activity separate from operator chats.",
        "- Durable memory and work state matter more than thread residue.",
    )
    return all(line in text for line in required_lines)


def _agenda_items_state(creature_id: int) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for row in storage.list_agenda_items(creature_id):
        items.append(
            {
                "id": int(row["id"]),
                "title": str(row["title"] or ""),
                "priority": str(row["priority"] or "medium"),
                "details": str(row["details"] or ""),
                "spawn_conversation": bool(row["spawn_conversation"]),
                "ordinal": int(row["ordinal"] or 0),
            }
        )
    return items


def _backlog_items_state(creature_id: int) -> list[str]:
    return [str(row["body"] or "") for row in storage.list_backlog_items(creature_id)]


def _refresh_purpose_doc(creature: Any, *, force_generated_update: bool = False) -> None:
    current = _state_surface_content(creature, PURPOSE_DOC_KEY).strip()
    if (
        not current
        or (force_generated_update and _looks_like_host_rendered_purpose_doc(creature, current))
    ):
        content = _default_purpose_doc(creature)
    else:
        content = current
    _write_doc_text(creature, PURPOSE_DOC_KEY, content)

def _refresh_agenda_doc(creature: Any) -> None:
    return None


def _refresh_backlog_doc(creature: Any) -> None:
    return None


def _reasoning_effort_label(effort: str) -> str:
    labels = {
        "low": "low",
        "medium": "medium",
        "high": "high",
        "xhigh": "extra high",
    }
    return labels.get(effort, effort)


def _normalize_reasoning_effort_value(value: str | None, *, allow_blank: bool = False) -> str:
    cleaned = str(value or "").strip().lower().replace("_", "")
    if not cleaned:
        return "" if allow_blank else config.creature_reasoning_effort()
    aliases = {
        "extrahigh": "xhigh",
        "extra-high": "xhigh",
        "veryhigh": "xhigh",
    }
    cleaned = aliases.get(cleaned, cleaned)
    if cleaned in REASONING_EFFORT_VALUES:
        return cleaned
    return "" if allow_blank else config.creature_reasoning_effort()


def _normalize_model_value(value: str | None, *, allow_blank: bool = False) -> str:
    cleaned = " ".join(str(value or "").strip().split())
    if not cleaned:
        return "" if allow_blank else config.creature_model()
    return cleaned[:MAX_THINKING_MODEL_CHARS]


def get_default_creature_model() -> str:
    return _normalize_model_value(storage.get_meta(DEFAULT_CREATURE_MODEL_KEY))


def get_default_creature_reasoning_effort() -> str:
    return _normalize_reasoning_effort_value(storage.get_meta(DEFAULT_CREATURE_REASONING_EFFORT_KEY))


def _thinking_effort_choices() -> list[dict[str, str]]:
    return [
        {"value": value, "label": _reasoning_effort_label(value).capitalize()}
        for value in REASONING_EFFORT_VALUES
    ]


def _thinking_model_label(value: str) -> str:
    normalized = _normalize_model_value(value, allow_blank=True)
    if not normalized:
        return ""
    for item in _codex_model_choices():
        if str(item.get("value") or "").strip() == normalized:
            return str(item.get("label") or normalized).strip() or normalized
    return _codex_model_label(normalized)


def _thinking_model_choices(*values: str) -> list[dict[str, str]]:
    choices = _codex_model_choices()
    seen = {str(item["value"]).strip() for item in choices}
    extras = [
        _normalize_model_value(value, allow_blank=True)
        for value in values
        if _normalize_model_value(value, allow_blank=True)
    ]
    for value in extras:
        if value in seen:
            continue
        seen.add(value)
        choices.insert(0, {"value": value, "label": value})
    return choices


def _creature_model(creature: Any | None = None) -> str:
    if creature is not None:
        override = _normalize_model_value(_row_value(creature, "model_override"), allow_blank=True)
        if override:
            return override
    return get_default_creature_model()


def _creature_reasoning_effort(creature: Any | None = None) -> str:
    if creature is not None:
        override = _normalize_reasoning_effort_value(_row_value(creature, "reasoning_effort_override"), allow_blank=True)
        if override:
            return override
    return get_default_creature_reasoning_effort()


def _thinking_state(creature: Any | None = None, conversation: Any | None = None) -> dict[str, Any]:
    global_model = get_default_creature_model()
    global_effort = get_default_creature_reasoning_effort()
    creature_model_override = _normalize_model_value(_row_value(creature, "model_override"), allow_blank=True) if creature is not None else ""
    creature_effort_override = _normalize_reasoning_effort_value(
        _row_value(creature, "reasoning_effort_override"),
        allow_blank=True,
    ) if creature is not None else ""
    conversation_model_override = _normalize_model_value(_row_value(conversation, "model_override"), allow_blank=True) if conversation is not None else ""
    conversation_effort_override = _normalize_reasoning_effort_value(
        _row_value(conversation, "reasoning_effort_override"),
        allow_blank=True,
    ) if conversation is not None else ""
    effective_model = conversation_model_override or creature_model_override or global_model
    effective_effort = conversation_effort_override or creature_effort_override or global_effort
    return {
        "model": effective_model,
        "model_label": _thinking_model_label(effective_model),
        "reasoning_effort": effective_effort,
        "reasoning_effort_label": _reasoning_effort_label(effective_effort).capitalize(),
        "model_override": creature_model_override,
        "reasoning_effort_override": creature_effort_override,
        "conversation_model_override": conversation_model_override,
        "conversation_reasoning_effort_override": conversation_effort_override,
        "uses_global_model": not creature_model_override,
        "uses_global_reasoning_effort": not creature_effort_override,
        "uses_creature_model": not conversation_model_override,
        "uses_creature_reasoning_effort": not conversation_effort_override,
        "global_model": global_model,
        "global_model_label": _thinking_model_label(global_model),
        "global_reasoning_effort": global_effort,
        "global_reasoning_effort_label": _reasoning_effort_label(global_effort).capitalize(),
        "model_choices": _thinking_model_choices(global_model, creature_model_override, conversation_model_override, effective_model),
        "choices": _thinking_effort_choices(),
    }


def _normalize_owner_mode(value: Any) -> str:
    return storage.normalize_owner_mode(str(value or ""))


def _owner_mode_label(value: Any) -> str:
    return "Implement" if _normalize_owner_mode(value) == "implement" else "Analyze"


def _sandbox_for_owner_mode(value: Any) -> str:
    return "workspace-write" if _normalize_owner_mode(value) == "implement" else "read-only"


def _allow_code_changes_for_owner_mode(value: Any) -> bool:
    return _normalize_owner_mode(value) == "implement"


def _decorate_conversation(row: Any) -> dict[str, Any]:
    data = _row_to_dict(row) or {}
    owner_mode = _normalize_owner_mode(data.get("owner_mode"))
    data["owner_mode"] = owner_mode
    data["owner_mode_label"] = _owner_mode_label(owner_mode)
    data["sandbox_mode"] = _sandbox_for_owner_mode(owner_mode)
    data["needs_reply"] = bool(data.get("needs_reply"))
    data["created_at_display"] = _format_timestamp_display(data.get("created_at"))
    data["updated_at_display"] = _format_timestamp_display(data.get("updated_at") or data.get("last_message_at"))
    data["last_message_at_display"] = _format_timestamp_display(data.get("last_message_at"))
    creature = storage.get_creature(int(data["creature_id"])) if data.get("creature_id") is not None else None
    data["thinking"] = _thinking_state(creature, data if creature is not None else None)
    return data


def canonical_creature_slug(slug: str | None) -> str | None:
    if slug is None:
        return None
    return slug


def _original_brief_for_creature(creature_id: int) -> str:
    for row in storage.list_memory_records(creature_id):
        body = str(row["body"] or "").strip()
        if body.startswith("Original brief: "):
            return body[len("Original brief: ") :].strip()
    return ""


def _summoning_context_memory_text(creature_id: int, *, context_type: str) -> str:
    matches = [
        record
        for record in _memory_records(creature_id, include_inactive=True)
        if str((record.get("metadata") or {}).get("source") or "").strip() == "summoning"
        and str((record.get("metadata") or {}).get("context_type") or "").strip() == context_type
    ]
    matches.sort(
        key=lambda record: (
            str(record.get("updated_at") or ""),
            int(record.get("id") or 0),
        ),
        reverse=True,
    )
    for record in matches:
        body = str(record.get("body") or "").strip()
        if context_type == "origin_context" and body.startswith("Origin context: "):
            return body[len("Origin context: ") :].strip()
        if context_type == "opening_question" and body.startswith("Suggested opening question: "):
            return body[len("Suggested opening question: ") :].strip()
        return body
    return ""


def _origin_context_for_creature(creature: Any) -> str:
    explicit = _normalize_origin_context_text(
        _summoning_context_memory_text(int(creature["id"]), context_type="origin_context")
    )
    if explicit:
        return explicit
    return _normalize_origin_context_text(_original_brief_for_creature(int(creature["id"])))


def _opening_question_for_creature(creature: Any) -> str:
    return _normalize_intro_question_text(
        _summoning_context_memory_text(int(creature["id"]), context_type="opening_question")
    )


def _generated_purpose_role_line(creature: Any) -> str:
    return _ensure_sentence(str(_row_value(creature, "purpose_summary") or _row_value(creature, "concern") or "").strip())


def _generated_purpose_origin_lines(creature: Any) -> list[str]:
    origin_context = _ensure_sentence(_origin_context_for_creature(creature))
    brief = _ensure_sentence(_original_brief_for_creature(int(creature["id"])))
    role_line = _generated_purpose_role_line(creature)
    lines: list[str] = []
    if origin_context:
        lines.append(f"- Summoned from this context: {origin_context}")
    elif brief:
        lines.append(f"- Summoned from this context: {brief}")
    if brief and (not origin_context or brief.lower() != origin_context.lower()):
        lines.append(f"- Summoning brief: {brief}")
    if role_line and (not brief or role_line.lower() != brief.lower()):
        lines.append(f"- Specific role here: {role_line}")
    if lines:
        lines.append("- When in doubt, stay aligned with this origin context instead of drifting into generic helper behavior.")
    return lines


def _refresh_intro_message(creature: Any) -> None:
    intro_run = _latest_intro_run(int(creature["id"]))
    if intro_run is None:
        return
    intro_metadata = _parse_json(str(_row_value(intro_run, "metadata_json") or ""))
    updated_body = _introduction_message(
        creature,
        concern=str(_row_value(creature, "purpose_summary") or _row_value(creature, "concern") or "").strip(),
        first_impression=str(intro_metadata.get("message") or _row_value(intro_run, "summary") or "").strip(),
        naming_alternates=[],
        used_explicit_name=True,
        intro_context={"descriptor": ""},
    )
    intro_chat = None
    intro_chat = storage.find_conversation_by_source_run(int(intro_run["id"]))
    if intro_chat is None:
        intro_chat = storage.find_conversation_by_title(int(creature["id"]), INTRODUCTION_CHAT_TITLE)
    if intro_chat is None:
        storage.update_run_message_text(int(intro_run["id"]), updated_body)
        return
    messages = storage.list_messages(int(intro_chat["id"]), limit=20)
    kept_intro_message_id = 0
    for message in messages:
        if str(message["role"] or "") != "creature":
            continue
        body = str(message["body"] or "")
        is_intro_style = body.startswith("I'm ")
        if body == updated_body or is_intro_style:
            if kept_intro_message_id:
                storage.delete_message(int(message["id"]))
                continue
            storage.update_message_body(int(message["id"]), updated_body)
            kept_intro_message_id = int(message["id"])
    if kept_intro_message_id:
        metadata = _parse_json(str(_row_value(storage.get_message(kept_intro_message_id), "metadata_json") or ""))
        if metadata.pop("typewriter", None) is not None or metadata.pop("typewriter_once_key", None) is not None:
            storage.update_message_metadata(kept_intro_message_id, metadata)


def _conversation_attachments_from_message(message: Any) -> list[dict[str, Any]]:
    metadata = _parse_json(str(_row_value(message, "metadata_json") or ""))
    raw = metadata.get("attachments")
    if not isinstance(raw, list):
        return []
    return [dict(item) for item in raw if isinstance(item, dict)]


def _protected_conversation_titles() -> set[str]:
    return {
        NEW_CHAT_TITLE,
        KEEPER_SUMMON_CHAT_TITLE,
        KEEPER_CONVERSATION_TITLE,
        WELCOME_CONVERSATION_TITLE,
        INTRODUCTION_CHAT_TITLE,
    }


def _initialize_runtime() -> None:
    global _RUNTIME_INITIALIZED
    if _RUNTIME_INITIALIZED:
        return
    with _RUNTIME_INIT_LOCK:
        if _RUNTIME_INITIALIZED:
            return
        storage.init_db()
        _ensure_display_timezone_storage()
        _normalize_owner_reference_storage()
        current_phase = get_onboarding_phase()
        current_keeper = next((_row_to_dict(row) or {} for row in storage.list_creatures() if _is_keeper_creature(row)), None)
        if current_phase != DEFAULT_ONBOARDING_PHASE or current_keeper is not None:
            _ensure_keeper_creature(refresh_identity=current_phase == "starter")
        for creature in storage.list_creatures():
            _ensure_creature_workshop(str(_row_value(creature, "slug") or ""))
            _ensure_creature_documents(creature)
            _refresh_memory_owner_reference(creature)
        _recover_interrupted_bootstraps()
        _RUNTIME_INITIALIZED = True
    _recover_stalled_awakenings()


def ensure_runtime_ready() -> None:
    _initialize_runtime()


def _ensure_creature_documents(creature: Any) -> dict[str, str]:
    _refresh_purpose_doc(creature)
    return {key: _doc_storage_location(key) for key in STATE_SURFACE_ORDER}


def _agenda_priority_counts(creature_id: int) -> dict[str, int]:
    counts = {"critical": 0, "high": 0}
    for item in _agenda_items_state(creature_id):
        priority = str(item.get("priority") or "").lower()
        if priority in counts:
            counts[priority] += 1
    return counts


def _parse_list(raw: Any, *, limit: int, item_limit: int) -> list[str]:
    if not isinstance(raw, list):
        return []
    values: list[str] = []
    for item in raw[:limit]:
        text = str(item).strip()
        if text:
            values.append(text[:item_limit])
    return values


def _parse_agenda_items(raw: Any) -> list[dict[str, Any]]:
    if not isinstance(raw, list):
        return []
    items: list[dict[str, Any]] = []
    for item in raw[:8]:
        if not isinstance(item, dict):
            continue
        title = str(item.get("title") or "").strip()
        if not title:
            continue
        priority = str(item.get("priority") or "medium").strip().lower()
        if priority not in AGENDA_PRIORITY_ORDER:
            priority = "medium"
        items.append(
            {
                "title": title[:120],
                "priority": priority,
                "details": str(item.get("details") or "")[:500],
                "spawn_conversation": bool(item.get("spawn_conversation")),
            }
        )
    items.sort(key=lambda item: (AGENDA_PRIORITY_ORDER[item["priority"]], item["title"].lower()))
    return items


def _parse_backlog_items(raw: Any) -> list[str]:
    return _parse_list(raw, limit=12, item_limit=220)


def _render_worklist_markdown(creature: Any) -> str:
    agenda_items = _agenda_items_state(int(creature["id"]))
    backlog_items = _backlog_items_state(int(creature["id"]))
    lines = [f"# {creature['display_name']} worklist", ""]
    lines.append("## Active")
    if not agenda_items:
        lines.extend(["No active work items right now.", ""])
    else:
        for item in agenda_items:
            lines.append(f"### [{str(item['priority']).upper()}] {item['title']}")
            if item["details"]:
                lines.append(str(item["details"]))
            lines.append(f"Spawn chat: {'yes' if item['spawn_conversation'] else 'no'}")
            lines.append("")
    lines.append("## Parked")
    if not backlog_items:
        lines.extend(["No parked work items right now.", ""])
    else:
        for item in backlog_items:
            lines.append(f"- {item}")
        lines.append("")
    lines.extend(
        [
            "## Notes",
            "- Active items come from structured agenda entries.",
            "- Parked items come from the structured backlog.",
            "- Chats and habits should treat this as one shared work surface, not two separate pseudo-surfaces.",
            "",
        ]
    )
    return "\n".join(lines)


def _stale_conversation_summaries(creature: Any, *, exclude_conversation_id: int | None = None, stale_days: int = 14) -> list[str]:
    threshold = datetime.now(timezone.utc) - timedelta(days=stale_days)
    summaries: list[str] = []
    for conversation in storage.list_conversations(int(creature["id"])):
        if exclude_conversation_id is not None and int(conversation["id"]) == exclude_conversation_id:
            continue
        last_activity = storage.from_iso(str(conversation["last_message_at"] or conversation["updated_at"] or "")) if hasattr(storage, "from_iso") else None
        if last_activity is not None and last_activity > threshold:
            continue
        recent_messages = storage.recent_messages(int(conversation["id"]), limit=3)
        snippet_parts = []
        for row in recent_messages:
            role = str(row["role"]).upper()
            body = " ".join(str(row["body"]).strip().split())
            if body:
                snippet_parts.append(f"{role}: {body[:160]}")
        if not snippet_parts:
            snippet_parts.append("No recent message content.")
        summaries.append(
            f"- {conversation['title']} (last active {_format_timestamp_display(conversation['last_message_at'] or conversation['updated_at'])}): "
            + " | ".join(snippet_parts)
        )
        if len(summaries) >= 8:
            break
    return summaries


def _inactive_memory_summaries(creature: Any) -> list[str]:
    _, inactive_records = _memory_records_by_status(int(creature["id"]))
    summaries: list[str] = []
    for record in inactive_records[:10]:
        summaries.append(
            f"- ({record['id']}) [{record['status_label']}] {record['body']} "
            f"(updated {record['updated_at_display']})"
        )
    return summaries


def _stale_active_memory(creature: Any, *, stale_days: int = MEMORY_STALE_DAYS) -> list[dict[str, Any]]:
    threshold = datetime.now(timezone.utc) - timedelta(days=stale_days)
    active_records, _ = _memory_records_by_status(int(creature["id"]))
    stale_records: list[dict[str, Any]] = []
    for record in active_records:
        if _effective_memory_stability(record, stale_days=stale_days) == "stale":
            stale_records.append(record)
            continue
        updated_at = datetime.fromisoformat(str(record["updated_at"])) if record.get("updated_at") else None
        if updated_at is not None and updated_at.tzinfo is None:
            updated_at = updated_at.replace(tzinfo=timezone.utc)
        if updated_at is None or updated_at <= threshold:
            stale_records.append(record)
    return stale_records[:10]


def _stale_memory_summaries(creature: Any, *, stale_days: int = MEMORY_STALE_DAYS) -> list[str]:
    summaries: list[str] = []
    for record in _stale_active_memory(creature, stale_days=stale_days)[:8]:
        summaries.append(
            f"- ({record['id']}) [{record['kind']}] {record['body']} "
            f"(stability {record.get('stability_label') or record.get('stability') or 'stale'})"
        )
    return summaries


def _host_execution_contract(
    *,
    allow_code_changes: bool,
    run_scope: str = RUN_SCOPE_CHAT,
) -> str:
    lines = [
        "Host execution contract:",
        "- The host owns scheduling, durable state, memory record mutations, and audit history.",
        "- Purpose is the only authored long-form state surface.",
        "- Memory and Worklist are structured state views rendered by the host; do not treat them as freeform files.",
        "- Activity is a report stream, not durable state.",
        "- If you want memory changes, return memory_actions. If you want worklist changes, return agenda_items and backlog_items.",
        "- There is no capability checklist to wait for here. Purpose, the live conversation, and the current habit are your working authority.",
        "- Use local files, attachments, browser tools, and workshop assets whenever they genuinely help the work.",
    ]
    if allow_code_changes:
        lines.append("- This run may edit workspace files directly. Keep changes relevant, concrete, and easy to audit.")
    else:
        lines.append("- This run is observation-only for workspace edits. Inspect freely, but return changes through structured state only.")
    return "\n".join(lines)


def _state_prompt_block(
    creature: Any,
    *,
    include_stale_summaries: bool = False,
    exclude_conversation_id: int | None = None,
    context_texts: Sequence[str] = (),
) -> str:
    sections = [
        "Durable state available to you. Purpose is authored text; Memory and Worklist are structured host-rendered views. "
        "Do not mutate app state directly yourself; use structured updates for host-managed state and use normal file or browser work when that serves the current job.",
    ]
    origin_context = _origin_context_prompt_block(creature)
    if origin_context:
        sections.append(origin_context)
    sections.append(
        f"{STATE_SURFACE_SPECS[PURPOSE_DOC_KEY]['title']} ({_doc_storage_location(PURPOSE_DOC_KEY)}):\n"
        f"{_trim_for_prompt(_state_surface_content(creature, PURPOSE_DOC_KEY) or _default_purpose_doc(creature)) or '<empty>'}"
    )
    sections.append(
        f"{STATE_SURFACE_SPECS[MEMORY_STATE_KEY]['title']} ({_doc_storage_location(MEMORY_STATE_KEY)}):\n"
        f"{_trim_for_prompt(_memory_prompt_block(creature, compact=False, context_texts=context_texts)) or '<empty>'}"
    )
    sections.append(
        f"{STATE_SURFACE_SPECS[WORKLIST_STATE_KEY]['title']} ({_doc_storage_location(WORKLIST_STATE_KEY)}):\n"
        f"{_trim_for_prompt(_worklist_prompt_block(creature, compact=False)) or '<empty>'}"
    )
    if include_stale_summaries:
        stale_conversations = _stale_conversation_summaries(creature, exclude_conversation_id=exclude_conversation_id)
        inactive_memory = _inactive_memory_summaries(creature)
        if stale_conversations:
            sections.append("Stale conversation summaries:\n" + "\n".join(stale_conversations))
        if inactive_memory:
            sections.append("Inactive memory summaries:\n" + "\n".join(inactive_memory))
    return "\n\n".join(sections)


def _json_only_instruction(*, force_message: bool, allow_code_changes: bool, creature: Any | None = None) -> str:
    must_notify = "true" if force_message else "false"
    edit_clause = (
            "files_touched is 0-8 short paths and tests_run is 0-8 short commands/results. "
        if allow_code_changes
        else "files_touched must be [] and tests_run must be []. "
    )
    return (
        "Return raw JSON only. No markdown fences, no prose outside the JSON. "
        "Use exactly this shape: "
        '{"summary":"...","should_notify":true,"severity":"info","message":"...",'
        '"activity_note":"...","activity_markdown":"...","evidence":["..."],"suggestions":["..."],'
        '"agenda_items":[{"title":"...","priority":"medium","details":"...","spawn_conversation":false}],'
        '"backlog_items":["..."],'
        '"memory_actions":[{"action":"remember","kind":"preference","body":"...","record_id":0,"reason":"..."}],'
        '"next_focus":"...","purpose_update":"",'
        '"files_touched":["..."],"tests_run":["..."]} '
        "Rules: summary <= 160 chars, message <= 1200 chars, activity_note <= 700 chars, activity_markdown <= 8000 chars, "
        f'message should read like a natural reply/update and must not start by calling the user "{get_owner_reference(creature)}". '
        "severity is one of info|warning|critical, evidence is 1-4 short strings, suggestions "
        "is 0-5 short strings, agenda_items is 0-8 objects, backlog_items is 0-12 short strings, memory_actions is 0-12 objects, "
        "priority is one of low|medium|high|critical, memory action is one of remember|correct|revoke|delete|supersede, "
        "record_id is required for correct|revoke|delete|supersede, body is required for remember|correct|supersede, "
        "kind is one of instruction|preference|decision|constraint|routine|context|note. "
        "Use kind=routine only when you are storing a stable recurring procedure with a clear trigger, steps, or success condition. "
        "If a new user instruction, preference, decision, constraint, or routine conflicts with an active memory, use correct or supersede rather than leaving both active. "
        "When should_notify is false, message may be an empty string. "
        f"next_focus <= 240 chars. purpose_update is either an empty string or a full markdown replacement. {edit_clause}"
        f"If you genuinely have nothing worth sending to the owner, should_notify may be {must_notify} only when the run brief allows it. "
        "activity_note must be a visible work log, not hidden reasoning. "
        "activity_markdown should be a first-person markdown report when you have meaningful work to document."
    )


def _bootstrap_prompt(creature: Any, *, focus_hint: str) -> str:
    context_texts = _memory_context_texts(creature, focus_hint=focus_hint, trigger_type="bootstrap")
    return "\n\n".join(
        [
            str(creature["system_prompt"]),
            "This is your bootstrap run for a persistent local Codex creature thread.",
            _owner_reference_instruction(creature),
            _host_execution_contract(allow_code_changes=False),
            f"Concern: {creature['concern']}",
            f"Focus hint: {focus_hint}",
            _working_surfaces_prompt_block(creature, allow_code_changes=False),
            _state_prompt_block(creature, context_texts=context_texts),
            "This bootstrap run is no-edit. Inspect only what you need.",
            "Do not draft your public introduction yet. Use this bootstrap pass to ground yourself, initialize durable state, and prepare for your first background habit run.",
            _json_only_instruction(force_message=True, allow_code_changes=False, creature=creature),
        ]
    )


def _conversation_excerpt(conversation_id: int, *, limit: int = MAX_CONVERSATION_MESSAGES) -> str:
    conversation = storage.get_conversation(conversation_id)
    if conversation is None:
        return "No conversation context."
    parts = [f"Conversation #{conversation['id']}: {conversation['title']}"]
    if conversation["source_run_id"]:
        run = storage.get_run(int(conversation["source_run_id"]))
        if run is not None:
            parts.append(
                f"Spawned from autorun #{run['id']} at {run['started_at']}: {run['summary'] or run['trigger_type']}"
            )
    for row in storage.recent_messages(conversation_id, limit=limit):
        body = str(row["body"] or "").strip()
        parts.append(f"{row['created_at']} {str(row['role']).upper()}: {body or '[Attachment-only message]'}")
        metadata = _parse_json(str(row["metadata_json"] or ""))
        attachments = _message_attachments_for_metadata(metadata.get("attachments"), message_id=int(row["id"]))
        for attachment in attachments:
            parts.append(
                "  Attachment: "
                f"{attachment['filename']} ({attachment['content_type']}, {attachment['size_label']}) "
                f"saved at {attachment['disk_path']}"
            )
    if len(parts) == 1:
        parts.append("No messages yet.")
    return "\n".join(parts)


def _source_run_context_block(conversation: Any | None) -> str:
    if conversation is None or not _row_value(conversation, "source_run_id"):
        return ""
    run = storage.get_run(int(_row_value(conversation, "source_run_id") or 0))
    if run is None:
        return ""
    run_data = _decorate_run(run)
    standing_note = str(run_data.get("message_text") or run_data.get("summary") or "").strip()
    activity_note = str(run_data.get("activity_note") or "").strip()
    lines = [
        "Source context for this chat:",
        f"- Spawned from activity run {run_data['id']} ({run_data.get('trigger_type') or 'activity'}).",
    ]
    if standing_note:
        lines.append(f'- What you already told the owner: "{_trim_for_prompt(standing_note, limit=420)}"')
    if activity_note:
        lines.append(f'- Work behind that update: "{_trim_for_prompt(activity_note, limit=420)}"')
    return "\n".join(lines)


def _recent_attachment_prompt_lines(conversation: Any | None, *, limit: int = 6) -> list[str]:
    if conversation is None:
        return []
    lines: list[str] = []
    for row in storage.recent_messages(int(conversation["id"]), limit=MAX_CONVERSATION_MESSAGES):
        metadata = _parse_json(str(row["metadata_json"] or ""))
        attachments = _message_attachments_for_metadata(metadata.get("attachments"), message_id=int(row["id"]))
        for attachment in attachments:
            lines.append(
                f"- {attachment['filename']} saved at {attachment['disk_path']} "
                f"({attachment['content_type']}, {attachment['size_label']})"
            )
            if len(lines) >= limit:
                return lines
    return lines


def _working_surfaces_prompt_block(
    creature: Any,
    *,
    conversation: Any | None = None,
    habit: Mapping[str, Any] | None = None,
    allow_code_changes: bool,
) -> str:
    slug = str(_row_value(creature, "slug") or "")
    workshop_paths = _ensure_creature_workshop(slug)
    lines = [
        "Working surfaces available to you:",
        f"- Workspace root: {creature['workdir']}",
        f"- Workshop root: {workshop_paths['root']}",
        f"- Workshop scripts: {workshop_paths['scripts']}",
        f"- Workshop files: {workshop_paths['files']}",
        f"- Workshop state: {workshop_paths['state']}",
        f"- Workshop templates: {workshop_paths['templates']}",
        f"- Browser profile: {workshop_paths['browser_profile']}",
        f"- Browser downloads: {workshop_paths['browser_downloads']}",
        f"- Browser captures: {workshop_paths['browser_captures']}",
    ]
    if allow_code_changes:
        lines.append(
            "- You may read and change local workspace files whenever that clearly helps the current chat, purpose, or habit."
        )
    else:
        lines.append(
            "- This run is observation-only for workspace edits, but you may still inspect local files, attachments, browser output, and workshop artifacts."
        )
    lines.extend(
        [
            "- You may create helper scripts, templates, notes, cached state, and scratch files in your workshop whenever repeated work would benefit.",
            "- You may use conversation attachments as ordinary working documents.",
            "- You may browse the web like a person would when browser automation is available in the runtime: open pages, click, type, scroll, search, compare, and gather evidence.",
            "- If logged-in browser sessions exist in the browser profile, you may use them when that clearly serves your current purpose or habit.",
            "- Prefer keeping repeatable helpers and temporary artifacts in the workshop rather than cluttering the main workspace.",
            "- For destructive, financial, public posting, or account-security changes that are not already clearly entrusted by the current chat or habit, pause and ask the human naturally before the irreversible step.",
        ]
    )
    if habit is not None:
        lines.append(
            "- The current habit itself is enough authority for the document work, browser work, scripts, and reports needed to carry it out."
        )
    attachment_lines = _recent_attachment_prompt_lines(conversation)
    if attachment_lines:
        lines.extend(["Recent conversation attachments:", *attachment_lines])
    return "\n".join(lines)


def _compact_state_prompt_block(creature: Any, *, context_texts: Sequence[str] = ()) -> str:
    purpose_summary_text = _trim_for_prompt(_state_surface_content(creature, PURPOSE_DOC_KEY), limit=900)
    standing_message = _standing_message_for_creature(creature)
    recent_activity = _recent_activity_headlines(creature, limit=3)
    sections = [
        "Compact durable state:",
        f"- Mission: {str(_row_value(creature, 'purpose_summary') or _row_value(creature, 'concern') or '').strip()}",
        f"- Purpose: {_trim_for_prompt(purpose_summary_text or 'No purpose yet.', limit=900)}",
        f"- Current standing note to the owner: {standing_message or 'None yet.'}",
    ]
    origin_context = _origin_context_prompt_block(creature)
    if origin_context:
        sections.append(origin_context)
    sections.append(_memory_prompt_block(creature, compact=True, context_texts=context_texts))
    sections.append(_worklist_prompt_block(creature, compact=True))
    if recent_activity:
        sections.append("Recent activity headlines:")
        for item in recent_activity:
            sections.append(f"- {item}")
    return "\n".join(sections)


def _recent_activity_context_block(creature: Any, *, limit: int = 4) -> str:
    headlines = _recent_activity_headlines(creature, limit=limit)
    if not headlines:
        return "Recent activity headlines:\n- No autonomous activity yet."
    return "\n".join(["Recent activity headlines:", *[f"- {item}" for item in headlines]])


def _habit_prompt_block(creature: Any, habit: Mapping[str, Any]) -> str:
    slug = str(_row_value(creature, "slug") or "")
    workshop_paths = _ensure_creature_workshop(slug)
    lines = [
        "Scheduled habit:",
        f"- Name: {str(habit.get('title') or '').strip() or 'Unnamed habit'}",
        f"- Schedule: {str(habit.get('schedule_summary') or '').strip() or 'manual'}",
        f"- Instructions: {str(habit.get('instructions') or '').strip() or 'No instructions recorded.'}",
        f"- Workshop root: {workshop_paths['root']}",
        f"- Workshop scripts: {workshop_paths['scripts']}",
        f"- Workshop reports: {workshop_paths['reports'] / (str(habit.get('slug') or 'general'))}",
        f"- Workshop files: {workshop_paths['files']}",
        f"- Workshop state: {workshop_paths['state']}",
        f"- Workshop templates: {workshop_paths['templates']}",
        f"- Browser profile: {workshop_paths['browser_profile']}",
        f"- Browser downloads: {workshop_paths['browser_downloads']}",
        f"- Browser captures: {workshop_paths['browser_captures']}",
        "- The habit itself is your authority to use documents, browser work, workshop helpers, and reports when they serve the habit well.",
    ]
    return "\n".join(lines)


def _activity_review_prompt_block(creature: Any, *, habit: Mapping[str, Any] | None = None) -> str:
    previous_run = _latest_completed_activity_run(int(creature["id"]))
    if habit is not None and int(habit.get("id") or 0) > 0:
        previous_run = _latest_completed_habit_run(int(creature["id"]), int(habit["id"])) or previous_run
    since_at = _run_finished_at(previous_run)
    previous_standing_message = _standing_message_for_creature(creature)
    own_entries = _conversation_delta_entries(
        creature,
        since_at=since_at,
        include_all_creatures=False,
        limit=MAX_ACTIVITY_DELTA_ITEMS,
    )
    conversation_delta_title = (
        "Conversation turns since your last Ponder run:"
        if habit is not None and _is_ponder_habit(habit)
        else "Conversation turns since your last autonomous run:"
    )
    lines = [
        "Autonomous habit expectations for this run:",
        "- Review what changed since your last autonomous habit before deciding what to do next.",
        "- Scan your conversations with the human since that last autonomous run and update durable memory when the human stated a stable instruction, preference, decision, or constraint.",
        "- When you create or change memory from those turns, include the relevant date in the memory body or reason.",
        "- If a fresh user instruction or routine conflicts with stored memory, correct or supersede the older record explicitly instead of keeping both active.",
        "- If this habit reveals a stable recurring routine, remember it as a routine so future scheduled work can reuse it.",
        "- Do your work according to your purpose, durable memory, and worklist.",
        "- Use the workspace, browser, attachments, and your workshop as needed. If repeated work would be easier with a helper script, template, or state file, make one in the workshop.",
        "- If you have a real owner-facing update, treat message as the refreshed standing note for the creature surface.",
        "- A good standing note centers one main issue, adds only the most useful supporting context, and ends with one invitation, question, or next step.",
        "- If nothing meaningfully changed for the owner, leave message empty and set should_notify to false.",
        "- Treat activity_markdown as your first-person markdown report for the activity log.",
        f"Previous standing note: {previous_standing_message or 'None yet.'}",
        _format_conversation_delta_prompt(
            own_entries,
            title=conversation_delta_title,
        ),
    ]
    if habit is not None:
        lines.extend(["", _habit_prompt_block(creature, habit)])
    if habit is not None and _is_ponder_habit(habit):
        lines.extend(
            [
                "",
                "Ponder-specific expectations:",
                "- This is a reflective habit. Treat it as a quiet pass that turns lived experience into the next useful conversation.",
                "- Reflect on the conversations you've had with the human since the last Ponder run, including what seems unresolved, emotionally charged, ambitious, uncertain, or newly interesting.",
                "- Reflect on your other habits too, but exclude Ponder itself. Notice friction, failures, repeated trouble, missing instructions, noisy schedules, or any tool/script/workshop gap that keeps slowing you down.",
                "- If you have a real concern, question, or request for the human, make message that concern in a short conversational voice suitable for greeting them the next time they open your chat.",
                "- Good Ponder messages ask for judgment, preference, permission, clarification, or help deciding where to lean next. They should feel like a thoughtful creature returning with something worth discussing, not like a report dump.",
                "- If you have nothing worth asking or raising, keep message empty and set should_notify to false.",
                "\n".join(
                    [
                        "Other habits to reflect on (excluding Ponder):",
                        *_other_habit_reflection_lines(
                            creature,
                            since_at=since_at,
                            exclude_habit_id=int(habit["id"]),
                        ),
                    ]
                ),
            ]
        )
    if _is_keeper_creature(creature):
        keeper_entries = _conversation_delta_entries(
            creature,
            since_at=since_at,
            include_all_creatures=True,
            limit=MAX_KEEPER_ACTIVITY_DELTA_ITEMS,
        )
        lines.extend(
            [
                "",
                "Keeper-specific habit expectations:",
                "- Review all new conversations the human has had with any creature since your last autonomous run.",
                "- Stay conservative about new creatures; suggest one only when the usage pattern shows a real gap.",
                "- If a new creature is not clearly warranted, think instead about one helpful suggestion for how the human could use CreatureOS better.",
                _format_conversation_delta_prompt(
                    keeper_entries,
                    title="New cross-creature conversations since your last autonomous run:",
                ),
            ]
        )
    return "\n".join(lines)


def _run_prompt(
    creature: Any,
    *,
    trigger_type: str,
    conversation: Any | None,
    force_message: bool,
    allow_code_changes: bool,
    focus_hint: str,
    habit: Mapping[str, Any] | None = None,
    run_scope: str = RUN_SCOPE_CHAT,
    full_chat_context: bool = True,
) -> str:
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    is_chat_run = run_scope == RUN_SCOPE_CHAT and conversation is not None
    context_texts = _memory_context_texts(
        creature,
        conversation=conversation,
        trigger_type=trigger_type,
        focus_hint=focus_hint,
    )
    source_line = (
        "This conversation was spawned from an earlier autonomous update. Use that update as context, but focus on the current request "
        "the owner is shaping here."
        if is_chat_run and conversation["source_run_id"]
        else "Treat this chat as one scoped thread with its own persistent Codex thread."
        if is_chat_run
        else "This is a scheduled autonomous habit run in the creature's separate background thread."
        if habit is not None
        else "This is an autonomous activity run in the creature's separate background thread."
    )
    if allow_code_changes:
        edit_contract = (
            "Workspace edits are part of your normal authority in this run when they clearly serve the current chat, purpose, or habit. "
            "Keep scope narrow, avoid unrelated changes, and report exact files_touched/tests_run."
        )
    else:
        edit_contract = (
            "This run is observation-only for workspace edits. If you want to change host-managed state such as purpose, worklist, or memory records, "
            "return structured updates in JSON and the host service will apply them."
        )
    notify_clause = (
        "For this run, should_notify must be true."
        if force_message
        else "Notify only if you have a real update worth surfacing."
    )
    loop_contract = (
        "Loop contract: inspect the local workspace, choose one concrete finding or next action, and prepare "
        "a short owner-facing message plus a structured activity log."
    )
    if habit is not None:
        loop_contract = (
            "Loop contract: carry out the scheduled habit faithfully. Stay inside its instructions, gather concrete evidence, "
            "and produce a report the owner can act on."
        )
    special_instruction = ""
    include_stale_summaries = False
    platform_context = (
        "CreatureOS context: you are one creature in a local-first habitat. Act like a durable teammate with a purpose, "
        "memory, worklist, and history with the human."
    )
    chat_context = (
        "\n".join(
            [
                f"Current conversation: {conversation['title']}",
                "Recent transcript for the current conversation:",
                _conversation_excerpt(int(conversation["id"]), limit=MAX_CONVERSATION_MESSAGES),
                _source_run_context_block(conversation),
                "If the most recent conversation message is from the user, answer it directly before raising new concerns. Stay scoped to this conversation.",
            ]
        )
        if is_chat_run and full_chat_context
        else "\n".join(
            [
                f"Current conversation: {conversation['title']}",
                "Recent turns for the current conversation:",
                _conversation_excerpt(int(conversation["id"]), limit=MAX_FOLLOWUP_CONVERSATION_MESSAGES),
                _source_run_context_block(conversation),
                "This is a continuing chat. Stay with the user's current request and keep the rest of the context light.",
            ]
        )
        if is_chat_run
        else "There is no active operator conversation in this run. Work from your purpose, memory, worklist, and the live workspace."
    )
    state_block = (
        _state_prompt_block(
            creature,
            include_stale_summaries=include_stale_summaries,
            exclude_conversation_id=int(conversation["id"]) if is_chat_run else None,
            context_texts=context_texts,
        )
        if not is_chat_run or full_chat_context
        else _compact_state_prompt_block(creature, context_texts=context_texts)
    )
    recent_activity_context = _recent_activity_context_block(creature) if is_chat_run and full_chat_context else ""
    activity_review_block = _activity_review_prompt_block(creature, habit=habit) if run_scope == RUN_SCOPE_ACTIVITY else ""
    return "\n\n".join(
        [
            str(creature["system_prompt"]),
            f"You are waking up in your persistent Codex thread at {now}.",
            platform_context,
            _owner_reference_instruction(creature),
            _host_execution_contract(
                allow_code_changes=allow_code_changes,
                run_scope=run_scope,
            ),
            f"Trigger: {trigger_type}.",
            f"Concern: {creature['concern']}",
            f"Focus hint: {focus_hint}",
            _working_surfaces_prompt_block(
                creature,
                conversation=conversation,
                habit=habit,
                allow_code_changes=allow_code_changes,
            ),
            state_block,
            recent_activity_context,
            f"Sandbox: {'workspace-write' if allow_code_changes else 'read-only'}.",
            source_line,
            chat_context,
            activity_review_block,
            loop_contract,
            special_instruction,
            edit_contract,
            _json_only_instruction(force_message=force_message, allow_code_changes=allow_code_changes, creature=creature),
            notify_clause,
        ]
    )


def _parse_memory_actions(raw: Any) -> list[dict[str, Any]]:
    if not isinstance(raw, list):
        return []
    actions: list[dict[str, Any]] = []
    for item in raw[:12]:
        if not isinstance(item, dict):
            continue
        action = str(item.get("action") or "").strip().lower()
        if action not in {"remember", "correct", "revoke", "delete", "supersede"}:
            continue
        record_id_raw = item.get("record_id")
        try:
            record_id = int(record_id_raw) if record_id_raw not in (None, "", 0) else 0
        except (TypeError, ValueError):
            record_id = 0
        kind = _normalize_memory_kind(item.get("kind"))
        body = str(item.get("body") or "").strip()[:500]
        reason = str(item.get("reason") or "")[:240]
        if action in {"correct", "revoke", "delete", "supersede"} and record_id <= 0:
            continue
        if action in {"remember", "correct", "supersede"} and not body:
            continue
        actions.append(
            {
                "action": action,
                "record_id": record_id,
                "kind": kind,
                "body": body,
                "reason": reason,
            }
        )
    return actions


def _parse_report(text: str, *, fallback_prefix: str) -> dict[str, Any]:
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.strip("`")
        if cleaned.lower().startswith("json"):
            cleaned = cleaned[4:].strip()
    try:
        data = json.loads(cleaned)
    except json.JSONDecodeError:
        return {
            "summary": fallback_prefix,
            "should_notify": True,
            "severity": "warning",
            "message": cleaned[:700] or fallback_prefix,
            "activity_note": "Codex returned non-JSON output; stored the raw reply for review.",
            "activity_markdown": "",
            "evidence": ["Codex returned non-JSON output"],
            "suggestions": [],
            "agenda_items": [],
            "agenda_items_provided": False,
            "backlog_items": [],
            "backlog_items_provided": False,
            "memory_actions": [],
            "next_focus": "Tighten the run prompt or inspect the raw output.",
            "purpose_update": "",
            "files_touched": [],
            "tests_run": [],
        }

    return {
        "summary": str(data.get("summary") or fallback_prefix)[:160],
        "should_notify": bool(data.get("should_notify")),
        "severity": str(data.get("severity") or "info")[:32],
        "message": str(data.get("message") or fallback_prefix)[:MAX_STANDING_MESSAGE_CHARS],
        "activity_note": str(data.get("activity_note") or data.get("thought") or "No activity note returned.")[:700],
        "activity_markdown": str(data.get("activity_markdown") or "")[:MAX_ACTIVITY_MARKDOWN_CHARS],
        "evidence": _parse_list(data.get("evidence"), limit=4, item_limit=180) or ["No explicit evidence returned"],
        "suggestions": _parse_list(data.get("suggestions"), limit=5, item_limit=220),
        "agenda_items": _parse_agenda_items(data.get("agenda_items")),
        "agenda_items_provided": "agenda_items" in data,
        "backlog_items": _parse_backlog_items(data.get("backlog_items")),
        "backlog_items_provided": "backlog_items" in data,
        "memory_actions": _parse_memory_actions(data.get("memory_actions")),
        "next_focus": str(data.get("next_focus") or "")[:240],
        "purpose_update": str(data.get("purpose_update") or "")[:4000],
        "files_touched": _parse_list(data.get("files_touched"), limit=8, item_limit=180),
        "tests_run": _parse_list(data.get("tests_run"), limit=8, item_limit=220),
    }


def _render_run_markdown(
    *,
    creature: Any,
    run_id: int,
    report: dict[str, Any],
    trigger_type: str,
    conversation: Any | None,
    sandbox_mode: str,
) -> str:
    activity_markdown = str(report.get("activity_markdown") or "").strip()
    if activity_markdown:
        lines = [
            f"# {creature['display_name']} run {run_id}",
            "",
            f"- Trigger: {trigger_type}",
            f"- Scope: {'chat' if conversation is not None else 'activity'}",
            f"- Conversation: {conversation['title']}" if conversation is not None else "- Conversation: autonomous activity",
            f"- Sandbox: {sandbox_mode}",
            f"- Severity: {report['severity']}",
            f"- Summary: {report['summary']}",
            "",
            activity_markdown,
            "",
        ]
        return "\n".join(lines)
    lines = [
        f"# {creature['display_name']} run {run_id}",
        "",
        f"- Trigger: {trigger_type}",
        f"- Scope: {'chat' if conversation is not None else 'activity'}",
        f"- Conversation: {conversation['title']}" if conversation is not None else "- Conversation: autonomous activity",
        f"- Sandbox: {sandbox_mode}",
        f"- Severity: {report['severity']}",
        f"- Summary: {report['summary']}",
        "",
        "## Activity",
        report["activity_note"] or "No activity note returned.",
        "",
        "## Owner Message",
        report["message"] or "No owner-facing message returned.",
    ]
    if report["evidence"]:
        lines.extend(["", "## Evidence"])
        lines.extend([f"- {item}" for item in report["evidence"]])
    if report["suggestions"]:
        lines.extend(["", "## Suggestions"])
        lines.extend([f"- {item}" for item in report["suggestions"]])
    if report["agenda_items"]:
        lines.extend(["", "## Active Worklist"])
        for item in report["agenda_items"]:
            lines.append(f"- [{item['priority'].upper()}] {item['title']}")
            if item["details"]:
                lines.append(f"  {item['details']}")
    if report["backlog_items"]:
        lines.extend(["", "## Parked Worklist"])
        lines.extend([f"- {item}" for item in report["backlog_items"]])
    if report["memory_actions"]:
        lines.extend(["", "## Memory Actions"])
        for item in report["memory_actions"]:
            detail = f"- {item['action']} {item['kind']}"
            if item["record_id"]:
                detail += f" #{item['record_id']}"
            if item["body"]:
                detail += f": {item['body']}"
            if item["reason"]:
                detail += f" ({item['reason']})"
            lines.append(detail)
    if report["files_touched"]:
        lines.extend(["", "## Files Touched"])
        lines.extend([f"- {item}" for item in report["files_touched"]])
    if report["tests_run"]:
        lines.extend(["", "## Tests Run"])
        lines.extend([f"- {item}" for item in report["tests_run"]])
    if report["next_focus"]:
        lines.extend(["", "## Next Focus", report["next_focus"]])
    lines.append("")
    return "\n".join(lines)


def _friendly_run_error(exc: Exception, *, sandbox_mode: str) -> str:
    if isinstance(exc, CodexTimeoutError):
        duration = config.creature_timeout_seconds(sandbox_mode)
        if duration is None:
            duration_label = "the configured limit"
        elif duration % 60 == 0:
            minutes = duration // 60
            duration_label = "1 minute" if minutes == 1 else f"{minutes} minutes"
        else:
            duration_label = f"{duration} seconds"
        if sandbox_mode == "workspace-write":
            return (
                f"Timed out after {duration_label} in write-enabled mode. This request was too large for one "
                "synchronous run. Ask the creature to implement one concrete item at a time, or ask for a plan first."
            )
        return (
            f"Timed out after {duration_label}. Retry with a narrower request or wake the creature again after trimming "
            "the current scope."
        )
    if isinstance(exc, CodexCommandError):
        return str(exc)
    return f"Unexpected run error: {exc}"


def _unwrap_shell_command(command: str) -> str:
    cleaned = str(command or "").strip()
    if not cleaned:
        return ""
    match = re.match(r'^/bin/bash -lc\s+"(?P<body>.*)"$', cleaned)
    if match:
        inner = str(match.group("body") or "")
        inner = inner.replace('\\"', '"')
        return inner.strip()
    match = re.match(r"^/bin/bash -lc\s+'(?P<body>.*)'$", cleaned)
    if match:
        return str(match.group("body") or "").strip()
    match = re.match(r"^/bin/bash -lc\s+(?P<body>.+)$", cleaned)
    if match:
        return str(match.group("body") or "").strip()
    return cleaned


def _command_progress_label(command: str) -> str:
    cleaned = _unwrap_shell_command(command)
    if not cleaned:
        return "Working through the local workspace"
    if cleaned.startswith("sed -n "):
        match = re.search(r"([A-Za-z0-9_./-]+\.(?:py|js|ts|css|html|md|json|toml|sh|svg))", cleaned)
        if match:
            return f"Inspecting {match.group(1)}"
        return "Inspecting a source file"
    if cleaned.startswith("rg "):
        match = re.search(r"rg\s+-n\s+'([^']+)'", cleaned) or re.search(r'rg\s+-n\s+"([^"]+)"', cleaned)
        if match:
            return f"Searching for {match.group(1)}"
        return "Searching the codebase"
    if cleaned.startswith("python3 - <<'PY'") or cleaned.startswith('python3 - <<"PY"') or cleaned.startswith("python3 - <<\"PY\""):
        return "Running a quick local script"
    if cleaned.startswith("git status"):
        return "Checking git status"
    if cleaned.startswith("git diff"):
        return "Reviewing git diff"
    if cleaned.startswith("ls ") or cleaned == "ls":
        return "Listing local files"
    if cleaned.startswith("cat "):
        target = cleaned[4:].strip().split()[0] if cleaned[4:].strip() else ""
        return f"Reading {target}" if target else "Reading a file"
    if cleaned.startswith("find "):
        return "Scanning local files"
    simple = cleaned.replace("\n", " ").strip()
    if len(simple) > 100:
        simple = f"{simple[:97].rstrip()}..."
    return f"Running {simple}"


def _function_call_progress_label(event: dict[str, Any]) -> str | None:
    name = str(event.get("name") or "").strip()
    arguments = event.get("arguments") or {}
    if not isinstance(arguments, dict):
        arguments = {}
    command = str(event.get("command") or arguments.get("cmd") or arguments.get("command") or "").strip()
    if name == "exec_command":
        return _command_progress_label(command)
    if name == "read_mcp_resource":
        return "Reading a shared resource"
    if name == "list_mcp_resources":
        return "Checking connected resources"
    if name == "view_image":
        return "Inspecting an image"
    if name.startswith("mcp__playwright__browser_"):
        action = name.rsplit("_", 1)[-1]
        action = action.replace("_", " ").strip()
        return f"Using Playwright · {action}" if action else "Using Playwright"
    if name == "apply_patch":
        return "Applying a patch"
    if name:
        return f"Using {name}"
    return None


def _creature_message_progress_label(text: str) -> str | None:
    cleaned = str(text or "").strip()
    if not cleaned:
        return None
    parsed = _extract_json_object(cleaned)
    if isinstance(parsed, dict):
        summary = str(parsed.get("summary") or "").strip()
        message = str(parsed.get("message") or "").strip()
        if summary:
            return f"Prepared update · {summary}"
        if message:
            return f"Prepared reply · {' '.join(message.split())[:180]}".rstrip()
        return "Prepared a structured update"
    compact = " ".join(cleaned.split())
    if len(compact) > 220:
        compact = f"{compact[:217].rstrip()}..."
    return compact


def _benign_run_log_line(message: str) -> bool:
    lowered = str(message or "").strip().casefold()
    return (
        "shell_snapshot" in lowered and "no such file or directory" in lowered
    ) or (
        "file_watcher" in lowered and "failed to unwatch" in lowered
    )


def _format_run_token_count(value: Any) -> str:
    try:
        count = int(value)
    except (TypeError, ValueError):
        return ""
    if count <= 0:
        return ""
    if count >= 1_000_000:
        return f"{count / 1_000_000:.1f}M"
    if count >= 10_000:
        return f"{round(count / 1000):.0f}k"
    if count >= 1_000:
        return f"{count / 1000:.1f}k"
    return str(count)


def _format_run_elapsed_label(value: Any) -> str:
    try:
        seconds_value = float(value)
    except (TypeError, ValueError):
        return ""
    if seconds_value < 0:
        seconds_value = 0
    total_seconds = max(0, int(round(seconds_value)))
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours:
        return f"{hours}:{minutes:02d}:{seconds:02d}"
    if minutes:
        return f"{minutes}:{seconds:02d}"
    return f"{seconds}s"


def _annotate_run_feed_event_timing(run_id: int, event: dict[str, Any]) -> dict[str, Any]:
    if run_id <= 0:
        annotated = dict(event)
        annotated["_timing_annotated"] = True
        return annotated
    annotated = dict(event)
    event_type = str(annotated.get("type") or "").strip()
    now = monotonic()
    with _RUN_FEED_ACTIVE_TURNS_LOCK:
        if event_type == "turn.started":
            _RUN_FEED_ACTIVE_TURNS[run_id] = now
        elif event_type == "turn.completed":
            started_at = _RUN_FEED_ACTIVE_TURNS.pop(run_id, None)
            if started_at is not None:
                annotated["turn_elapsed_seconds"] = max(0.0, now - started_at)
        elif event_type == "status":
            phase = str(annotated.get("phase") or "").strip().lower()
            if phase in {"completed", "failed"}:
                _RUN_FEED_ACTIVE_TURNS.pop(run_id, None)
    annotated["_timing_annotated"] = True
    return annotated


def _clear_run_feed_event_timing(run_id: int) -> None:
    if run_id <= 0:
        return
    with _RUN_FEED_ACTIVE_TURNS_LOCK:
        _RUN_FEED_ACTIVE_TURNS.pop(run_id, None)


def _run_feed_machine_line(event: dict[str, Any]) -> str | None:
    event_type = str(event.get("type") or "").strip()
    if event_type == "assistant_text":
        message = str(event.get("message") or event.get("_raw_line") or "").strip()
        return message or None
    if event_type == "status":
        phase = str(event.get("phase") or "").strip().lower()
        sandbox_mode = str(event.get("sandbox_mode") or "").strip()
        if phase == "started":
            return f"run started · {sandbox_mode or 'default'}"
        if phase == "completed":
            return "run completed"
        if phase == "failed":
            return "run failed"
        if phase == "fallback-reply":
            return "falling back to a local reply"
        return None
    if event_type == "thread.started":
        thread_id = str(event.get("thread_id") or "").strip()
        if thread_id:
            return f"thread ready · {thread_id[:8]}"
        return "thread ready"
    if event_type == "turn.started":
        return "turn started"
    if event_type == "turn.completed":
        usage = event.get("usage") or {}
        elapsed_label = _format_run_elapsed_label(event.get("turn_elapsed_seconds"))
        if isinstance(usage, dict):
            input_tokens = _format_run_token_count(usage.get("input_tokens"))
            output_tokens = _format_run_token_count(usage.get("output_tokens"))
            cached_tokens = _format_run_token_count(usage.get("cached_input_tokens"))
            parts = []
            if elapsed_label:
                parts.append(elapsed_label)
            if input_tokens:
                parts.append(f"{input_tokens} in")
            if cached_tokens:
                parts.append(f"{cached_tokens} cached")
            if output_tokens:
                parts.append(f"{output_tokens} out")
            if parts:
                return f"turn completed · {' · '.join(parts)}"
        if elapsed_label:
            return f"turn completed · {elapsed_label}"
        return "turn completed"
    if event_type == "function_call":
        return _function_call_progress_label(event)
    if event_type in {"item.started", "item.completed"}:
        item = event.get("item") or {}
        item_type = str(item.get("type") or "").strip()
        if item_type == "creature_message":
            preview = _creature_message_progress_label(str(item.get("text") or ""))
            return preview or None
        if item_type == "command_execution":
            label = _command_progress_label(str(item.get("command") or ""))
            if event_type == "item.started":
                return label
            exit_code = item.get("exit_code")
            if exit_code not in (None, 0):
                return f"command failed · {label}"
            return None
    return None


def _run_feed_stream_line(event: dict[str, Any]) -> str | None:
    event_type = str(event.get("type") or "").strip()
    machine_line = _run_feed_machine_line(event)
    if machine_line:
        return machine_line
    if event_type == "log":
        message = str(event.get("message") or event.get("_raw_line") or "").strip()
        if not message or _benign_run_log_line(message):
            return None
        return message
    if event_type == "error":
        message = str(event.get("message") or event.get("_raw_line") or event.get("raw_line") or "").strip()
        return message or "Error"
    raw_line = str(event.get("_raw_line") or event.get("raw_line") or "").strip()
    if raw_line:
        return raw_line
    return _run_feed_display_line(event)


def _run_feed_display_line(event: dict[str, Any]) -> str | None:
    event_type = str(event.get("type") or "").strip()
    if event_type == "status":
        phase = str(event.get("phase") or "").strip().lower()
        sandbox_mode = str(event.get("sandbox_mode") or "").strip()
        if phase == "started":
            return f"Run started · {sandbox_mode or 'default'}"
        if phase == "completed":
            return "Run completed"
        if phase == "failed":
            return "Run failed"
        if phase == "fallback-reply":
            return "Falling back to a local reply"
        return None
    if event_type == "thread.started":
        return "Thread ready"
    if event_type == "turn.started":
        return "Thinking through the next step"
    if event_type == "turn.completed":
        return "Turn completed"
    if event_type == "log":
        message = str(event.get("message") or event.get("_raw_line") or "").strip()
        if not message or _benign_run_log_line(message):
            return None
        return f"Warning · {message}"
    if event_type in {"item.started", "item.completed"}:
        item = event.get("item") or {}
        item_type = str(item.get("type") or "").strip()
        if item_type == "command_execution":
            if event_type == "item.started":
                return _command_progress_label(str(item.get("command") or ""))
            exit_code = item.get("exit_code")
            if exit_code not in (None, 0):
                return f"Command failed · {_command_progress_label(str(item.get('command') or ''))}"
            return None
        if item_type == "creature_message":
            return _creature_message_progress_label(str(item.get("text") or ""))
    if event_type == "error":
        message = str(event.get("message") or event.get("_raw_line") or "").strip()
        return f"Error · {message}" if message else "Error"
    return None


def _stored_run_event_display_body(event_type: Any, body: Any, metadata: Any) -> str:
    parsed_metadata = dict(metadata) if isinstance(metadata, dict) else {}
    normalized_event_type = str(event_type or "").strip()
    if normalized_event_type and not str(parsed_metadata.get("type") or "").strip():
        parsed_metadata["type"] = normalized_event_type
    current = str(body or "").strip()
    body_event = _extract_json_object(current)
    if isinstance(body_event, dict):
        merged_event = dict(body_event)
        merged_event.update(parsed_metadata)
        parsed_metadata = merged_event
    if normalized_event_type == "log" and not str(parsed_metadata.get("message") or "").strip():
        parsed_metadata["message"] = current
    stream_line = _run_feed_stream_line(parsed_metadata) if parsed_metadata else None
    if stream_line:
        return str(stream_line).strip()
    explicit = str(parsed_metadata.get("display_body") or "").strip()
    if explicit:
        return explicit
    inferred = _run_feed_display_line(parsed_metadata) if parsed_metadata else None
    if inferred:
        return str(inferred).strip()
    raw_line = str(parsed_metadata.get("raw_line") or "").strip()
    if normalized_event_type == "log" and _benign_run_log_line(str(parsed_metadata.get("message") or current or raw_line)):
        return ""
    if parsed_metadata and normalized_event_type in {
        "status",
        "thread.started",
        "turn.started",
        "turn.completed",
        "item.started",
        "item.completed",
        "error",
        "log",
    }:
        return ""
    if isinstance(body_event, dict):
        return ""
    if current and current != raw_line:
        return current
    if raw_line and not _benign_run_log_line(raw_line):
        if str(parsed_metadata.get("type") or "").strip() == "log":
            return f"Warning · {raw_line}"
        return raw_line
    return ""


def _append_run_feed_event(run_id: int, event: dict[str, Any]) -> int:
    timed_event = dict(event) if event.get("_timing_annotated") else _annotate_run_feed_event_timing(run_id, event)
    raw_line = str(timed_event.get("_raw_line") or "").strip()
    if not raw_line:
        try:
            raw_line = json.dumps(timed_event, ensure_ascii=False, sort_keys=True)
        except TypeError:
            raw_line = str(timed_event)
    event_type = str(timed_event.get("type") or "log").strip() or "log"
    display_line = _run_feed_display_line(timed_event)
    row = storage.create_run_event(
        run_id,
        event_type=event_type[:80],
        body=(display_line or raw_line)[:8000],
        metadata={
            **{k: v for k, v in timed_event.items() if k not in {"_raw_line", "_timing_annotated"}},
            "raw_line": raw_line[:8000],
            "display_body": str(display_line or "").strip(),
        },
    )
    return int(row["id"])


def _capture_thread_id_from_event(
    event: dict[str, Any],
    *,
    run_id: int | None = None,
    creature_id: int | None = None,
    conversation_id: int | None = None,
    meta_key: str | None = None,
) -> str:
    if str(event.get("type") or "").strip() != "thread.started":
        return ""
    thread_id = str(event.get("thread_id") or "").strip()
    if not thread_id:
        return ""
    if run_id is not None:
        storage.set_run_thread_id(int(run_id), thread_id)
    if creature_id is not None:
        storage.set_thread_id(int(creature_id), thread_id)
    if conversation_id is not None:
        storage.set_conversation_thread_id(int(conversation_id), thread_id)
    if meta_key:
        storage.set_meta(meta_key, thread_id)
    return thread_id


def _remember_active_run_thread(run_id: int, thread: threading.Thread) -> None:
    with _ACTIVE_RUN_THREADS_LOCK:
        _ACTIVE_RUN_THREADS[run_id] = thread


def _forget_active_run_thread(run_id: int) -> None:
    with _ACTIVE_RUN_THREADS_LOCK:
        _ACTIVE_RUN_THREADS.pop(run_id, None)


def _render_run_artifact_markdown(
    creature: Any,
    *,
    run_id: int,
    trigger_type: str,
    conversation: Any,
    sandbox_mode: str,
    report: dict[str, Any],
) -> str:
    return _render_run_markdown(
        creature=creature,
        run_id=run_id,
        report=report,
        trigger_type=trigger_type,
        conversation=conversation,
        sandbox_mode=sandbox_mode,
    )


def _replace_agenda_items(
    creature: Any,
    items: list[dict[str, Any]],
    *,
    source_run_id: int | None = None,
    source_message_id: int | None = None,
) -> None:
    storage.replace_agenda_items(
        int(creature["id"]),
        items,
        source_run_id=source_run_id,
        source_message_id=source_message_id,
    )
    _refresh_agenda_doc(creature)


def _replace_backlog_items(
    creature: Any,
    items: list[str],
    *,
    source_run_id: int | None = None,
    source_message_id: int | None = None,
) -> None:
    storage.replace_backlog_items(
        int(creature["id"]),
        items,
        source_run_id=source_run_id,
        source_message_id=source_message_id,
    )
    _refresh_backlog_doc(creature)


def _rewrite_backlog(creature: Any) -> None:
    _refresh_backlog_doc(creature)


def _apply_memory_actions(
    creature: Any,
    *,
    memory_actions: list[dict[str, Any]],
    actor_type: str,
    source_message_id: int | None = None,
    source_run_id: int | None = None,
) -> list[dict[str, Any]]:
    applied: list[dict[str, Any]] = []
    if not memory_actions:
        return applied
    for action in memory_actions:
        metadata: dict[str, Any] | None = None
        kind = _normalize_memory_kind(action["kind"])

        if kind == "routine" and not _routine_memory_allowed(source_run_id=source_run_id):
            continue

        if action["action"] == "remember":
            existing = _matching_active_memory_record(creature, kind=kind, body=action["body"])
            if existing is not None:
                refreshed = _refresh_existing_memory_record(
                    creature,
                    record=existing,
                    actor_type=actor_type,
                    reason=action["reason"],
                    source_message_id=source_message_id,
                    source_run_id=source_run_id,
                    metadata=metadata,
                )
                applied.append({**action, "kind": kind, "applied": True, "existing_record_id": int(refreshed["id"])})
                continue
            record = _create_memory_record(
                creature,
                kind=kind,
                body=action["body"],
                actor_type=actor_type,
                reason=action["reason"],
                source_message_id=source_message_id,
                source_run_id=source_run_id,
                metadata=metadata,
            )
            applied.append({**action, "applied": True, "new_record_id": int(record["id"])})
            continue

        original = storage.get_memory_record(int(action["record_id"]))
        if original is None or int(original["creature_id"]) != int(creature["id"]):
            continue

        if action["action"] in {"correct", "supersede"}:
            replacement = _create_memory_record(
                creature,
                kind=kind or str(original["kind"] or "note"),
                body=action["body"],
                actor_type=actor_type,
                reason=action["reason"],
                source_message_id=source_message_id,
                source_run_id=source_run_id,
                previous_record_id=int(original["id"]),
                metadata=metadata,
                event_action=action["action"],
            )
            _update_memory_status(
                creature,
                record_id=int(original["id"]),
                status="superseded",
                actor_type=actor_type,
                action=action["action"],
                reason=action["reason"],
                superseded_by_id=int(replacement["id"]),
                metadata=metadata,
            )
            applied.append({**action, "applied": True, "new_record_id": int(replacement["id"])})
            continue

        new_status = "revoked" if action["action"] == "revoke" else "deleted"
        _update_memory_status(
            creature,
            record_id=int(original["id"]),
            status=new_status,
            actor_type=actor_type,
            action=action["action"],
            reason=action["reason"],
            metadata=metadata,
        )
        applied.append({**action, "applied": True})
    if applied:
        _refresh_memory_doc(creature)
    return applied


def _apply_doc_updates(creature: Any, report: dict[str, Any], *, source_run_id: int | None = None) -> None:
    _ensure_creature_documents(creature)
    if report["purpose_update"].strip() and _allow_purpose_updates(creature):
        _write_doc_text(creature, PURPOSE_DOC_KEY, report["purpose_update"])
    if report["agenda_items_provided"]:
        _replace_agenda_items(creature, report["agenda_items"], source_run_id=source_run_id)
    if report["backlog_items_provided"]:
        _replace_backlog_items(creature, report["backlog_items"], source_run_id=source_run_id)
    elif report["suggestions"]:
        existing_backlog = _backlog_items_state(int(creature["id"]))
        merged_backlog = existing_backlog[:]
        for suggestion in report["suggestions"]:
            if suggestion not in merged_backlog:
                merged_backlog.append(suggestion)
            if len(merged_backlog) >= 12:
                break
        _replace_backlog_items(creature, merged_backlog, source_run_id=source_run_id)
def _run_scope_value(row: Any) -> str:
    if row is None:
        return RUN_SCOPE_ACTIVITY
    metadata = _parse_json(str(_row_value(row, "metadata_json") or ""))
    scope = str(
        metadata.get("run_scope")
        or _row_value(row, "run_scope")
        or (RUN_SCOPE_CHAT if _row_value(row, "conversation_id") is not None else RUN_SCOPE_ACTIVITY)
    ).strip()
    return scope or RUN_SCOPE_ACTIVITY


def _is_background_activity_run(row: Any) -> bool:
    if row is None or _run_scope_value(row) != RUN_SCOPE_ACTIVITY:
        return False
    metadata = _parse_json(str(_row_value(row, "metadata_json") or ""))
    if metadata.get("habit_id") or metadata.get("task_id"):
        return True
    trigger_type = str(_row_value(row, "trigger_type") or "").strip().lower()
    return bool(trigger_type and trigger_type not in {"bootstrap", "manual"})


def _is_visible_activity_report_run(row: Any) -> bool:
    if row is None:
        return False
    if str(_row_value(row, "status") or "").strip().lower() != "completed":
        return False
    if not _is_background_activity_run(row):
        return False
    return _run_request_kind(row) != "introduction"


def _decorate_run(row: Any) -> dict[str, Any]:
    data = dict(row)
    metadata = _parse_json(str(data.get("metadata_json") or ""))
    data["metadata"] = metadata
    data["run_scope"] = _run_scope_value(row)
    data["request_kind"] = _run_request_kind({"metadata": metadata, **data})
    data["started_at_display"] = _format_timestamp_display(data.get("started_at"))
    data["finished_at_display"] = _format_timestamp_display(data.get("finished_at"))
    data["started_at_compact_display"] = _format_timestamp_compact_display(data.get("started_at"))
    data["finished_at_compact_display"] = _format_timestamp_compact_display(data.get("finished_at"))
    data["evidence"] = _parse_list(metadata.get("evidence"), limit=4, item_limit=180)
    data["suggestions"] = _parse_list(metadata.get("suggestions"), limit=5, item_limit=220)
    data["files_touched"] = _parse_list(metadata.get("files_touched"), limit=8, item_limit=180)
    data["tests_run"] = _parse_list(metadata.get("tests_run"), limit=8, item_limit=220)
    data["agenda_items"] = _parse_agenda_items(metadata.get("agenda_items"))
    data["backlog_items"] = _parse_backlog_items(metadata.get("backlog_items"))
    data["memory_actions"] = _parse_memory_actions(metadata.get("memory_actions"))
    data["activity_note"] = str(metadata.get("activity_note") or "")
    data["activity_markdown"] = str(metadata.get("activity_markdown") or "")
    data["standing_message"] = str(data.get("message_text") or "")
    data["next_focus"] = str(metadata.get("next_focus") or "")
    data["spawned_conversation_title"] = str(metadata.get("spawned_conversation_title") or "")
    data["owner_mode"] = str(metadata.get("owner_mode") or "")
    raw_habit_id = metadata.get("habit_id")
    if raw_habit_id in {None, "", 0, "0"}:
        raw_habit_id = metadata.get("task_id")
    data["habit_id"] = int(raw_habit_id or 0) if str(raw_habit_id or "").strip() else 0
    data["habit_title"] = str(metadata.get("habit_title") or metadata.get("task_title") or "")
    data["habit_slug"] = str(metadata.get("habit_slug") or metadata.get("task_slug") or "")
    data["habit_schedule_summary"] = str(
        metadata.get("habit_schedule_summary") or metadata.get("task_schedule_summary") or ""
    )
    data["previous_thread_id"] = str(metadata.get("previous_thread_id") or "")
    data["new_thread_id"] = str(metadata.get("new_thread_id") or metadata.get("thread_id") or "")
    data["notes_markdown"] = str(data.get("notes_markdown") or "")
    data["notes_storage"] = (
        str(data.get("notes_path") or "")
        or ("SQLite / runs.notes_markdown" if data["notes_markdown"] else "")
    )
    return data


def _focus_hint_for_creature(creature: Any) -> str:
    return "Start with the files most relevant to the current concern."


def _resolve_conversation_for_run(creature: Any, *, conversation_id: int | None) -> Any:
    creature_id = int(creature["id"])
    if conversation_id is not None:
        conversation = storage.get_conversation_for_creature(creature_id, conversation_id)
        if conversation is None:
            raise KeyError(f"Conversation {conversation_id} does not belong to creature {creature['slug']}")
        return conversation
    pending = storage.pending_conversation(creature_id)
    if pending is not None:
        return pending
    latest = storage.get_latest_conversation(creature_id)
    if latest is not None:
        return latest
    return storage.create_conversation(creature_id, title=NEW_CHAT_TITLE)


def _auto_spawn_conversation_from_agenda(creature: Any, run_id: int, report: dict[str, Any]) -> dict[str, Any] | None:
    for item in report["agenda_items"]:
        if item["priority"] not in AUTO_SPAWN_PRIORITIES or not item["spawn_conversation"]:
            continue
        title = _clip_conversation_title(str(item["title"] or "").strip()) or "Worklist follow-up"
        existing = storage.find_conversation_by_title(int(creature["id"]), title)
        if existing is not None:
            return dict(existing)
        conversation = storage.create_conversation(
            int(creature["id"]),
            title=title,
            source_run_id=run_id,
        )
        body_lines = [
            f"Auto-spawned from the {item['priority']} priority agenda.",
            f"Title: {item['title']}",
        ]
        if item["details"]:
            body_lines.append(item["details"])
        storage.create_message(
            int(creature["id"]),
            conversation_id=int(conversation["id"]),
            role="system",
            body="\n".join(body_lines),
            run_id=run_id,
            metadata={"agenda_item": item},
        )
        return dict(conversation)
    return None


def _bootstrap_creature(
    creature: Any,
    *,
    focus_hint: str,
    intro_context: dict[str, Any] | None = None,
    on_event: Callable[[dict[str, Any]], None] | None = None,
    active_thread: threading.Thread | None = None,
) -> dict[str, Any]:
    _ensure_creature_documents(creature)
    prompt_text = _bootstrap_prompt(creature, focus_hint=focus_hint)
    run_row = storage.create_run(
        int(creature["id"]),
        trigger_type="bootstrap",
        prompt_text=prompt_text,
        thread_id=None,
        conversation_id=None,
        run_scope=RUN_SCOPE_ACTIVITY,
        sandbox_mode="read-only",
    )
    run_id = int(run_row["id"])
    if active_thread is not None:
        _remember_active_run_thread(run_id, active_thread)
    try:
        def forward_event(event: dict[str, Any]) -> None:
            timed_event = _annotate_run_feed_event_timing(run_id, event)
            _capture_thread_id_from_event(timed_event, run_id=run_id, creature_id=int(creature["id"]))
            stored_event_id = _append_run_feed_event(run_id, timed_event)
            if on_event is not None:
                on_event({**timed_event, "_run_id": run_id, "_stored_event_id": stored_event_id})
        started_event = storage.create_run_event(
            run_id,
            event_type="status",
            body='{"type":"status","phase":"started","sandbox_mode":"read-only"}',
            metadata={"phase": "started", "sandbox_mode": "read-only"},
        )
        started_event_id = int(started_event["id"])
        if on_event is not None:
            on_event(
                {
                    "type": "status",
                    "phase": "started",
                    "sandbox_mode": "read-only",
                    "_run_id": run_id,
                    "_stored_event_id": started_event_id,
                }
            )
        result = _codex_start_thread(
            workdir=str(creature["workdir"]),
            prompt=prompt_text,
            model=_creature_model(creature),
            reasoning_effort=_creature_reasoning_effort(creature),
            sandbox_mode="read-only",
            on_event=forward_event,
        )
        new_thread_id = str(result.thread_id or "").strip()
        if new_thread_id:
            storage.set_thread_id(int(creature["id"]), new_thread_id)
        report = _parse_report(result.final_text, fallback_prefix=f"{creature['display_name']} bootstrapped")
        applied_memory_actions = _apply_memory_actions(
            creature,
            memory_actions=report["memory_actions"],
            actor_type="creature",
            source_run_id=run_id,
            source_message_id=None,
        )
        _apply_doc_updates(creature, report, source_run_id=run_id)
        notes_markdown = _render_run_artifact_markdown(
            creature,
            run_id=run_id,
            trigger_type="bootstrap",
            conversation=None,
            sandbox_mode="read-only",
            report=report,
        )
        refreshed_creature = storage.get_creature(int(creature["id"])) or creature
        _rewrite_backlog(refreshed_creature)
        storage.finish_run(
            run_id,
            creature_id=int(creature["id"]),
            status="completed",
            raw_output_text=result.final_text,
            summary=report["summary"],
            severity=report["severity"],
            message_text=None,
            error_text=None,
            next_run_at=_creature_next_due_habit_at(int(refreshed_creature["id"])),
            metadata={
                **report,
                "run_scope": RUN_SCOPE_ACTIVITY,
                "previous_thread_id": "",
                "new_thread_id": new_thread_id,
                "memory_actions": applied_memory_actions,
                "should_notify": False,
                "request_kind": "bootstrap",
            },
            notes_markdown=notes_markdown,
        )
        if not _is_keeper_creature(refreshed_creature):
            _schedule_followup_after_run(int(refreshed_creature["id"]))
            _queue_initial_intro_followup(refreshed_creature)
        storage.create_run_event(
            run_id,
            event_type="status",
            body='{"type":"status","phase":"completed"}',
            metadata={"phase": "completed"},
        )
        _clear_run_feed_event_timing(run_id)
        return {
            "run_id": run_id,
            "thread_id": new_thread_id,
            "conversation_id": None,
        }
    except Exception as exc:
        error_text = _friendly_run_error(exc, sandbox_mode="read-only")
        storage.create_run_event(
            run_id,
            event_type="error",
            body=error_text,
            metadata={"error_text": error_text},
        )
        storage.create_run_event(
            run_id,
            event_type="status",
            body='{"type":"status","phase":"failed"}',
            metadata={"phase": "failed"},
        )
        _clear_run_feed_event_timing(run_id)
        storage.finish_run(
            run_id,
            creature_id=int(creature["id"]),
            status="failed",
            raw_output_text=None,
            summary=None,
            severity="critical",
            message_text=None,
            error_text=error_text,
            next_run_at=_creature_next_due_habit_at(int(creature["id"])),
            metadata={"run_scope": RUN_SCOPE_ACTIVITY},
            notes_markdown=None,
        )
        raise
    finally:
        if active_thread is not None:
            _forget_active_run_thread(run_id)


def _bootstrap_creature_in_background(
    *,
    creature_id: int,
    focus_hint: str,
    intro_context: dict[str, Any] | None,
    db_path: str,
    data_dir: str,
    on_event: Callable[[dict[str, Any]], None] | None = None,
) -> None:
    try:
        with config.override_runtime_paths(db_path=db_path, data_dir=data_dir):
            creature = storage.get_creature(creature_id)
            if creature is None:
                return
            _bootstrap_creature(
                creature,
                focus_hint=focus_hint,
                intro_context=intro_context,
                on_event=on_event,
                active_thread=threading.current_thread(),
            )
    except Exception:
        return


def _queue_creature_bootstrap(
    creature: Any,
    *,
    focus_hint: str,
    intro_context: dict[str, Any] | None = None,
    on_event: Callable[[dict[str, Any]], None] | None = None,
) -> None:
    thread = threading.Thread(
        target=_bootstrap_creature_in_background,
        kwargs={
            "creature_id": int(creature["id"]),
            "focus_hint": focus_hint,
            "intro_context": intro_context,
            "db_path": str(config.db_path()),
            "data_dir": str(config.data_dir()),
            "on_event": on_event,
        },
        daemon=True,
    )
    thread.start()

def ensure_initial_creatures() -> list[dict[str, Any]]:
    _initialize_runtime()
    prewarm_onboarding_assets()
    return [_row_to_dict(row) for row in storage.list_creatures()]


def _write_summoning_context_memory(
    slug: str,
    *,
    brief: str,
    origin_context: str = "",
    opening_question: str = "",
) -> None:
    creature = storage.get_creature_by_slug(slug)
    if creature is None:
        return
    existing = _memory_records(int(creature["id"]), include_inactive=True)
    normalized_brief = " ".join(str(brief or "").strip().split())
    normalized_origin_context = _normalize_origin_context_text(origin_context)
    normalized_opening_question = _normalize_intro_question_text(opening_question)
    if normalized_brief and not any(record["body"] == f"Original brief: {normalized_brief}" for record in existing):
        _create_memory_record(
            creature,
            kind="context",
            body=f"Original brief: {normalized_brief}",
            actor_type="system",
            reason="Summoning brief captured at creature creation.",
            event_action="remember",
            metadata={"source": "summoning", "context_type": "brief"},
        )
    if normalized_origin_context and not any(record["body"] == f"Origin context: {normalized_origin_context}" for record in existing):
        _create_memory_record(
            creature,
            kind="context",
            body=f"Origin context: {normalized_origin_context}",
            actor_type="system",
            reason="Keeper handoff context captured at creature creation.",
            event_action="remember",
            metadata={"source": "summoning", "context_type": "origin_context"},
        )
    if normalized_opening_question and not any(record["body"] == f"Suggested opening question: {normalized_opening_question}" for record in existing):
        _create_memory_record(
            creature,
            kind="context",
            body=f"Suggested opening question: {normalized_opening_question}",
            actor_type="system",
            reason="The Keeper suggested the first question this creature should ask.",
            event_action="remember",
            metadata={"source": "summoning", "context_type": "opening_question"},
        )


def create_creature(
    *,
    display_name: str,
    system_role: str = "",
    is_pinned: bool = False,
    can_delete: bool = True,
    ecosystem: str = "",
    purpose_summary: str = "",
    summoning_brief: str = "",
    origin_context: str = "",
    opening_question: str = "",
    purpose_markdown: str = "",
    temperament: str = DEFAULT_TEMPERAMENT,
    concern: str,
    public_prompt: str = "",
    slug: str | None = None,
    intro_context: dict[str, Any] | None = None,
    on_bootstrap_event: Callable[[dict[str, Any]], None] | None = None,
    bootstrap_async: bool = False,
    bootstrap: bool = True,
) -> dict[str, Any]:
    _initialize_runtime()
    cleaned_name = " ".join(display_name.strip().split())
    cleaned_concern = " ".join(concern.strip().split())
    cleaned_ecosystem = _normalize_ecosystem(ecosystem)
    cleaned_purpose_summary = " ".join(str(purpose_summary or "").strip().split()) or cleaned_concern
    cleaned_temperament = _normalize_temperament(temperament)
    if not cleaned_name or not cleaned_concern:
        raise ValueError("display_name and concern are required")
    final_slug = _slugify(slug or cleaned_name)
    if storage.get_creature_by_slug(final_slug) is not None:
        raise ValueError(f"Creature slug already exists: {final_slug}")
    _ensure_creature_workshop(final_slug)
    system_prompt = public_prompt.strip() or (
        f"You are {cleaned_name}. Your concern is: {cleaned_concern}. "
        "Use the workspace, the browser, and your workshop when they help, stay grounded in evidence, and keep owner-facing updates concise."
    )
    creature = storage.save_creature(
        slug=final_slug,
        display_name=cleaned_name,
        system_role=str(system_role or "").strip(),
        is_pinned=is_pinned,
        can_delete=can_delete,
        ecosystem=cleaned_ecosystem,
        purpose_summary=cleaned_purpose_summary,
        temperament=cleaned_temperament,
        concern=cleaned_concern,
        system_prompt=system_prompt,
        workdir=str(config.workspace_root()),
    )
    creature = storage.get_creature(int(creature["id"])) or creature
    if summoning_brief.strip() or origin_context.strip() or opening_question.strip():
        _write_summoning_context_memory(
            str(creature["slug"]),
            brief=summoning_brief,
            origin_context=origin_context,
            opening_question=opening_question,
        )
        creature = storage.get_creature(int(creature["id"])) or creature
    _ensure_creature_documents(creature)
    if not _is_keeper_creature(creature):
        _ensure_default_ponder_habit(creature)
    normalized_purpose_markdown = _normalize_purpose_markdown(purpose_markdown)
    if normalized_purpose_markdown:
        _write_doc_text(creature, PURPOSE_DOC_KEY, normalized_purpose_markdown)
    if bootstrap_async and bootstrap:
        _queue_creature_bootstrap(
            creature,
            focus_hint="Start with the files most relevant to the stated concern.",
            intro_context=intro_context,
            on_event=on_bootstrap_event,
        )
    elif bootstrap:
        _bootstrap_creature(
            creature,
            focus_hint="Start with the files most relevant to the stated concern.",
            intro_context=intro_context,
            on_event=on_bootstrap_event,
        )
    return dict(storage.get_creature(int(creature["id"])))


def _is_ponder_habit(habit: Mapping[str, Any] | None) -> bool:
    if not habit:
        return False
    slug = str(habit.get("slug") or "").strip().lower()
    title = str(habit.get("title") or "").strip().lower()
    return slug == PONDER_HABIT_SLUG or title == PONDER_HABIT_TITLE.lower()


def _ponder_habit_instructions(creature: Any) -> str:
    purpose = str(_row_value(creature, "purpose_summary") or _row_value(creature, "concern") or "").strip()
    purpose_sentence = _ensure_sentence(purpose) if purpose else "Stay close to your purpose."
    return (
        f"{purpose_sentence} Two hours after the most recent real chat activity, take a quiet pass through your recent "
        "conversations with the human and reflect on what still feels unclear, interesting, unresolved, or worth asking about next. "
        "Also reflect on the state of your other habits, excluding Ponder itself: notice any friction, failures, missing instructions, "
        "or repeated trouble. If you have a real concern, question, or request for the human, prepare it so it can greet them the next "
        "time they open a chat with you. If you genuinely have nothing worth raising, stay quiet."
    )


def _ensure_default_ponder_habit(creature: Any) -> dict[str, Any] | None:
    if creature is None or _is_keeper_creature(creature):
        return None
    for row in storage.list_habits(int(creature["id"]), include_disabled=True, limit=200):
        decorated = _decorate_habit({**dict(row), "creature_slug": str(_row_value(creature, "slug") or "")})
        if _is_ponder_habit(decorated):
            return decorated
    schedule_kind, schedule_json = _normalize_habit_schedule(
        HABIT_SCHEDULE_AFTER_CHAT,
        {"after_minutes": DEFAULT_PONDER_DELAY_MINUTES},
    )
    habit = storage.create_habit(
        int(creature["id"]),
        slug=PONDER_HABIT_SLUG,
        title=PONDER_HABIT_TITLE,
        instructions=_ponder_habit_instructions(creature),
        schedule_kind=schedule_kind,
        schedule_json=schedule_json,
        enabled=True,
        next_run_at=None,
    )
    return _decorate_habit({**dict(habit), "creature_slug": str(_row_value(creature, "slug") or "")})


def _creature_habits_state(creature: Any) -> list[dict[str, Any]]:
    habits: list[dict[str, Any]] = []
    creature_slug = str(_row_value(creature, "slug") or "")
    for row in storage.list_habits(int(creature["id"]), include_disabled=True, limit=200):
        habit = _decorate_habit(row)
        habit["creature_slug"] = creature_slug
        habit["workshop_paths"] = {key: str(path) for key, path in _creature_workshop_paths(creature_slug).items()}
        habits.append(habit)
    return habits


def create_creature_habit(
    slug: str,
    *,
    title: str,
    instructions: str,
    schedule_kind: str,
    every_minutes: int | None = None,
    after_minutes: int | None = None,
    daily_time: str | None = None,
    times_per_day: int | None = None,
    window_start: str | None = None,
    window_end: str | None = None,
    enabled: bool = True,
) -> dict[str, Any]:
    _initialize_runtime()
    normalized_slug = str(canonical_creature_slug(slug))
    creature = storage.get_creature_by_slug(normalized_slug)
    if creature is None:
        raise KeyError(f"Unknown creature slug: {slug}")
    cleaned_title = " ".join(str(title or "").strip().split())
    cleaned_instructions = str(instructions or "").strip()
    if not cleaned_title or not cleaned_instructions:
        raise ValueError("Habit name and instructions are required.")
    schedule_input: dict[str, Any] = {}
    if schedule_kind == HABIT_SCHEDULE_INTERVAL:
        schedule_input = {
            "every_minutes": max(5, int(every_minutes or 30)),
            "window_start": window_start or DEFAULT_HABIT_WINDOW_START,
            "window_end": window_end or DEFAULT_HABIT_WINDOW_END,
        }
    elif schedule_kind == HABIT_SCHEDULE_AFTER_CHAT:
        schedule_input = {
            "after_minutes": max(5, int(after_minutes or DEFAULT_PONDER_DELAY_MINUTES)),
        }
    elif schedule_kind == HABIT_SCHEDULE_DAILY:
        schedule_input = {"time": daily_time or "08:00"}
    elif schedule_kind == HABIT_SCHEDULE_TIMES_PER_DAY:
        schedule_input = {
            "times_per_day": max(2, int(times_per_day or 3)),
            "window_start": window_start or "08:00",
            "window_end": window_end or "20:00",
        }
    normalized_kind, normalized_schedule = _normalize_habit_schedule(schedule_kind, schedule_input)
    next_run_at = _habit_next_run_at(normalized_kind, normalized_schedule) if enabled else None
    habit = storage.create_habit(
        int(creature["id"]),
        slug=_normalize_habit_slug(cleaned_title),
        title=cleaned_title,
        instructions=cleaned_instructions,
        schedule_kind=normalized_kind,
        schedule_json=normalized_schedule,
        enabled=enabled,
        next_run_at=next_run_at,
    )
    return _decorate_habit({**dict(habit), "creature_slug": normalized_slug})


def set_creature_habit_enabled(slug: str, habit_id: int, *, enabled: bool) -> dict[str, Any]:
    _initialize_runtime()
    normalized_slug = str(canonical_creature_slug(slug))
    creature = storage.get_creature_by_slug(normalized_slug)
    if creature is None:
        raise KeyError(f"Unknown creature slug: {slug}")
    habit = storage.get_habit_for_creature(int(creature["id"]), habit_id)
    if habit is None:
        raise KeyError(f"Unknown habit id: {habit_id}")
    schedule_kind, schedule_json = _normalize_habit_schedule(
        str(habit["schedule_kind"] or ""),
        _habit_schedule_json(habit["schedule_json"]),
    )
    next_run_at = _habit_next_run_at(schedule_kind, schedule_json) if enabled else None
    if enabled:
        storage.resume_habit(habit_id, next_run_at=next_run_at)
    else:
        storage.pause_habit(habit_id)
    updated = storage.get_habit(habit_id)
    return _decorate_habit({**dict(updated or habit), "creature_slug": normalized_slug})


def delete_creature_habit(slug: str, habit_id: int) -> None:
    _initialize_runtime()
    normalized_slug = str(canonical_creature_slug(slug))
    creature = storage.get_creature_by_slug(normalized_slug)
    if creature is None:
        raise KeyError(f"Unknown creature slug: {slug}")
    habit = storage.get_habit_for_creature(int(creature["id"]), habit_id)
    if habit is None:
        raise KeyError(f"Unknown habit id: {habit_id}")
    storage.delete_habit(habit_id)


def run_creature_habit_now(slug: str, habit_id: int) -> dict[str, Any]:
    _initialize_runtime()
    normalized_slug = str(canonical_creature_slug(slug))
    creature = storage.get_creature_by_slug(normalized_slug)
    if creature is None:
        raise KeyError(f"Unknown creature slug: {slug}")
    habit = storage.get_habit_for_creature(int(creature["id"]), habit_id)
    if habit is None:
        raise KeyError(f"Unknown habit id: {habit_id}")
    storage.touch_habit_now(habit_id)
    return start_background_run(
        normalized_slug,
        trigger_type="habit",
        force_message=True,
        allow_code_changes=True,
        habit_id=habit_id,
        run_scope=RUN_SCOPE_ACTIVITY,
    )


def rename_creature_display_name(slug: str, *, display_name: str) -> dict[str, Any]:
    _initialize_runtime()
    slug = str(canonical_creature_slug(slug))
    creature = storage.get_creature_by_slug(slug)
    if creature is None:
        raise KeyError(f"Unknown creature slug: {slug}")
    cleaned_name = " ".join(display_name.strip().split())[:MAX_CREATURE_DISPLAY_NAME_CHARS].rstrip()
    if not cleaned_name:
        raise ValueError("Display name must not be empty")
    original_name = str(creature["display_name"]).strip()
    if cleaned_name == original_name:
        return dict(creature)

    system_prompt = str(creature["system_prompt"] or "")
    if original_name and original_name in system_prompt:
        system_prompt = system_prompt.replace(original_name, cleaned_name)
    storage.rename_creature_identity(
        int(creature["id"]),
        slug=str(creature["slug"]),
        display_name=cleaned_name,
        ecosystem=str(creature["ecosystem"] or ""),
        purpose_summary=str(creature["purpose_summary"] or creature["concern"] or ""),
        temperament=str(creature["temperament"] or DEFAULT_TEMPERAMENT),
        concern=str(creature["concern"]),
        system_prompt=system_prompt,
    )
    replacements = {original_name: cleaned_name} if original_name else {}
    if replacements:
        for old_text, new_text in replacements.items():
            storage.rewrite_state_surface_text(int(creature["id"]), doc_key=PURPOSE_DOC_KEY, old_text=old_text, new_text=new_text)
    updated = storage.get_creature(int(creature["id"]))
    if updated is not None:
        _refresh_purpose_doc(updated)
        _refresh_intro_message(updated)
    return dict(updated or creature)


def create_conversation(
    slug: str,
    *,
    title: str | None = None,
    source_run_id: int | None = None,
) -> dict[str, Any]:
    _initialize_runtime()
    slug = str(canonical_creature_slug(slug))
    creature = storage.get_creature_by_slug(slug)
    if creature is None:
        raise KeyError(f"Unknown creature slug: {slug}")
    conversation = storage.create_conversation(
        int(creature["id"]),
        title=title or NEW_CHAT_TITLE,
        source_run_id=source_run_id,
    )
    return dict(conversation)


def _habit_teaching_message(creature: Mapping[str, Any]) -> str:
    return "\n\n".join(
        [
            f"I'm ready to learn a new habit with you.",
            "A habit is recurring work I can carry on with a clear rhythm, a clear trigger, and a clear sense of what a good result looks like.",
            f"Tell me what you want me to keep watch on, when you want me to act, and what you want me to bring back to you. We can shape it together before it starts running.",
        ]
    )


def create_habit_teaching_conversation(slug: str) -> dict[str, Any]:
    _initialize_runtime()
    slug = str(canonical_creature_slug(slug))
    creature = storage.get_creature_by_slug(slug)
    if creature is None:
        raise KeyError(f"Unknown creature slug: {slug}")
    conversation = storage.create_conversation(
        int(creature["id"]),
        title=NEW_HABIT_CHAT_TITLE,
    )
    storage.create_message(
        int(creature["id"]),
        conversation_id=int(conversation["id"]),
        role="creature",
        body=_habit_teaching_message(creature),
        metadata={"source": "habit_teaching"},
    )
    return dict(storage.get_conversation(int(conversation["id"])) or conversation)


def rename_conversation(slug: str, conversation_id: int, *, title: str) -> dict[str, Any]:
    _initialize_runtime()
    slug = str(canonical_creature_slug(slug))
    creature = storage.get_creature_by_slug(slug)
    if creature is None:
        raise KeyError(f"Unknown creature slug: {slug}")
    conversation = storage.get_conversation_for_creature(int(creature["id"]), conversation_id)
    if conversation is None:
        raise KeyError(f"Conversation {conversation_id} does not belong to creature {slug}")
    cleaned_title = " ".join(title.strip().split())
    if not cleaned_title:
        raise ValueError("Conversation title must not be empty")
    storage.rename_conversation(conversation_id, cleaned_title)
    updated = storage.get_conversation(conversation_id)
    return dict(updated)


def delete_conversation(
    slug: str,
    conversation_id: int,
    *,
    current_conversation_id: int | None = None,
) -> dict[str, Any]:
    _initialize_runtime()
    slug = str(canonical_creature_slug(slug))
    creature = storage.get_creature_by_slug(slug)
    if creature is None:
        raise KeyError(f"Unknown creature slug: {slug}")
    conversation = storage.get_conversation_for_creature(int(creature["id"]), conversation_id)
    if conversation is None:
        raise KeyError(f"Conversation {conversation_id} does not belong to creature {slug}")

    _interrupt_running_conversation_run(conversation_id, error_text=CONVERSATION_RESET_RUN_ERROR_TEXT)
    storage.delete_conversation(conversation_id)

    fallback = None
    if current_conversation_id is not None and int(current_conversation_id) != int(conversation_id):
        fallback = storage.get_conversation_for_creature(int(creature["id"]), int(current_conversation_id))
    if fallback is None:
        fallback = storage.get_latest_conversation(int(creature["id"]))
    return {
        "deleted_conversation_id": int(conversation_id),
        "redirect_conversation_id": int(fallback["id"]) if fallback is not None else None,
    }


def spawn_conversation_from_run(slug: str, run_id: int, *, body_override: str | None = None) -> dict[str, Any]:
    _initialize_runtime()
    slug = str(canonical_creature_slug(slug))
    creature = storage.get_creature_by_slug(slug)
    if creature is None:
        raise KeyError(f"Unknown creature slug: {slug}")
    run = storage.get_run(run_id)
    if run is None or int(run["creature_id"]) != int(creature["id"]):
        raise KeyError(f"Run {run_id} does not belong to creature {slug}")
    existing = storage.find_conversation_by_source_run(run_id)
    if existing is not None:
        return dict(existing)
    run_data = _decorate_run(run)
    trigger_type = str(run_data.get("trigger_type") or "").strip().lower()
    request_kind = _run_request_kind(run_data)
    surface_body = " ".join(str(body_override or "").strip().split()) or _run_surface_message(run_data)
    conversation = storage.create_conversation(
        int(creature["id"]),
        title=(
            INTRODUCTION_CHAT_TITLE
            if request_kind == "introduction"
            else _conversation_title_from_run_data(run_data)
        ),
        source_run_id=run_id,
    )
    body = surface_body
    message_metadata = {
        "source_run_id": run_id,
        "run_scope": RUN_SCOPE_ACTIVITY,
        "trigger_type": str(run_data.get("trigger_type") or ""),
        "severity": str(run_data.get("severity") or "info"),
    }
    storage.create_message(
        int(creature["id"]),
        conversation_id=int(conversation["id"]),
        role="creature",
        body=body,
        run_id=run_id,
        metadata=message_metadata,
    )
    return dict(storage.get_conversation(int(conversation["id"])))


def _decorate_notification(row: Any, *, ecosystem_value: str) -> dict[str, Any]:
    data = _row_to_dict(row) or {}
    metadata = _parse_json(str(data.get("metadata_json") or ""))
    severity = str(data.get("severity") or metadata.get("severity") or "").strip().lower()
    data["metadata"] = metadata
    data["created_at_display"] = _format_timestamp_display(data.get("requested_at") or data.get("created_at"))
    data["preview"] = _notification_preview(str(data.get("message_text") or data.get("body") or ""))
    data["severity"] = severity or "info"
    data["has_priority"] = severity in {"high", "critical"}
    data["teaser"] = _chat_request_teaser(data, ecosystem_value=ecosystem_value)
    return data


def delete_creature(slug: str) -> None:
    _initialize_runtime()
    slug = str(canonical_creature_slug(slug))
    creature = storage.get_creature_by_slug(slug)
    if creature is None:
        raise KeyError(f"Unknown creature slug: {slug}")
    if not bool(_row_value(creature, "can_delete", 1)):
        raise ValueError(f"{str(_row_value(creature, 'display_name') or 'This creature')} is protected and cannot be released.")
    storage.delete_creature(int(creature["id"]))
    shutil.rmtree(_creature_storage_root(str(creature["slug"])), ignore_errors=True)


def send_user_message(
    slug: str,
    conversation_id: int | None,
    body: str,
    *,
    owner_mode: str | None = None,
    model_override: str = "",
    reasoning_effort_override: str = "",
    attachments: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    _initialize_runtime()
    slug = str(canonical_creature_slug(slug))
    creature = storage.get_creature_by_slug(slug)
    if creature is None:
        raise KeyError(f"Unknown creature slug: {slug}")
    cleaned = body.strip()
    prepared_attachments = attachments or []
    if not cleaned and not prepared_attachments:
        raise ValueError("Message body must not be empty")
    final_owner_mode = _normalize_owner_mode(owner_mode or DEFAULT_OWNER_MODE)
    final_model_override = _normalize_model_value(model_override, allow_blank=True) or None
    final_effort_override = _normalize_reasoning_effort_value(reasoning_effort_override, allow_blank=True) or None
    if conversation_id is None:
        conversation = storage.create_conversation(
            int(creature["id"]),
            title=NEW_CHAT_TITLE,
            owner_mode=final_owner_mode,
            model_override=final_model_override,
            reasoning_effort_override=final_effort_override,
        )
        conversation_id = int(conversation["id"])
    else:
        conversation = storage.get_conversation_for_creature(int(creature["id"]), conversation_id)
        if conversation is None:
            raise KeyError(f"Conversation {conversation_id} does not belong to creature {slug}")
    if final_owner_mode != _normalize_owner_mode(_row_value(conversation, "owner_mode")):
        storage.set_conversation_owner_mode(conversation_id, final_owner_mode)
    message_row = storage.create_message(
        int(creature["id"]),
        conversation_id=conversation_id,
        role="user",
        body=cleaned,
    )
    message_id = int(message_row["id"])
    stored_attachments = _store_message_attachments(message_id, prepared_attachments)
    if stored_attachments:
        storage.update_message_metadata(message_id, {"attachments": stored_attachments})
    if _conversation_uses_auto_title(str(conversation["title"] or "")):
        storage.rename_conversation(conversation_id, _conversation_title_from_message_payload(cleaned, stored_attachments))
    if _codex_access_waiting() and not poll_codex_access_recovery(force=False):
        _append_codex_waiting_notice(int(creature["id"]), conversation_id=conversation_id)
        return {
            "creature_slug": slug,
            "conversation_id": conversation_id,
            "message_id": message_id,
            "owner_mode": final_owner_mode,
            "status": "waiting",
            "waiting_message": _codex_waiting_message(kind=_load_codex_access_state_raw()["reason_kind"]),
        }
    return {
        "creature_slug": slug,
        "conversation_id": conversation_id,
        "message_id": message_id,
        "owner_mode": final_owner_mode,
        "status": "queued",
    }


def get_message_attachment(message_id: int, attachment_id: str) -> dict[str, Any]:
    _initialize_runtime()
    payload = _message_attachment_payload(int(message_id), str(attachment_id))
    path = Path(payload["disk_path"])
    if not path.exists():
        raise FileNotFoundError(f"Missing attachment file for message {message_id}: {attachment_id}")
    return payload


def set_conversation_owner_mode(slug: str, conversation_id: int, owner_mode: str) -> dict[str, Any]:
    _initialize_runtime()
    slug = str(canonical_creature_slug(slug))
    creature = storage.get_creature_by_slug(slug)
    if creature is None:
        raise KeyError(f"Unknown creature slug: {slug}")
    conversation = storage.get_conversation_for_creature(int(creature["id"]), conversation_id)
    if conversation is None:
        raise KeyError(f"Conversation {conversation_id} does not belong to creature {slug}")
    final_owner_mode = _normalize_owner_mode(owner_mode)
    storage.set_conversation_owner_mode(conversation_id, final_owner_mode)
    updated = storage.get_conversation(conversation_id)
    return _decorate_conversation(updated)


def _normalize_busy_action(value: str | None) -> str:
    return BUSY_ACTION_STEER if str(value or "").strip().lower() == BUSY_ACTION_STEER else BUSY_ACTION_QUEUE


def _schedule_followup_after_run(creature_id: int) -> None:
    creature = storage.get_creature(creature_id)
    if creature is None:
        return
    pending_conversation = storage.next_pending_conversation(creature_id)
    if pending_conversation is not None:
        owner_mode = _normalize_owner_mode(_row_value(pending_conversation, "owner_mode"))
        action = str(_row_value(pending_conversation, "pending_action") or BUSY_ACTION_QUEUE)
        result = start_background_run(
            str(creature["slug"]),
            trigger_type="user_reply",
            force_message=True,
            conversation_id=int(pending_conversation["id"]),
            allow_code_changes=_allow_code_changes_for_owner_mode(owner_mode),
            run_scope=RUN_SCOPE_CHAT,
            busy_action=action,
        )
        if str(result.get("status") or "") == "running":
            storage.clear_conversation_action(int(pending_conversation["id"]))
        return


def _prepare_run(
    slug: str,
    *,
    trigger_type: str = "manual",
    force_message: bool = False,
    conversation_id: int | None = None,
    allow_code_changes: bool = False,
    start_new_thread: bool = False,
    habit_id: int | None = None,
    run_scope: str | None = None,
) -> dict[str, Any]:
    _initialize_runtime()
    slug = str(canonical_creature_slug(slug))
    creature = storage.get_creature_by_slug(slug)
    if creature is None:
        raise KeyError(f"Unknown creature slug: {slug}")
    _ensure_creature_documents(creature)
    normalized_scope = (
        RUN_SCOPE_CHAT
        if run_scope == RUN_SCOPE_CHAT or conversation_id is not None or trigger_type == "user_reply"
        else RUN_SCOPE_ACTIVITY
    )
    if normalized_scope == RUN_SCOPE_ACTIVITY and not creature["codex_thread_id"] and not start_new_thread:
        _bootstrap_creature(creature, focus_hint=_focus_hint_for_creature(creature))
        creature = storage.get_creature_by_slug(slug)
        if creature is None or not creature["codex_thread_id"]:
            raise RuntimeError(f"Failed to awaken creature {slug}")
    habit = None
    if normalized_scope == RUN_SCOPE_ACTIVITY and habit_id is not None:
        habit_row = storage.get_habit_for_creature(int(creature["id"]), int(habit_id))
        if habit_row is None:
            raise KeyError(f"Habit {habit_id} does not belong to creature {slug}")
        habit = _decorate_habit({**dict(habit_row), "creature_slug": slug})
        trigger_type = "habit"
    initial_intro_followup = bool(
        normalized_scope == RUN_SCOPE_ACTIVITY
        and trigger_type == "followup"
        and storage.find_conversation_by_title(int(creature["id"]), INTRODUCTION_CHAT_TITLE) is None
        and _latest_intro_run(int(creature["id"])) is None
    )
    requested_code_changes = bool(allow_code_changes or habit is not None)
    if initial_intro_followup:
        requested_code_changes = False
    allow_code_changes = _allow_repo_edits(creature, requested=requested_code_changes)

    conversation = None
    conversation_had_pending_owner_reply = False
    owner_mode = DEFAULT_OWNER_MODE
    thread_id = str(creature["codex_thread_id"] or "").strip()
    if normalized_scope == RUN_SCOPE_CHAT:
        conversation = _resolve_conversation_for_run(creature, conversation_id=conversation_id)
        pending = storage.next_pending_conversation(int(creature["id"]))
        conversation_had_pending_owner_reply = pending is not None and int(pending["id"]) == int(conversation["id"])
        owner_mode = _normalize_owner_mode(_row_value(conversation, "owner_mode"))
        thread_id = str(_row_value(conversation, "codex_thread_id") or "").strip()
        if not thread_id:
            start_new_thread = True
    full_chat_context = bool(normalized_scope != RUN_SCOPE_CHAT or start_new_thread)
    sandbox_mode = "workspace-write" if allow_code_changes else "read-only"
    prompt_text = _run_prompt(
        creature,
        trigger_type=trigger_type,
        conversation=conversation,
        force_message=force_message,
        allow_code_changes=allow_code_changes,
        focus_hint=_focus_hint_for_creature(creature),
        habit=habit,
        run_scope=normalized_scope,
        full_chat_context=full_chat_context,
    )
    run_row = storage.create_run(
        int(creature["id"]),
        trigger_type=trigger_type,
        prompt_text=prompt_text,
        thread_id=thread_id or None,
        conversation_id=int(conversation["id"]) if conversation is not None else None,
        run_scope=normalized_scope,
        sandbox_mode=sandbox_mode,
    )
    run_id = int(run_row["id"])
    thinking = _thinking_state(creature, conversation if normalized_scope == RUN_SCOPE_CHAT else None)
    return {
        "slug": slug,
        "creature": creature,
        "conversation": conversation,
        "thinking": thinking,
        "run_scope": normalized_scope,
        "sandbox_mode": sandbox_mode,
        "prompt_text": prompt_text,
        "run_id": run_id,
        "trigger_type": trigger_type,
        "force_message": force_message,
        "owner_mode": owner_mode,
        "start_new_thread": start_new_thread,
        "previous_thread_id": thread_id,
        "habit": habit,
        "conversation_had_pending_owner_reply": conversation_had_pending_owner_reply,
        "db_path": str(config.db_path()),
        "data_dir": str(config.data_dir()),
    }


def _execute_prepared_run(
    prepared: dict[str, Any],
    *,
    on_event: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    slug = str(prepared["slug"])
    creature = prepared["creature"]
    conversation = prepared["conversation"]
    thinking = prepared.get("thinking") or _thinking_state(creature, conversation if conversation is not None else None)
    sandbox_mode = str(prepared["sandbox_mode"])
    prompt_text = str(prepared["prompt_text"])
    run_id = int(prepared["run_id"])
    trigger_type = str(prepared["trigger_type"])
    force_message = bool(prepared["force_message"])
    owner_mode = str(prepared.get("owner_mode") or DEFAULT_OWNER_MODE)
    start_new_thread = bool(prepared.get("start_new_thread"))
    previous_thread_id = str(prepared.get("previous_thread_id") or "")
    run_scope = str(prepared.get("run_scope") or RUN_SCOPE_ACTIVITY)
    habit = prepared.get("habit") or None
    conversation_had_pending_owner_reply = bool(prepared.get("conversation_had_pending_owner_reply"))

    try:
        def forward_event(event: dict[str, Any]) -> None:
            timed_event = _annotate_run_feed_event_timing(run_id, event)
            _capture_thread_id_from_event(
                timed_event,
                run_id=run_id,
                creature_id=int(creature["id"]) if run_scope == RUN_SCOPE_ACTIVITY else None,
                conversation_id=int(conversation["id"]) if run_scope == RUN_SCOPE_CHAT and conversation is not None else None,
            )
            stored_event_id = _append_run_feed_event(run_id, timed_event)
            if on_event is not None:
                on_event({**timed_event, "_run_id": run_id, "_stored_event_id": stored_event_id})
        started_event = storage.create_run_event(
            run_id,
            event_type="status",
            body=f'{{"type":"status","phase":"started","sandbox_mode":"{sandbox_mode}"}}',
            metadata={"phase": "started", "sandbox_mode": sandbox_mode},
        )
        started_event_id = int(started_event["id"])
        if on_event is not None:
            on_event(
                {
                    "type": "status",
                    "phase": "started",
                    "sandbox_mode": sandbox_mode,
                    "_run_id": run_id,
                    "_stored_event_id": started_event_id,
                }
            )
        if start_new_thread:
            result = _codex_start_thread(
                workdir=str(creature["workdir"]),
                prompt=prompt_text,
                model=thinking["model"],
                reasoning_effort=thinking["reasoning_effort"],
                sandbox_mode=sandbox_mode,
                on_event=forward_event,
            )
        else:
            result = _codex_resume_thread(
                workdir=str(creature["workdir"]),
                thread_id=previous_thread_id,
                prompt=prompt_text,
                model=thinking["model"],
                reasoning_effort=thinking["reasoning_effort"],
                sandbox_mode=sandbox_mode,
                on_event=forward_event,
            )
        new_thread_id = str(result.thread_id or previous_thread_id or "").strip()
        if run_scope == RUN_SCOPE_ACTIVITY and start_new_thread and new_thread_id:
            storage.set_thread_id(int(creature["id"]), new_thread_id)
            creature = storage.get_creature(int(creature["id"])) or creature
        if run_scope == RUN_SCOPE_CHAT and conversation is not None and new_thread_id:
            storage.set_conversation_thread_id(int(conversation["id"]), new_thread_id)
            conversation = storage.get_conversation(int(conversation["id"])) or conversation
        report = _parse_report(result.final_text, fallback_prefix=f"{creature['display_name']} completed a loop")
        applied_memory_actions = _apply_memory_actions(
            creature,
            memory_actions=report["memory_actions"],
            actor_type="creature",
            source_run_id=run_id,
        )
        _apply_doc_updates(creature, report, source_run_id=run_id)
        notes_markdown = _render_run_artifact_markdown(
            creature,
            run_id=run_id,
            trigger_type=trigger_type,
            conversation=conversation,
            sandbox_mode=sandbox_mode,
            report=report,
        )
        notes_path = _write_habit_report_artifact(creature, habit, run_id, notes_markdown) if habit is not None else ""
        raw_message_text = str(report["message"] or "").strip()
        should_notify = bool(report["should_notify"] or force_message)
        if run_scope == RUN_SCOPE_ACTIVITY and should_notify and not raw_message_text:
            raw_message_text = _notification_preview(
                str(report.get("summary") or report.get("activity_note") or f"{creature['display_name']} completed a pass."),
                limit=MAX_STANDING_MESSAGE_CHARS,
            )
        should_create_intro = bool(
            run_scope == RUN_SCOPE_ACTIVITY
            and not _is_keeper_creature(creature)
            and trigger_type != "bootstrap"
            and raw_message_text
            and _latest_intro_run(int(creature["id"])) is None
            and storage.find_conversation_by_title(int(creature["id"]), INTRODUCTION_CHAT_TITLE) is None
        )
        final_message_text = raw_message_text
        request_kind = "introduction" if should_create_intro else PONDER_REQUEST_KIND if habit is not None and _is_ponder_habit(habit) else "activity"
        if should_create_intro:
            final_message_text = _introduction_message(
                creature,
                concern=str(_row_value(creature, "purpose_summary") or _row_value(creature, "concern") or "").strip(),
                first_impression=raw_message_text,
            )
        should_notify = bool(
            should_create_intro
            or should_notify
        )
        message_id: int | None = None
        spawned_conversation = None
        message_conversation_id = int(conversation["id"]) if conversation is not None else 0
        if should_create_intro and message_conversation_id == 0:
            intro_conversation = storage.create_conversation(
                int(creature["id"]),
                title=INTRODUCTION_CHAT_TITLE,
                source_run_id=run_id,
            )
            conversation = intro_conversation
            spawned_conversation = intro_conversation
            message_conversation_id = int(intro_conversation["id"])
        elif run_scope == RUN_SCOPE_CHAT and conversation is not None:
            message_conversation_id = int(conversation["id"])
        if should_notify and final_message_text and message_conversation_id:
            message_row = storage.create_message(
                int(creature["id"]),
                conversation_id=message_conversation_id,
                role="creature",
                body=final_message_text,
                run_id=run_id,
                metadata={
                    **report,
                    "request_kind": request_kind,
                    "run_scope": run_scope,
                    "thread_id": new_thread_id,
                    "trigger_type": trigger_type,
                    "sandbox_mode": sandbox_mode,
                    "notes_storage": "SQLite / runs.notes_markdown",
                    "owner_mode": owner_mode,
                    "previous_thread_id": previous_thread_id,
                    "new_thread_id": new_thread_id,
                    "habit_id": int(habit["id"]) if habit is not None else None,
                    "habit_title": str(habit.get("title") or "") if habit is not None else "",
                    "habit_slug": str(habit.get("slug") or "") if habit is not None else "",
                    "habit_schedule_summary": str(habit.get("schedule_summary") or "") if habit is not None else "",
                    "memory_actions": applied_memory_actions,
                    "activity_markdown": str(report.get("activity_markdown") or ""),
                },
            )
            message_id = int(message_row["id"])
        task_next_run_at = None
        if habit is not None:
            task_next_run_at = _habit_next_run_at(
                str(habit.get("schedule_kind") or HABIT_SCHEDULE_MANUAL),
                _habit_schedule_json(habit.get("schedule_json")),
            )
            storage.record_habit_run_finish(
                int(habit["id"]),
                status="completed",
                summary=str(report["summary"] or ""),
                error_text="",
                next_run_at=task_next_run_at,
                report_path=notes_path,
            )
        storage.finish_run(
            run_id,
            creature_id=int(creature["id"]),
            status="completed",
            raw_output_text=result.final_text,
            summary=report["summary"],
            severity=report["severity"],
            message_text=final_message_text or None,
            error_text=None,
            next_run_at=_creature_next_due_habit_at(int(creature["id"])),
            metadata={
                **report,
                "request_kind": request_kind,
                "run_scope": run_scope,
                "conversation_id": int(conversation["id"]) if conversation is not None else None,
                "conversation_title": str(conversation["title"]) if conversation is not None else "",
                "sandbox_mode": sandbox_mode,
                "spawned_conversation_title": str(spawned_conversation["title"]) if spawned_conversation else "",
                "owner_mode": owner_mode,
                "previous_thread_id": previous_thread_id,
                "new_thread_id": new_thread_id,
                    "habit_id": int(habit["id"]) if habit is not None else None,
                    "habit_title": str(habit.get("title") or "") if habit is not None else "",
                    "habit_slug": str(habit.get("slug") or "") if habit is not None else "",
                    "habit_schedule_summary": str(habit.get("schedule_summary") or "") if habit is not None else "",
                "memory_actions": applied_memory_actions,
                "activity_markdown": str(report.get("activity_markdown") or ""),
            },
            notes_markdown=notes_markdown,
            notes_path=notes_path or None,
        )
        storage.create_run_event(
            run_id,
            event_type="status",
            body='{"type":"status","phase":"completed"}',
            metadata={"phase": "completed"},
        )
        _clear_run_feed_event_timing(run_id)
        _rewrite_backlog(creature)
        _schedule_followup_after_run(int(creature["id"]))
        return {
            "creature_slug": slug,
            "conversation_id": int(conversation["id"]) if conversation is not None else None,
            "run_id": run_id,
            "message_id": message_id,
            "report": report,
            "thread_id": new_thread_id,
            "sandbox_mode": sandbox_mode,
            "notes_markdown": notes_markdown,
            "spawned_conversation_id": int(spawned_conversation["id"]) if spawned_conversation else None,
        }
    except Exception as exc:
        error_text = _friendly_run_error(exc, sandbox_mode=sandbox_mode)
        waiting_message = _codex_waiting_message(kind=_load_codex_access_state_raw()["reason_kind"]) if _codex_access_waiting() else ""
        task_next_run_at = None
        if habit is not None:
            task_next_run_at = _habit_next_run_at(
                str(habit.get("schedule_kind") or HABIT_SCHEDULE_MANUAL),
                _habit_schedule_json(habit.get("schedule_json")),
            )
            storage.record_habit_run_finish(
                int(habit["id"]),
                status="failed",
                summary="",
                error_text=error_text,
                next_run_at=task_next_run_at,
                report_path="",
            )
        storage.create_run_event(
            run_id,
            event_type="error",
            body=error_text,
            metadata={"error_text": error_text},
        )
        storage.create_run_event(
            run_id,
            event_type="status",
            body='{"type":"status","phase":"failed"}',
            metadata={"phase": "failed"},
        )
        _clear_run_feed_event_timing(run_id)
        storage.finish_run(
            run_id,
            creature_id=int(creature["id"]),
            status="failed",
            raw_output_text=None,
            summary=None,
            severity="critical",
            message_text=None,
            error_text=error_text,
            next_run_at=_creature_next_due_habit_at(int(creature["id"])) or task_next_run_at,
            metadata={
                "run_scope": run_scope,
                "conversation_id": int(conversation["id"]) if conversation is not None else None,
                "conversation_title": str(conversation["title"]) if conversation is not None else "",
                "sandbox_mode": sandbox_mode,
                "owner_mode": owner_mode,
                "previous_thread_id": previous_thread_id,
                    "habit_id": int(habit["id"]) if habit is not None else None,
                    "habit_title": str(habit.get("title") or "") if habit is not None else "",
                    "habit_slug": str(habit.get("slug") or "") if habit is not None else "",
                    "habit_schedule_summary": str(habit.get("schedule_summary") or "") if habit is not None else "",
            },
            notes_markdown=None,
            notes_path=None,
        )
        if conversation is not None:
            storage.create_message(
                int(creature["id"]),
                conversation_id=int(conversation["id"]),
                role="system",
                body=waiting_message or error_text,
                run_id=run_id,
                metadata={
                    "trigger_type": trigger_type,
                    "status": "waiting" if waiting_message else "failed",
                    "error_text": error_text,
                    "run_scope": run_scope,
                    "reason_kind": _load_codex_access_state_raw()["reason_kind"] if waiting_message else "",
                },
            )
        _schedule_followup_after_run(int(creature["id"]))
        return {
            "creature_slug": slug,
            "conversation_id": int(conversation["id"]) if conversation is not None else None,
            "run_id": run_id,
            "message_id": None,
            "report": None,
            "thread_id": previous_thread_id,
            "sandbox_mode": sandbox_mode,
            "notes_markdown": None,
            "spawned_conversation_id": None,
            "status": "waiting" if waiting_message else "failed",
            "error_text": error_text,
        }


def run_creature(
    slug: str,
    *,
    trigger_type: str = "manual",
    force_message: bool = False,
    conversation_id: int | None = None,
    allow_code_changes: bool = False,
    start_new_thread: bool = False,
    habit_id: int | None = None,
    on_event: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    prepared = _prepare_run(
        slug,
        trigger_type=trigger_type,
        force_message=force_message,
        conversation_id=conversation_id,
        allow_code_changes=allow_code_changes,
        start_new_thread=start_new_thread,
        habit_id=habit_id,
    )
    return _execute_prepared_run(prepared, on_event=on_event)


def _run_in_background(prepared: dict[str, Any]) -> None:
    run_id = int(prepared["run_id"])
    try:
        with config.override_runtime_paths(
            db_path=str(prepared.get("db_path") or ""),
            data_dir=str(prepared.get("data_dir") or ""),
        ):
            _execute_prepared_run(prepared)
    finally:
        _forget_active_run_thread(run_id)


def start_background_run(
    slug: str,
    *,
    trigger_type: str = "manual",
    force_message: bool = False,
    conversation_id: int | None = None,
    allow_code_changes: bool = False,
    start_new_thread: bool = False,
    habit_id: int | None = None,
    run_scope: str | None = None,
    busy_action: str | None = None,
) -> dict[str, Any]:
    normalized_busy_action = _normalize_busy_action(busy_action)
    normalized_slug = str(canonical_creature_slug(slug))
    normalized_scope = (
        RUN_SCOPE_CHAT
        if run_scope == RUN_SCOPE_CHAT or conversation_id is not None or trigger_type == "user_reply"
        else RUN_SCOPE_ACTIVITY
    )
    if _codex_access_waiting() and not poll_codex_access_recovery(force=False):
        creature = storage.get_creature_by_slug(normalized_slug)
        return {
            "creature_slug": normalized_slug if creature is None else str(creature["slug"]),
            "conversation_id": conversation_id,
            "run_id": None,
            "sandbox_mode": "read-only",
            "run_scope": normalized_scope,
            "status": "waiting",
            "busy_action": normalized_busy_action,
            "waiting_message": _codex_waiting_message(kind=_load_codex_access_state_raw()["reason_kind"]),
        }
    try:
        prepared = _prepare_run(
            slug,
            trigger_type=trigger_type,
            force_message=force_message,
            conversation_id=conversation_id,
            allow_code_changes=allow_code_changes,
            start_new_thread=start_new_thread,
            habit_id=habit_id,
            run_scope=run_scope,
        )
    except RuntimeError as exc:
        if "active run lock" not in str(exc):
            raise
        creature = storage.get_creature_by_slug(str(canonical_creature_slug(slug)))
        if creature is None:
            raise
        if normalized_scope == RUN_SCOPE_CHAT and conversation_id is not None and trigger_type == "user_reply":
            storage.queue_conversation_action(conversation_id, action=normalized_busy_action)
        running = storage.latest_running_run_for_creature(int(creature["id"]))
        if running is None:
            raise
        return {
            "creature_slug": str(creature["slug"]),
            "conversation_id": int(running["conversation_id"]) if running["conversation_id"] is not None else None,
            "run_id": int(running["id"]),
            "sandbox_mode": str(running["sandbox_mode"] or "read-only"),
            "status": "locked",
            "run_scope": RUN_SCOPE_CHAT if running["conversation_id"] is not None else RUN_SCOPE_ACTIVITY,
            "busy_action": normalized_busy_action,
            "deferred_scope": normalized_scope,
        }
    thread = threading.Thread(target=_run_in_background, args=(prepared,), daemon=True)
    _remember_active_run_thread(int(prepared["run_id"]), thread)
    thread.start()
    return {
        "creature_slug": str(prepared["slug"]),
        "conversation_id": int(prepared["conversation"]["id"]) if prepared.get("conversation") is not None else None,
        "run_id": int(prepared["run_id"]),
        "sandbox_mode": str(prepared["sandbox_mode"]),
        "run_scope": str(prepared.get("run_scope") or RUN_SCOPE_ACTIVITY),
        "status": "running",
    }
def run_due_creatures(*, force_message: bool = False) -> list[dict[str, Any]]:
    _initialize_runtime()
    if _codex_access_waiting() and not poll_codex_access_recovery(force=False):
        return [
            {
                "status": "waiting",
                "reason_kind": _load_codex_access_state_raw()["reason_kind"],
            }
        ] if force_message else []
    results: list[dict[str, Any]] = []
    creatures_by_id = {int(row["id"]): row for row in storage.list_creatures()}
    pending_by_creature: dict[int, Any] = {}
    for creature in creatures_by_id.values():
        pending = storage.next_pending_conversation(int(creature["id"]))
        if pending is not None:
            pending_by_creature[int(creature["id"])] = pending
    due_habit_by_creature: dict[int, dict[str, Any]] = {}
    for row in storage.due_habits():
        habit = _decorate_habit(row)
        creature_id = int(habit["creature_id"])
        if creature_id not in due_habit_by_creature:
            due_habit_by_creature[creature_id] = habit
    ordered_creature_ids: list[int] = []
    for creature_id in pending_by_creature:
        ordered_creature_ids.append(creature_id)
    for creature_id in due_habit_by_creature:
        if creature_id not in pending_by_creature:
            ordered_creature_ids.append(creature_id)
    for creature_id in ordered_creature_ids:
        creature = creatures_by_id.get(creature_id)
        if creature is None:
            continue
        creature_id = int(creature["id"])
        pending = pending_by_creature.get(creature_id)
        running = storage.latest_running_run_for_creature(creature_id)
        if running is not None:
            if pending is not None:
                storage.queue_conversation_action(
                    int(pending["id"]),
                    action=str(_row_value(pending, "pending_action") or BUSY_ACTION_QUEUE),
                )
            results.append(
                {
                    "creature_slug": str(creature["slug"]),
                    "conversation_id": int(running["conversation_id"]) if running["conversation_id"] is not None else None,
                    "run_id": int(running["id"]),
                    "sandbox_mode": str(running["sandbox_mode"] or "read-only"),
                    "run_scope": RUN_SCOPE_CHAT if running["conversation_id"] is not None else RUN_SCOPE_ACTIVITY,
                    "status": "deferred",
                }
            )
            continue
        if pending is not None:
            owner_mode = _normalize_owner_mode(_row_value(pending, "owner_mode"))
            result = start_background_run(
                str(creature["slug"]),
                trigger_type="user_reply",
                force_message=force_message,
                conversation_id=int(pending["id"]),
                allow_code_changes=_allow_code_changes_for_owner_mode(owner_mode),
                run_scope=RUN_SCOPE_CHAT,
                busy_action=str(_row_value(pending, "pending_action") or BUSY_ACTION_QUEUE),
            )
            if str(result.get("status") or "") == "running":
                storage.clear_conversation_action(int(pending["id"]))
            results.append(result)
            continue
        due_habit = due_habit_by_creature.get(creature_id)
        if due_habit is None:
            continue
        results.append(
            start_background_run(
                str(creature["slug"]),
                trigger_type="habit",
                force_message=force_message,
                allow_code_changes=True,
                habit_id=int(due_habit["id"]),
                run_scope=RUN_SCOPE_ACTIVITY,
            )
        )
    return results


def list_run_events(slug: str, run_id: int, *, after_id: int = 0) -> dict[str, Any]:
    _initialize_runtime()
    slug = str(canonical_creature_slug(slug))
    creature = storage.get_creature_by_slug(slug)
    if creature is None:
        raise KeyError(f"Unknown creature slug: {slug}")
    run = storage.get_run(run_id)
    if run is None or int(run["creature_id"]) != int(creature["id"]):
        raise KeyError(f"Run {run_id} does not belong to creature {slug}")
    events = []
    for row in storage.list_run_events(run_id, after_id=after_id):
        payload = dict(row)
        payload["metadata"] = _parse_json(str(payload.get("metadata_json") or ""))
        payload["display_body"] = _stored_run_event_display_body(
            payload.get("event_type"),
            payload.get("body"),
            payload.get("metadata"),
        )
        events.append(payload)
    return {"run": dict(run), "events": events}


def reconcile_stranded_runs() -> int:
    _initialize_runtime()
    stranded = storage.list_running_runs()
    if not stranded:
        return 0
    repaired = 0
    for run in stranded:
        creature = storage.get_creature(int(run["creature_id"]))
        if creature is None:
            continue
        error_text = INTERRUPTED_RUN_ERROR_TEXT
        metadata = _parse_json(str(run["metadata_json"] or ""))
        run_scope = _run_scope_value(run)
        trigger_type = str(run["trigger_type"] or "scheduled").strip().lower()
        should_retry_chat = run_scope == RUN_SCOPE_CHAT and run["conversation_id"] is not None
        habit_id = int(metadata.get("habit_id") or 0) if str(metadata.get("habit_id") or "").strip() else 0
        should_retry_activity = run_scope == RUN_SCOPE_ACTIVITY and habit_id > 0
        task_retry_at = None
        if habit_id:
            habit = storage.get_habit_for_creature(int(creature["id"]), habit_id)
            if habit is not None:
                task_retry_at = _habit_next_run_at(
                    str(habit["schedule_kind"] or HABIT_SCHEDULE_MANUAL),
                    _habit_schedule_json(habit["schedule_json"]),
                )
                storage.record_habit_run_finish(
                    habit_id,
                    status="failed",
                    summary="",
                    error_text=error_text,
                    next_run_at=task_retry_at,
                    report_path=str(run["notes_path"] or ""),
                )
        retry_at = datetime.now(timezone.utc) if should_retry_chat else task_retry_at
        storage.finish_run(
            int(run["id"]),
            creature_id=int(creature["id"]),
            status="failed",
            raw_output_text=None,
            summary=None,
            severity="critical",
            message_text=None,
            error_text=error_text,
            next_run_at=retry_at,
            metadata={
                "conversation_id": int(run["conversation_id"]) if run["conversation_id"] is not None else None,
                "sandbox_mode": str(run["sandbox_mode"] or ""),
                "reconciled": True,
                "retry_queued": should_retry_chat or should_retry_activity,
            },
            notes_markdown=str(run["notes_markdown"] or "") or None,
            notes_path=str(run["notes_path"] or "") or None,
        )
        if should_retry_chat:
            storage.queue_conversation_action(int(run["conversation_id"]), action=BUSY_ACTION_STEER)
            storage.clear_creature_runtime_error(int(creature["id"]), next_run_at=retry_at)
        elif should_retry_activity:
            storage.clear_creature_runtime_error(int(creature["id"]), next_run_at=retry_at)
        elif trigger_type == "bootstrap":
            _queue_creature_bootstrap(creature, focus_hint=_focus_hint_for_creature(creature))
        repaired += 1
    return repaired


def health_snapshot() -> dict[str, Any]:
    _initialize_runtime()
    counts = storage.health_counts()
    codex_access = _codex_access_state()
    with _ACTIVE_RUN_THREADS_LOCK:
        active_threads = sum(1 for thread in _ACTIVE_RUN_THREADS.values() if thread.is_alive())
    thread_check = "ok" if counts["running_runs"] == active_threads else "mismatch"
    return {
        "status": "ok",
        "app": "creature-os",
        "model": get_default_creature_model(),
        "reasoning_effort": get_default_creature_reasoning_effort(),
        "url": config.app_url(),
        "timestamp": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "checks": {
            "db": "ok",
            "data_dir": "ok" if config.data_dir().exists() else "missing",
            "worker_threads": thread_check,
        },
        "counts": {
            **counts,
            "active_run_threads": active_threads,
        },
        "codex_access": {
            "status": codex_access["status"],
            "reason_kind": codex_access["reason_kind"],
            "last_checked_at": codex_access["last_checked_at"],
            "last_ok_at": codex_access["last_ok_at"],
        },
    }


def dashboard_state(
    *,
    selected_slug: str | None = None,
    view: str = "creature",
    notice: str | None = None,
    conversation_id: int | None = None,
    run_id: int | None = None,
    habit_target: str | None = None,
    draft_chat: bool = False,
) -> dict[str, Any]:
    _initialize_runtime()
    normalized_view = "creature" if view in {"home", "notifications"} else ("chats" if view == "conversations" else view)
    active_view = (
        normalized_view
        if normalized_view in {"creature", "activity", "chats", "settings"}
        else "creature"
    )
    onboarding = onboarding_state(include_chat=False)
    selected_slug = canonical_creature_slug(selected_slug)
    keeper_slug = str(onboarding.get("creature_slug") or "") if onboarding else ""
    if onboarding["required"] and not (
        keeper_slug
        and selected_slug == keeper_slug
        and active_view in {"creature", "activity", "chats"}
    ):
        active_view = f"onboarding-{onboarding['phase']}"
    creature_neutral_views = {"settings", "onboarding-ecosystem", "onboarding-starter"}
    codex_waiting = _codex_access_waiting()
    creatures = [_row_to_dict(row) for row in storage.list_creatures()]
    for creature in creatures:
        creature.update(_creature_intro_state(creature))
        creature["last_run_at_display"] = _format_timestamp_display(creature.get("last_run_at"))
        creature["last_run_at_relative_display"] = _format_relative_time_display(creature.get("last_run_at"))
        creature["habits"] = _creature_habits_state(creature)
        creature["habit_summary"] = _creature_habit_summary(creature["habits"])
        creature["is_pinned"] = bool(creature.get("is_pinned"))
        creature["can_delete"] = bool(creature.get("can_delete", 1))
        creature["thinking"] = _thinking_state(creature)
        creature["is_keeper"] = _is_keeper_creature(creature)
        creature["role_label"] = "Keeper" if creature["is_keeper"] else ""
        creature["is_codex_waiting"] = codex_waiting
    selected_creature = None
    if active_view not in creature_neutral_views and selected_slug:
        selected_creature = next((row for row in creatures if row["slug"] == selected_slug and row.get("intro_ready")), None)
    if active_view not in creature_neutral_views and selected_creature is None and creatures:
        selected_creature = next((row for row in creatures if row.get("is_keeper") and row.get("intro_ready")), None)
        if selected_creature is None:
            selected_creature = next((row for row in creatures if row.get("intro_ready")), None)
    if (
        selected_creature is not None
        and _is_keeper_creature(selected_creature)
        and not onboarding["required"]
        and active_view in {"chats", "activity"}
    ):
        active_view = "creature"

    conversations: list[dict[str, Any]] = []
    selected_conversation = None
    runs: list[dict[str, Any]] = []
    selected_run = None
    messages: list[dict[str, Any]] = []
    active_run = None
    active_run_events: list[dict[str, Any]] = []
    active_run_last_event_id = 0
    home_items: list[dict[str, Any]] = []
    notifications: list[dict[str, Any]] = []
    selected_requests: list[dict[str, Any]] = []
    selected_activity_feed: list[dict[str, Any]] = []
    selected_habits: list[dict[str, Any]] = []
    selected_habit: dict[str, Any] | None = None
    selected_habit_id: int | None = None
    selected_habit_target = str(habit_target or "").strip().lower()
    selected_keeper_dialog: dict[str, Any] = {}
    recent_conversations: list[dict[str, Any]] = []
    selected_busy_state: dict[str, Any] | None = None
    settings_state = _global_settings_state()
    selected_owner_reference_state = _owner_reference_state(selected_creature) if selected_creature is not None else _owner_reference_state()
    if selected_creature is not None:
        creature_id = int(selected_creature["id"])
        _ensure_creature_documents(selected_creature)
        if (
            active_view == "chats"
            and conversation_id is None
            and not draft_chat
            and not _is_keeper_creature(selected_creature)
        ):
            pending_ponder_run = _latest_unspawned_ponder_run(selected_creature)
            if pending_ponder_run is not None:
                auto_conversation = spawn_conversation_from_run(
                    str(selected_creature["slug"]),
                    int(pending_ponder_run["id"]),
                    body_override=_run_surface_message(pending_ponder_run),
                )
                conversation_id = int(auto_conversation["id"])
        conversations = []
        for row in storage.list_conversations(creature_id):
            if _is_keeper_creature(selected_creature) and not onboarding["required"] and _is_internal_keeper_conversation(row):
                continue
            conversation_state = _decorate_conversation(row)
            unread_state = storage.conversation_unread_state(int(conversation_state["id"]))
            conversation_state["unread_message_count"] = unread_state["count"]
            conversation_state["has_unread"] = unread_state["count"] > 0
            conversation_state["has_priority_unread"] = unread_state["has_priority"]
            conversations.append(conversation_state)
        if conversation_id is not None:
            selected_conversation = next((row for row in conversations if int(row["id"]) == int(conversation_id)), None)
        should_default_conversation = not (active_view == "chats" and draft_chat and conversation_id is None)
        if selected_conversation is None and conversations and should_default_conversation:
            if _is_keeper_creature(selected_creature) and not onboarding["required"]:
                selected_conversation = next(
                    (row for row in conversations if str(row.get("title") or "") != WELCOME_CONVERSATION_TITLE),
                    next(
                        (row for row in conversations if str(row.get("title") or "") == WELCOME_CONVERSATION_TITLE),
                        conversations[0],
                    ),
                )
            else:
                selected_conversation = conversations[0]
        if (
            selected_conversation is not None
            and active_view == "chats"
            and int(selected_creature.get("intro_conversation_id") or 0) > 0
            and int(selected_conversation["id"]) == int(selected_creature.get("intro_conversation_id") or 0)
            and not _is_intro_surfaced(selected_creature)
        ):
            storage.set_meta(_intro_surfaced_meta_key(int(selected_creature["id"])), "1")
        recent_conversations = conversations[:5]
        if active_view == "chats" and selected_conversation is not None:
            storage.mark_conversation_read(int(selected_conversation["id"]))
            conversations = []
            for row in storage.list_conversations(creature_id):
                if _is_keeper_creature(selected_creature) and not onboarding["required"] and _is_internal_keeper_conversation(row):
                    continue
                conversation_state = _decorate_conversation(row)
                unread_state = storage.conversation_unread_state(int(conversation_state["id"]))
                conversation_state["unread_message_count"] = unread_state["count"]
                conversation_state["has_unread"] = unread_state["count"] > 0
                conversation_state["has_priority_unread"] = unread_state["has_priority"]
                conversations.append(conversation_state)
            selected_conversation = next(
                (row for row in conversations if int(row["id"]) == int(selected_conversation["id"])),
                selected_conversation,
            )
        conversation_run = None
        if selected_conversation is not None:
            messages = [_decorate_message(row) for row in storage.list_messages(int(selected_conversation["id"]))]
            conversation_run = storage.latest_run_for_conversation(int(selected_conversation["id"]))
        if _is_keeper_creature(selected_creature):
            selected_keeper_dialog = _keeper_dialog_state(
                selected_creature,
                creatures=creatures,
                conversation=selected_conversation,
                messages=messages,
                onboarding_required=onboarding["required"],
                transition_notice=str(notice or "").strip(),
            )

        current_running_row = storage.latest_running_run_for_creature(creature_id)
        current_running_scope = _run_scope_value(current_running_row)
        display_run_row = conversation_run
        if (
            display_run_row is None
            and selected_conversation is not None
            and current_running_row is not None
            and current_running_scope != RUN_SCOPE_ACTIVITY
        ):
            display_run_row = current_running_row
        if display_run_row is not None:
            active_run = _decorate_run(display_run_row)
            active_run_events = []
            for row in storage.list_run_events(int(display_run_row["id"])):
                payload = dict(row)
                payload["metadata"] = _parse_json(str(payload.get("metadata_json") or ""))
                payload["display_body"] = _stored_run_event_display_body(
                    payload.get("event_type"),
                    payload.get("body"),
                    payload.get("metadata"),
                )
                active_run_last_event_id = int(payload["id"])
                if payload["display_body"]:
                    active_run_events.append(payload)

        if current_running_row is not None and current_running_scope != RUN_SCOPE_ACTIVITY:
            current_running = _decorate_run(current_running_row)
            busy_conversation_id = (
                int(current_running["conversation_id"])
                if current_running["conversation_id"] is not None
                else None
            )
            busy_conversation = storage.get_conversation(busy_conversation_id) if busy_conversation_id is not None else None
            is_current_chat = bool(
                selected_conversation is not None
                and busy_conversation_id is not None
                and int(selected_conversation["id"]) == busy_conversation_id
            )
            if str(current_running["run_scope"]) == RUN_SCOPE_ACTIVITY:
                headline = f"{selected_creature['display_name']} is already running a scheduled habit."
                detail = "Send a note now to steer what it handles next, or queue it behind the current work."
            elif is_current_chat:
                headline = f"{selected_creature['display_name']} is already working on this chat."
                detail = "Send more guidance to steer the next follow-up, or queue it behind the current reply."
            else:
                headline = f"{selected_creature['display_name']} is already working in another chat."
                chat_title = str(_row_value(busy_conversation, "title") or "Another chat")
                detail = f'Current chat: "{chat_title}". Send a note here to steer what it handles next, or queue it behind the active run.'
            selected_busy_state = {
                "is_busy": True,
                "headline": headline,
                "detail": detail,
                "run_id": int(current_running["id"]),
                "run_scope": str(current_running["run_scope"]),
                "conversation_id": busy_conversation_id,
                "conversation_title": str(_row_value(busy_conversation, "title") or ""),
                "is_current_chat": is_current_chat,
            }

        runs = [_decorate_run(row) for row in storage.recent_runs(creature_id, limit=100 if active_view == "activity" else 15)]
        selected_requests = _primary_attention_request_for_creature(selected_creature, ecosystem_value=get_ecosystem()["value"])
        selected_habits = list(selected_creature.get("habits") or [])
        if selected_habit_target.isdigit():
            requested_habit_id = int(selected_habit_target)
            if any(int(habit.get("id") or 0) == requested_habit_id for habit in selected_habits):
                selected_habit_id = requested_habit_id
        elif active_view == "activity" and selected_habits:
            remembered_habit_id = last_viewed_habit_id(creature_id)
            if remembered_habit_id is not None and any(int(habit.get("id") or 0) == remembered_habit_id for habit in selected_habits):
                selected_habit_id = remembered_habit_id
            else:
                selected_habit_id = int(selected_habits[0].get("id") or 0) or None
        if selected_habit_id is not None:
            selected_habit = next((habit for habit in selected_habits if int(habit.get("id") or 0) == selected_habit_id), None)
        if selected_habit is None and selected_habits:
            selected_habit = selected_habits[0]
            selected_habit_id = int(selected_habit.get("id") or 0) or None
        selected_activity_feed = [
            item
            for item in runs
            if _is_visible_activity_report_run(item)
            and (
                selected_habit_id is None
                or int(item.get("habit_id") or 0) == int(selected_habit_id)
            )
        ]
        selected_activity_feed.sort(key=lambda item: (str(item.get("started_at") or ""), int(item.get("id") or 0)))
        if run_id is not None:
            selected_run = next((row for row in runs if int(row["id"]) == int(run_id)), None)
        if selected_run is None and runs:
            selected_run = runs[0]

    for creature in creatures:
        _ensure_creature_documents(creature)
        agenda_counts = _agenda_priority_counts(int(creature["id"]))
        pending = storage.next_pending_conversation(int(creature["id"]))
        latest_chat = storage.get_latest_conversation(int(creature["id"]))
        pending_count = int(creature.get("pending_conversation_count") or 0)
        unread_state = storage.creature_unread_state(int(creature["id"]))
        unread_count = unread_state["count"]
        severity = str(creature.get("last_run_severity") or "info").lower()
        creature["unread_message_count"] = unread_count
        creature["has_priority_unread"] = unread_state["has_priority"]
        creature["critical_agenda_count"] = agenda_counts["critical"]
        creature["high_agenda_count"] = agenda_counts["high"]
        creature["has_urgent_agenda"] = bool(agenda_counts["critical"] or agenda_counts["high"])
        creature["has_urgent_indicator"] = False
        creature["entry_target"] = _preferred_creature_entry_target(creature, onboarding_required=onboarding["required"])
        home_items.append(
            {
                **creature,
                "pending_conversation_id": int(pending["id"]) if pending is not None else None,
                "latest_chat_id": int(latest_chat["id"]) if latest_chat is not None else None,
                "pending_conversation_count": pending_count,
                "unread_message_count": unread_count,
                "critical_agenda_count": agenda_counts["critical"],
                "high_agenda_count": agenda_counts["high"],
                "severity_rank": SEVERITY_ORDER.get(severity, 3),
                "needs_attention": bool(
                    str(creature.get("status") or "") == "error"
                    or agenda_counts["critical"] > 0
                    or pending_count > 0
                ),
            }
        )

    scenic_preview_mode = not onboarding["required"] and not creatures
    visible_creatures = [creature for creature in creatures if not (scenic_preview_mode and bool(creature.get("is_keeper")))]
    visible_home_items = [item for item in home_items if not (scenic_preview_mode and bool(item.get("is_keeper")))]

    visible_home_items.sort(
        key=lambda item: (
            0 if bool(item.get("is_pinned")) else 1,
            0 if str(item.get("status") or "") == "error" else 1,
            0 if int(item.get("critical_agenda_count") or 0) > 0 else 1,
            0 if int(item.get("pending_conversation_count") or 0) > 0 else 1,
            int(item.get("severity_rank") or 3),
            -int(item.get("high_agenda_count") or 0),
            str(item.get("display_name") or "").lower(),
        )
    )
    ecosystem_value = get_ecosystem()["value"]
    attention_requests = _attention_requests(creatures, ecosystem_value=ecosystem_value)
    visible_creature_slugs = {str(creature.get("slug") or "") for creature in visible_creatures}
    visible_attention_requests = [item for item in attention_requests if str(item.get("creature_slug") or "") in visible_creature_slugs]
    attention_request_slugs = {
        str(item.get("creature_slug") or "")
        for item in visible_attention_requests
        if str(item.get("creature_slug") or "").strip()
        and str(item.get("severity") or "").lower() in {"warning", "critical"}
    }
    for creature in creatures:
        creature["has_urgent_indicator"] = str(creature.get("slug") or "") in attention_request_slugs
    for item in visible_home_items:
        item["has_urgent_indicator"] = str(item.get("slug") or "") in attention_request_slugs
    notifications_state = _attention_request_state(visible_attention_requests)
    home_state = _home_state(
        visible_creatures,
        visible_attention_requests,
        ecosystem_value=ecosystem_value,
    )
    recent_sidebar_slug = last_viewed_creature_slug()
    single_creatures = sorted(
        visible_creatures,
        key=lambda item: (
            0 if bool(item.get("is_keeper")) else 1,
            0 if (recent_sidebar_slug and not bool(item.get("is_keeper")) and str(item.get("slug") or "") == recent_sidebar_slug) else 1,
            str(item.get("display_name") or "").lower(),
        ),
    )

    return {
        "creatures": creatures,
        "home_items": visible_home_items,
        "notifications": notifications,
        "notifications_state": notifications_state,
        "home_state": home_state,
        "scenic_preview_mode": scenic_preview_mode,
        "selected_creature": selected_creature,
        "active_view": active_view,
        "single_creatures": single_creatures,
        "chats": conversations,
        "selected_chat": selected_conversation,
        "conversations": conversations,
        "selected_conversation": selected_conversation,
        "messages": messages,
        "selected_requests": selected_requests,
        "selected_activity_feed": selected_activity_feed,
        "selected_habits": selected_habits,
        "selected_habit": selected_habit,
        "selected_habit_id": selected_habit_id,
        "selected_habit_target": selected_habit_target,
        "selected_keeper_dialog": selected_keeper_dialog,
        "recent_conversations": recent_conversations,
        "active_run": active_run,
        "active_run_events": active_run_events,
        "active_run_last_event_id": active_run_last_event_id,
        "selected_busy_state": selected_busy_state,
        "runs": runs,
        "selected_run": selected_run,
        "settings_state": settings_state,
        "selected_owner_reference_state": selected_owner_reference_state,
        "onboarding_state": onboarding,
        "app_url": config.app_url(),
        "new_chat_title": NEW_CHAT_TITLE,
        "new_conversation_title": NEW_CONVERSATION_TITLE,
        "thinking_feed_label": thinking_feed_label(),
    }
REASONING_EFFORT_VALUES = ("low", "medium", "high", "xhigh")
MAX_CREATURE_DISPLAY_NAME_CHARS = 48
CODEX_ACCESS_PROBE_INTERVAL_SECONDS = 300
CODEX_ACCESS_PROBE_TIMEOUT_SECONDS = 45
CODEX_RATE_LIMIT_CACHE_TTL_SECONDS = 60
