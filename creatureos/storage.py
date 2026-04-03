from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator

from . import config

DEFAULT_OWNER_MODE = "implement"
PRIORITY_MESSAGE_SEVERITIES = {"high", "critical"}
AFTER_CHAT_HABIT_SCHEDULE = "after_chat"
STATE_SURFACES_TABLE = "state_surfaces"
PURPOSE_SURFACE_KEY = "purpose"


def normalize_owner_mode(value: str | None) -> str:
    return "implement" if str(value or "").strip().lower() == "implement" else DEFAULT_OWNER_MODE


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def to_iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat()


def from_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value)


def _message_priority_flag(metadata_json: str | None) -> bool:
    if not metadata_json:
        return False
    try:
        payload = json.loads(metadata_json)
    except json.JSONDecodeError:
        return False
    if not isinstance(payload, dict):
        return False
    severity = str(payload.get("severity") or "").strip().lower()
    return severity in PRIORITY_MESSAGE_SEVERITIES


def _normalize_after_chat_minutes(value: Any) -> int:
    try:
        minutes = int(value)
    except (TypeError, ValueError):
        minutes = 120
    return max(5, minutes)


def _reschedule_after_chat_habits(
    conn: sqlite3.Connection,
    *,
    creature_id: int,
    activity_at: datetime,
) -> None:
    rows = conn.execute(
        """
        SELECT id, schedule_json
        FROM creature_habits
        WHERE creature_id = ?
          AND enabled = 1
          AND schedule_kind = ?
        """,
        (creature_id, AFTER_CHAT_HABIT_SCHEDULE),
    ).fetchall()
    if not rows:
        return
    now = to_iso(activity_at)
    for row in rows:
        try:
            schedule = json.loads(str(row["schedule_json"] or "{}"))
        except json.JSONDecodeError:
            schedule = {}
        if not isinstance(schedule, dict):
            schedule = {}
        delay_minutes = _normalize_after_chat_minutes(schedule.get("after_minutes"))
        next_run_at = to_iso(activity_at + timedelta(minutes=delay_minutes))
        conn.execute(
            """
            UPDATE creature_habits
            SET next_run_at = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (next_run_at, now, int(row["id"])),
        )


@contextmanager
def connect() -> Iterator[sqlite3.Connection]:
    conn = sqlite3.connect(config.db_path())
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db() -> None:
    with connect() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS creatures (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                slug TEXT NOT NULL UNIQUE,
                display_name TEXT NOT NULL,
                system_role TEXT NOT NULL DEFAULT '',
                is_pinned INTEGER NOT NULL DEFAULT 0,
                can_delete INTEGER NOT NULL DEFAULT 1,
                ecosystem TEXT NOT NULL DEFAULT '',
                purpose_summary TEXT NOT NULL DEFAULT '',
                temperament TEXT NOT NULL DEFAULT 'Quiet Observer',
                concern TEXT NOT NULL,
                system_prompt TEXT NOT NULL,
                workdir TEXT NOT NULL,
                codex_thread_id TEXT,
                owner_reference_override TEXT,
                model_override TEXT,
                reasoning_effort_override TEXT,
                status TEXT NOT NULL DEFAULT 'idle',
                last_run_at TEXT,
                next_run_at TEXT,
                last_error TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                creature_id INTEGER NOT NULL REFERENCES creatures(id) ON DELETE CASCADE,
                trigger_type TEXT NOT NULL,
                status TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                codex_thread_id TEXT,
                prompt_text TEXT,
                raw_output_text TEXT,
                summary TEXT,
                severity TEXT,
                message_text TEXT,
                error_text TEXT,
                notes_markdown TEXT,
                conversation_id INTEGER,
                run_scope TEXT,
                sandbox_mode TEXT,
                metadata_json TEXT,
                notes_path TEXT
            );

            CREATE TABLE IF NOT EXISTS conversations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                creature_id INTEGER NOT NULL REFERENCES creatures(id) ON DELETE CASCADE,
                title TEXT NOT NULL,
                owner_mode TEXT NOT NULL DEFAULT 'implement',
                model_override TEXT,
                reasoning_effort_override TEXT,
                codex_thread_id TEXT,
                pending_action TEXT NOT NULL DEFAULT '',
                pending_action_at TEXT,
                is_default INTEGER NOT NULL DEFAULT 0,
                source_run_id INTEGER REFERENCES runs(id) ON DELETE SET NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                last_message_at TEXT,
                owner_last_read_at TEXT
            );

            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                creature_id INTEGER NOT NULL REFERENCES creatures(id) ON DELETE CASCADE,
                conversation_id INTEGER,
                run_id INTEGER REFERENCES runs(id) ON DELETE SET NULL,
                role TEXT NOT NULL,
                body TEXT NOT NULL,
                metadata_json TEXT,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS app_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS run_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
                event_type TEXT NOT NULL,
                body TEXT,
                metadata_json TEXT,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS state_surfaces (
                creature_id INTEGER NOT NULL REFERENCES creatures(id) ON DELETE CASCADE,
                doc_key TEXT NOT NULL,
                content_text TEXT NOT NULL DEFAULT '',
                revision INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (creature_id, doc_key)
            );

            CREATE TABLE IF NOT EXISTS memory_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                creature_id INTEGER NOT NULL REFERENCES creatures(id) ON DELETE CASCADE,
                kind TEXT NOT NULL,
                body TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'active',
                source_message_id INTEGER REFERENCES messages(id) ON DELETE SET NULL,
                source_run_id INTEGER REFERENCES runs(id) ON DELETE SET NULL,
                previous_record_id INTEGER REFERENCES memory_records(id) ON DELETE SET NULL,
                superseded_by_id INTEGER REFERENCES memory_records(id) ON DELETE SET NULL,
                metadata_json TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS memory_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                creature_id INTEGER NOT NULL REFERENCES creatures(id) ON DELETE CASCADE,
                record_id INTEGER REFERENCES memory_records(id) ON DELETE SET NULL,
                actor_type TEXT NOT NULL,
                action TEXT NOT NULL,
                reason TEXT,
                metadata_json TEXT,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS agenda_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                creature_id INTEGER NOT NULL REFERENCES creatures(id) ON DELETE CASCADE,
                title TEXT NOT NULL,
                priority TEXT NOT NULL,
                details TEXT NOT NULL DEFAULT '',
                spawn_conversation INTEGER NOT NULL DEFAULT 0,
                ordinal INTEGER NOT NULL DEFAULT 0,
                source_message_id INTEGER REFERENCES messages(id) ON DELETE SET NULL,
                source_run_id INTEGER REFERENCES runs(id) ON DELETE SET NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS backlog_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                creature_id INTEGER NOT NULL REFERENCES creatures(id) ON DELETE CASCADE,
                body TEXT NOT NULL,
                ordinal INTEGER NOT NULL DEFAULT 0,
                source_message_id INTEGER REFERENCES messages(id) ON DELETE SET NULL,
                source_run_id INTEGER REFERENCES runs(id) ON DELETE SET NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS creature_habits (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                creature_id INTEGER NOT NULL REFERENCES creatures(id) ON DELETE CASCADE,
                slug TEXT NOT NULL,
                title TEXT NOT NULL,
                instructions TEXT NOT NULL DEFAULT '',
                schedule_kind TEXT NOT NULL DEFAULT 'manual',
                schedule_json TEXT NOT NULL DEFAULT '{}',
                enabled INTEGER NOT NULL DEFAULT 1,
                next_run_at TEXT,
                last_run_at TEXT,
                last_status TEXT NOT NULL DEFAULT '',
                last_summary TEXT NOT NULL DEFAULT '',
                last_error TEXT NOT NULL DEFAULT '',
                last_report_path TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_messages_creature_created_at
            ON messages(creature_id, created_at DESC, id DESC);

            CREATE INDEX IF NOT EXISTS idx_runs_creature_started_at
            ON runs(creature_id, started_at DESC, id DESC);

            CREATE INDEX IF NOT EXISTS idx_conversations_creature_updated_at
            ON conversations(creature_id, is_default DESC, updated_at DESC, id DESC);

            CREATE INDEX IF NOT EXISTS idx_run_events_run_id_id
            ON run_events(run_id, id ASC);

            CREATE INDEX IF NOT EXISTS idx_state_surfaces_creature_key
            ON state_surfaces(creature_id, doc_key);

            CREATE INDEX IF NOT EXISTS idx_memory_records_creature_status_updated
            ON memory_records(creature_id, status, updated_at DESC, id DESC);

            CREATE INDEX IF NOT EXISTS idx_memory_events_creature_created
            ON memory_events(creature_id, created_at DESC, id DESC);

            CREATE INDEX IF NOT EXISTS idx_agenda_items_creature_ordinal
            ON agenda_items(creature_id, ordinal ASC, id ASC);

            CREATE INDEX IF NOT EXISTS idx_backlog_items_creature_ordinal
            ON backlog_items(creature_id, ordinal ASC, id ASC);

            CREATE INDEX IF NOT EXISTS idx_creature_habits_creature_updated
            ON creature_habits(creature_id, updated_at DESC, id DESC);

            CREATE INDEX IF NOT EXISTS idx_creature_habits_due
            ON creature_habits(enabled, next_run_at ASC, id ASC);

            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_messages_conversation_created_at
            ON messages(conversation_id, created_at DESC, id DESC)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_conversations_creature_pending_action
            ON conversations(creature_id, pending_action, pending_action_at DESC, updated_at DESC, id DESC)
            """
        )
        run_lock_columns = {
            str(row["name"] or "")
            for row in conn.execute("PRAGMA table_info(run_locks)").fetchall()
        }
        if run_lock_columns and {"conversation_id", "run_scope"} - run_lock_columns:
            conn.execute("DROP TABLE run_locks")
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS run_locks (
                run_id INTEGER PRIMARY KEY REFERENCES runs(id) ON DELETE CASCADE,
                creature_id INTEGER NOT NULL REFERENCES creatures(id) ON DELETE CASCADE,
                conversation_id INTEGER REFERENCES conversations(id) ON DELETE CASCADE,
                run_scope TEXT NOT NULL DEFAULT '',
                acquired_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_run_locks_creature_recent
            ON run_locks(creature_id, acquired_at DESC, run_id DESC);

            CREATE UNIQUE INDEX IF NOT EXISTS idx_run_locks_activity_creature
            ON run_locks(creature_id)
            WHERE run_scope = 'activity';

            CREATE UNIQUE INDEX IF NOT EXISTS idx_run_locks_chat_conversation
            ON run_locks(conversation_id)
            WHERE run_scope = 'chat' AND conversation_id IS NOT NULL;
            """
        )
        conn.execute(
            """
            UPDATE creatures
            SET purpose_summary = concern
            WHERE COALESCE(TRIM(purpose_summary), '') = ''
            """
        )
        conn.execute(
            """
            UPDATE creatures
            SET temperament = 'Quiet Observer'
            WHERE COALESCE(TRIM(temperament), '') = ''
            """
        )
        conn.execute("UPDATE conversations SET is_default = 0 WHERE is_default != 0")


def save_creature(
    *,
    slug: str,
    display_name: str,
    system_role: str = "",
    is_pinned: bool = False,
    can_delete: bool = True,
    ecosystem: str = "",
    purpose_summary: str = "",
    temperament: str = "Quiet Observer",
    concern: str,
    system_prompt: str,
    workdir: str,
    model_override: str | None = None,
    reasoning_effort_override: str | None = None,
) -> sqlite3.Row:
    now = to_iso(utcnow())
    with connect() as conn:
        existing = conn.execute(
            "SELECT * FROM creatures WHERE slug = ?",
            (slug,),
        ).fetchone()
        if existing is None:
            conn.execute(
                """
                INSERT INTO creatures (
                    slug, display_name, system_role, is_pinned, can_delete,
                    ecosystem, purpose_summary, temperament,
                    concern, system_prompt, workdir,
                    model_override, reasoning_effort_override,
                    status,
                    next_run_at, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    slug,
                    display_name,
                    system_role,
                    1 if is_pinned else 0,
                    1 if can_delete else 0,
                    ecosystem,
                    purpose_summary,
                    temperament,
                    concern,
                    system_prompt,
                    workdir,
                    model_override,
                    reasoning_effort_override,
                    "idle",
                    None,
                    now,
                    now,
                ),
            )
        else:
            conn.execute(
                """
                UPDATE creatures
                SET display_name = ?,
                    system_role = ?,
                    is_pinned = ?,
                    can_delete = ?,
                    ecosystem = ?,
                    purpose_summary = ?,
                    temperament = ?,
                    concern = ?,
                    system_prompt = ?,
                    workdir = ?,
                    model_override = COALESCE(?, model_override),
                    reasoning_effort_override = COALESCE(?, reasoning_effort_override),
                    updated_at = ?
                WHERE slug = ?
                """,
                (
                    display_name,
                    system_role,
                    1 if is_pinned else 0,
                    1 if can_delete else 0,
                    ecosystem,
                    purpose_summary,
                    temperament,
                    concern,
                    system_prompt,
                    workdir,
                    model_override,
                    reasoning_effort_override,
                    now,
                    slug,
                ),
            )
        creature = conn.execute(
            "SELECT * FROM creatures WHERE slug = ?",
            (slug,),
        ).fetchone()
        return creature


def rename_creature_identity(
    creature_id: int,
    *,
    slug: str,
    display_name: str,
    ecosystem: str,
    purpose_summary: str,
    temperament: str,
    concern: str,
    system_prompt: str,
) -> None:
    now = to_iso(utcnow())
    with connect() as conn:
        conn.execute(
            """
            UPDATE creatures
            SET slug = ?,
                display_name = ?,
                ecosystem = ?,
                purpose_summary = ?,
                temperament = ?,
                concern = ?,
                system_prompt = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (slug, display_name, ecosystem, purpose_summary, temperament, concern, system_prompt, now, creature_id),
        )


def list_creatures() -> list[sqlite3.Row]:
    with connect() as conn:
        return conn.execute(
            """
            SELECT
                a.*,
                (
                    SELECT body
                    FROM messages m
                    WHERE m.creature_id = a.id
                    ORDER BY m.created_at DESC, m.id DESC
                    LIMIT 1
                ) AS last_message_body,
                (
                    SELECT created_at
                    FROM messages m
                    WHERE m.creature_id = a.id
                    ORDER BY m.created_at DESC, m.id DESC
                    LIMIT 1
                ) AS last_message_at,
                (
                    SELECT summary
                    FROM runs r
                    WHERE r.creature_id = a.id
                    ORDER BY r.started_at DESC, r.id DESC
                    LIMIT 1
                ) AS last_run_summary,
                (
                    SELECT severity
                    FROM runs r
                    WHERE r.creature_id = a.id
                    ORDER BY r.started_at DESC, r.id DESC
                    LIMIT 1
                ) AS last_run_severity,
                (
                    SELECT COUNT(*)
                    FROM conversations c
                    WHERE c.creature_id = a.id
                ) AS conversation_count,
                (
                    SELECT COUNT(*)
                    FROM conversations c
                    WHERE c.creature_id = a.id
                      AND EXISTS (
                          SELECT 1
                          FROM messages mu
                          WHERE mu.conversation_id = c.id
                            AND mu.role = 'user'
                            AND NOT EXISTS (
                                SELECT 1
                                FROM messages ma
                                WHERE ma.conversation_id = c.id
                                  AND ma.role = 'creature'
                                  AND (
                                      ma.created_at > mu.created_at
                                      OR (ma.created_at = mu.created_at AND ma.id > mu.id)
                                  )
                          )
                      )
                ) AS pending_conversation_count
            FROM creatures a
            ORDER BY COALESCE(a.is_pinned, 0) DESC, a.display_name COLLATE NOCASE, a.id
            """
        ).fetchall()


def keeper_runtime_snapshot(
    *,
    keeper_system_role: str = "keeper",
    max_creatures: int = 16,
    max_requests: int = 10,
    max_recent_messages: int = 10,
) -> dict[str, Any]:
    active_keeper_role = str(keeper_system_role or "").strip().lower() or "keeper"
    with connect() as conn:
        summary_row = conn.execute(
            """
            SELECT
                COUNT(*) AS operational_creatures,
                SUM(CASE WHEN LOWER(COALESCE(status, 'idle')) = 'running' THEN 1 ELSE 0 END) AS running_creatures,
                SUM(CASE WHEN LOWER(COALESCE(status, 'idle')) = 'error' THEN 1 ELSE 0 END) AS error_creatures
            FROM creatures
            WHERE LOWER(COALESCE(system_role, '')) != ?
            """,
            (active_keeper_role,),
        ).fetchone()
        creature_rows = conn.execute(
            """
            SELECT
                a.id,
                a.slug,
                a.display_name,
                a.ecosystem,
                a.purpose_summary,
                a.status,
                a.last_run_at,
                a.next_run_at,
                (
                    SELECT COUNT(*)
                    FROM conversations c
                    WHERE c.creature_id = a.id
                ) AS conversation_count,
                (
                    SELECT COUNT(*)
                    FROM runs r
                    WHERE r.creature_id = a.id
                      AND LOWER(COALESCE(r.status, '')) = 'completed'
                      AND COALESCE(TRIM(r.message_text), '') != ''
                      AND r.conversation_id IS NULL
                      AND NOT EXISTS (
                          SELECT 1
                          FROM conversations c
                          WHERE c.source_run_id = r.id
                      )
                ) AS pending_request_count,
                (
                    SELECT r.severity
                    FROM runs r
                    WHERE r.creature_id = a.id
                    ORDER BY r.started_at DESC, r.id DESC
                    LIMIT 1
                ) AS last_run_severity,
                (
                    SELECT r.summary
                    FROM runs r
                    WHERE r.creature_id = a.id
                    ORDER BY r.started_at DESC, r.id DESC
                    LIMIT 1
                ) AS last_run_summary
            FROM creatures a
            WHERE LOWER(COALESCE(a.system_role, '')) != ?
            ORDER BY COALESCE(a.is_pinned, 0) DESC, a.display_name COLLATE NOCASE, a.id
            LIMIT ?
            """,
            (active_keeper_role, max_creatures),
        ).fetchall()
        request_rows = conn.execute(
            """
            SELECT
                r.id AS run_id,
                a.display_name AS creature_display_name,
                a.purpose_summary,
                r.trigger_type,
                r.severity,
                COALESCE(r.finished_at, r.started_at, '') AS requested_at,
                COALESCE(r.message_text, r.summary, '') AS preview
            FROM runs r
            INNER JOIN creatures a ON a.id = r.creature_id
            WHERE LOWER(COALESCE(a.system_role, '')) != ?
              AND LOWER(COALESCE(r.status, '')) = 'completed'
              AND COALESCE(TRIM(r.message_text), '') != ''
              AND r.conversation_id IS NULL
              AND NOT EXISTS (
                  SELECT 1
                  FROM conversations c
                  WHERE c.source_run_id = r.id
              )
            ORDER BY COALESCE(r.finished_at, r.started_at, '') DESC, r.id DESC
            LIMIT ?
            """,
            (active_keeper_role, max_requests),
        ).fetchall()
        message_rows = conn.execute(
            """
            SELECT
                a.display_name AS creature_display_name,
                c.title AS conversation_title,
                m.body,
                m.created_at
            FROM messages m
            INNER JOIN conversations c ON c.id = m.conversation_id
            INNER JOIN creatures a ON a.id = c.creature_id
            WHERE m.role = 'user'
              AND LOWER(COALESCE(a.system_role, '')) != ?
            ORDER BY m.created_at DESC, m.id DESC
            LIMIT ?
            """,
            (active_keeper_role, max_recent_messages),
        ).fetchall()
    return {
        "summary": dict(summary_row or {}),
        "creatures": [dict(row) for row in creature_rows],
        "requests": [dict(row) for row in request_rows],
        "recent_messages": [dict(row) for row in message_rows],
    }


def creature_count() -> int:
    with connect() as conn:
        row = conn.execute("SELECT COUNT(*) AS count FROM creatures").fetchone()
        return int(row["count"] or 0)


def health_counts() -> dict[str, int]:
    with connect() as conn:
        creature_row = conn.execute(
            """
            SELECT
                COUNT(*) AS creatures,
                SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END) AS running_creatures,
                SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) AS error_creatures
            FROM creatures
            """
        ).fetchone()
        run_row = conn.execute(
            """
            SELECT
                COUNT(*) AS running_runs
            FROM runs
            WHERE status = 'running'
            """
        ).fetchone()
    return {
        "creatures": int(creature_row["creatures"] or 0),
        "running_creatures": int(creature_row["running_creatures"] or 0),
        "error_creatures": int(creature_row["error_creatures"] or 0),
        "running_runs": int(run_row["running_runs"] or 0),
    }


def get_creature_by_slug(slug: str) -> sqlite3.Row | None:
    with connect() as conn:
        return conn.execute(
            "SELECT * FROM creatures WHERE slug = ?",
            (slug,),
        ).fetchone()


def get_creature(creature_id: int) -> sqlite3.Row | None:
    with connect() as conn:
        return conn.execute(
            "SELECT * FROM creatures WHERE id = ?",
            (creature_id,),
        ).fetchone()


def set_thread_id(creature_id: int, thread_id: str) -> None:
    now = to_iso(utcnow())
    with connect() as conn:
        conn.execute(
            """
            UPDATE creatures
            SET codex_thread_id = ?, updated_at = ?
            WHERE id = ?
            """,
            (thread_id, now, creature_id),
        )


def set_conversation_thread_id(conversation_id: int, thread_id: str | None) -> None:
    now = to_iso(utcnow())
    with connect() as conn:
        conn.execute(
            """
            UPDATE conversations
            SET codex_thread_id = ?, updated_at = ?
            WHERE id = ?
            """,
            (str(thread_id or "").strip() or None, now, conversation_id),
        )


def set_run_thread_id(run_id: int, thread_id: str | None) -> None:
    with connect() as conn:
        conn.execute(
            """
            UPDATE runs
            SET codex_thread_id = ?
            WHERE id = ?
            """,
            (str(thread_id or "").strip() or None, run_id),
        )


def queue_conversation_action(conversation_id: int, *, action: str) -> None:
    final_action = "steer" if str(action or "").strip().lower() == "steer" else "queue"
    now = to_iso(utcnow())
    with connect() as conn:
        existing = conn.execute(
            "SELECT pending_action FROM conversations WHERE id = ?",
            (conversation_id,),
        ).fetchone()
        existing_action = str(existing["pending_action"] or "").strip().lower() if existing is not None else ""
        if existing_action == "steer" and final_action == "queue":
            final_action = "steer"
        conn.execute(
            """
            UPDATE conversations
            SET pending_action = ?,
                pending_action_at = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (final_action, now, now, conversation_id),
        )


def clear_conversation_action(conversation_id: int) -> None:
    now = to_iso(utcnow())
    with connect() as conn:
        conn.execute(
            """
            UPDATE conversations
            SET pending_action = '',
                pending_action_at = NULL,
                updated_at = ?
            WHERE id = ?
            """,
            (now, conversation_id),
        )


def next_pending_conversation(creature_id: int) -> sqlite3.Row | None:
    with connect() as conn:
        return conn.execute(
            """
            SELECT c.*
            FROM conversations c
            WHERE c.creature_id = ?
              AND EXISTS (
                  SELECT 1
                  FROM messages mu
                  WHERE mu.conversation_id = c.id
                    AND mu.role = 'user'
                    AND NOT EXISTS (
                        SELECT 1
                        FROM messages ma
                        WHERE ma.conversation_id = c.id
                          AND ma.role = 'creature'
                          AND (
                              ma.created_at > mu.created_at
                              OR (ma.created_at = mu.created_at AND ma.id > mu.id)
                          )
                    )
              )
            ORDER BY
              CASE
                WHEN c.pending_action = 'steer' THEN 0
                WHEN c.pending_action = 'queue' THEN 1
                ELSE 2
              END,
              COALESCE(c.pending_action_at, c.last_message_at, c.updated_at) DESC,
              c.id DESC
            LIMIT 1
            """,
            (creature_id,),
        ).fetchone()

def create_conversation(
    creature_id: int,
    *,
    title: str,
    source_run_id: int | None = None,
    is_default: bool = False,
    owner_mode: str = DEFAULT_OWNER_MODE,
    model_override: str | None = None,
    reasoning_effort_override: str | None = None,
) -> sqlite3.Row:
    now = to_iso(utcnow())
    final_owner_mode = normalize_owner_mode(owner_mode)
    with connect() as conn:
        cursor = conn.execute(
            """
            INSERT INTO conversations (
                creature_id, title, owner_mode, model_override, reasoning_effort_override, is_default, source_run_id, created_at, updated_at, last_message_at, owner_last_read_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL)
            """,
            (
                creature_id,
                title,
                final_owner_mode,
                model_override,
                reasoning_effort_override,
                1 if is_default else 0,
                source_run_id,
                now,
                now,
            ),
        )
        return conn.execute(
            "SELECT * FROM conversations WHERE id = ?",
            (int(cursor.lastrowid),),
        ).fetchone()


def rename_conversation(conversation_id: int, title: str) -> None:
    now = to_iso(utcnow())
    with connect() as conn:
        conn.execute(
            """
            UPDATE conversations
            SET title = ?, updated_at = ?
            WHERE id = ?
            """,
            (title, now, conversation_id),
        )


def delete_conversation(conversation_id: int) -> None:
    with connect() as conn:
        conn.execute(
            """
            DELETE FROM messages
            WHERE conversation_id = ?
            """,
            (conversation_id,),
        )
        conn.execute(
            """
            UPDATE runs
            SET conversation_id = NULL
            WHERE conversation_id = ?
            """,
            (conversation_id,),
        )
        conn.execute(
            """
            DELETE FROM conversations
            WHERE id = ?
            """,
            (conversation_id,),
        )


def set_conversation_owner_mode(conversation_id: int, owner_mode: str) -> None:
    now = to_iso(utcnow())
    final_owner_mode = normalize_owner_mode(owner_mode)
    with connect() as conn:
        conn.execute(
            """
            UPDATE conversations
            SET owner_mode = ?, updated_at = ?
            WHERE id = ?
            """,
            (final_owner_mode, now, conversation_id),
        )


def set_conversation_thinking_overrides(
    conversation_id: int,
    *,
    model_override: str | None,
    reasoning_effort_override: str | None,
) -> None:
    now = to_iso(utcnow())
    with connect() as conn:
        conn.execute(
            """
            UPDATE conversations
            SET model_override = ?,
                reasoning_effort_override = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (model_override, reasoning_effort_override, now, conversation_id),
        )


def mark_conversation_read(conversation_id: int) -> None:
    now = to_iso(utcnow())
    with connect() as conn:
        conn.execute(
            """
            UPDATE conversations
            SET owner_last_read_at = ?, updated_at = ?
            WHERE id = ?
            """,
            (now, now, conversation_id),
        )


def list_conversations(creature_id: int) -> list[sqlite3.Row]:
    with connect() as conn:
        return conn.execute(
            """
            SELECT
                c.*,
                (
                    SELECT body
                    FROM messages m
                    WHERE m.conversation_id = c.id
                    ORDER BY m.created_at DESC, m.id DESC
                    LIMIT 1
                ) AS last_message_body,
                (
                    SELECT role
                    FROM messages m
                    WHERE m.conversation_id = c.id
                    ORDER BY m.created_at DESC, m.id DESC
                    LIMIT 1
                ) AS last_message_role,
                (
                    SELECT COUNT(*)
                    FROM messages m
                    WHERE m.conversation_id = c.id
                ) AS message_count,
                (
                    SELECT COUNT(*)
                    FROM messages m
                    WHERE m.conversation_id = c.id
                      AND m.role = 'creature'
                      AND m.created_at > COALESCE(c.owner_last_read_at, '')
                ) AS unread_message_count,
                CASE
                    WHEN EXISTS (
                        SELECT 1
                        FROM messages mu
                        WHERE mu.conversation_id = c.id
                          AND mu.role = 'user'
                          AND NOT EXISTS (
                              SELECT 1
                              FROM messages ma
                              WHERE ma.conversation_id = c.id
                                AND ma.role = 'creature'
                                AND (
                                    ma.created_at > mu.created_at
                                    OR (ma.created_at = mu.created_at AND ma.id > mu.id)
                                )
                          )
                    ) THEN 1
                    ELSE 0
                END AS needs_reply
            FROM conversations c
            WHERE c.creature_id = ?
            ORDER BY COALESCE(c.last_message_at, c.updated_at) DESC, c.id DESC
            """,
            (creature_id,),
        ).fetchall()


def creature_unread_message_count(creature_id: int) -> int:
    with connect() as conn:
        row = conn.execute(
            """
            SELECT COUNT(*) AS count
            FROM messages m
            INNER JOIN conversations c ON c.id = m.conversation_id
            WHERE c.creature_id = ?
              AND m.role = 'creature'
              AND m.created_at > COALESCE(c.owner_last_read_at, '')
            """,
            (creature_id,),
        ).fetchone()
        return int(row["count"] or 0)


def conversation_unread_state(conversation_id: int) -> dict[str, Any]:
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT m.metadata_json
            FROM messages m
            INNER JOIN conversations c ON c.id = m.conversation_id
            WHERE m.conversation_id = ?
              AND m.role = 'creature'
              AND m.created_at > COALESCE(c.owner_last_read_at, '')
            ORDER BY m.created_at DESC, m.id DESC
            """,
            (conversation_id,),
        ).fetchall()
    count = len(rows)
    return {
        "count": count,
        "has_priority": any(_message_priority_flag(row["metadata_json"]) for row in rows),
    }


def creature_unread_state(creature_id: int) -> dict[str, Any]:
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT m.metadata_json
            FROM messages m
            INNER JOIN conversations c ON c.id = m.conversation_id
            WHERE c.creature_id = ?
              AND m.role = 'creature'
              AND m.created_at > COALESCE(c.owner_last_read_at, '')
            ORDER BY m.created_at DESC, m.id DESC
            """,
            (creature_id,),
        ).fetchall()
    count = len(rows)
    return {
        "count": count,
        "has_priority": any(_message_priority_flag(row["metadata_json"]) for row in rows),
    }


def unread_notifications_state() -> dict[str, Any]:
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT m.metadata_json
            FROM messages m
            INNER JOIN conversations c ON c.id = m.conversation_id
            WHERE m.role = 'creature'
              AND m.created_at > COALESCE(c.owner_last_read_at, '')
            ORDER BY m.created_at DESC, m.id DESC
            """
        ).fetchall()
    count = len(rows)
    return {
        "count": count,
        "has_priority": any(_message_priority_flag(row["metadata_json"]) for row in rows),
    }


def list_unread_notifications(*, limit: int = 200) -> list[sqlite3.Row]:
    with connect() as conn:
        return conn.execute(
            """
            SELECT
                m.*,
                c.title AS conversation_title,
                a.slug AS creature_slug,
                a.display_name AS creature_display_name,
                (
                    SELECT COUNT(*)
                    FROM messages mu
                    WHERE mu.conversation_id = c.id
                      AND mu.role = 'creature'
                      AND mu.created_at > COALESCE(c.owner_last_read_at, '')
                ) AS conversation_unread_count
            FROM messages m
            INNER JOIN conversations c ON c.id = m.conversation_id
            INNER JOIN creatures a ON a.id = c.creature_id
            WHERE m.role = 'creature'
              AND m.created_at > COALESCE(c.owner_last_read_at, '')
            ORDER BY m.created_at DESC, m.id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()


def get_conversation(conversation_id: int) -> sqlite3.Row | None:
    with connect() as conn:
        return conn.execute(
            "SELECT * FROM conversations WHERE id = ?",
            (conversation_id,),
        ).fetchone()


def get_conversation_for_creature(creature_id: int, conversation_id: int) -> sqlite3.Row | None:
    with connect() as conn:
        return conn.execute(
            """
            SELECT *
            FROM conversations
            WHERE id = ? AND creature_id = ?
            """,
            (conversation_id, creature_id),
        ).fetchone()


def get_latest_conversation(creature_id: int) -> sqlite3.Row | None:
    with connect() as conn:
        return conn.execute(
            """
            SELECT *
            FROM conversations
            WHERE creature_id = ?
            ORDER BY COALESCE(last_message_at, updated_at) DESC, id DESC
            LIMIT 1
            """,
            (creature_id,),
        ).fetchone()


def create_run(
    creature_id: int,
    *,
    trigger_type: str,
    prompt_text: str,
    thread_id: str | None,
    conversation_id: int | None = None,
    run_scope: str | None = None,
    sandbox_mode: str = "read-only",
) -> sqlite3.Row:
    now = to_iso(utcnow())
    final_scope = "chat" if str(run_scope or "").strip().lower() == "chat" or conversation_id is not None else "activity"
    with connect() as conn:
        if final_scope == "chat":
            if conversation_id is None:
                raise RuntimeError("Chat runs require a conversation id")
            existing_lock = conn.execute(
                """
                SELECT run_id
                FROM run_locks
                WHERE run_scope = 'chat'
                  AND conversation_id = ?
                """,
                (conversation_id,),
            ).fetchone()
            if existing_lock is not None:
                raise RuntimeError(f"Conversation {conversation_id} already has an active run lock")
        else:
            existing_lock = conn.execute(
                """
                SELECT run_id
                FROM run_locks
                WHERE run_scope = 'activity'
                  AND creature_id = ?
                """,
                (creature_id,),
            ).fetchone()
            if existing_lock is not None:
                raise RuntimeError(f"Creature {creature_id} already has an active activity run lock")
        cursor = conn.execute(
            """
            INSERT INTO runs (
                creature_id, trigger_type, status, started_at, codex_thread_id, prompt_text,
                conversation_id, run_scope, sandbox_mode
            ) VALUES (?, ?, 'running', ?, ?, ?, ?, ?, ?)
            """,
            (
                creature_id,
                trigger_type,
                now,
                thread_id,
                prompt_text,
                conversation_id,
                final_scope,
                sandbox_mode,
            ),
        )
        run_id = int(cursor.lastrowid)
        conn.execute(
            """
            INSERT INTO run_locks (run_id, creature_id, conversation_id, run_scope, acquired_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (run_id, creature_id, conversation_id, final_scope, now),
        )
        conn.execute(
            """
            UPDATE creatures
            SET status = 'running', updated_at = ?
            WHERE id = ?
            """,
            (now, creature_id),
        )
        return conn.execute(
            "SELECT * FROM runs WHERE id = ?",
            (run_id,),
        ).fetchone()


def finish_run(
    run_id: int,
    *,
    creature_id: int,
    status: str,
    raw_output_text: str | None,
    summary: str | None,
    severity: str | None,
    message_text: str | None,
    error_text: str | None,
    next_run_at: datetime | None = None,
    metadata: dict[str, Any] | None = None,
    notes_markdown: str | None = None,
    notes_path: str | None = None,
) -> None:
    finished_at = utcnow()
    metadata_json = json.dumps(metadata or {}, sort_keys=True)
    with connect() as conn:
        conn.execute(
            """
            DELETE FROM run_locks
            WHERE run_id = ?
            """,
            (run_id,),
        )
        remaining_lock = conn.execute(
            """
            SELECT 1
            FROM run_locks
            WHERE creature_id = ?
            LIMIT 1
            """,
            (creature_id,),
        ).fetchone()
        creature_status = "running" if remaining_lock is not None else ("idle" if status == "completed" else "error")
        conn.execute(
            """
            UPDATE runs
            SET status = ?,
                finished_at = ?,
                raw_output_text = ?,
                summary = ?,
                severity = ?,
                message_text = ?,
                error_text = ?,
                notes_markdown = ?,
                metadata_json = ?,
                notes_path = ?
            WHERE id = ?
            """,
            (
                status,
                to_iso(finished_at),
                raw_output_text,
                summary,
                severity,
                message_text,
                error_text,
                notes_markdown,
                metadata_json,
                notes_path,
                run_id,
            ),
        )
        conn.execute(
            """
            UPDATE creatures
            SET status = ?,
                last_run_at = ?,
                next_run_at = ?,
                last_error = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                creature_status,
                to_iso(finished_at),
                to_iso(next_run_at) if next_run_at is not None else None,
                error_text,
                to_iso(finished_at),
                creature_id,
            ),
        )

def create_message(
    creature_id: int,
    *,
    conversation_id: int,
    role: str,
    body: str,
    run_id: int | None = None,
    metadata: dict[str, Any] | None = None,
) -> sqlite3.Row:
    created_dt = utcnow()
    created_at = to_iso(created_dt)
    metadata_json = json.dumps(metadata or {}, sort_keys=True)
    with connect() as conn:
        cursor = conn.execute(
            """
            INSERT INTO messages (creature_id, conversation_id, run_id, role, body, metadata_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (creature_id, conversation_id, run_id, role, body, metadata_json, created_at),
        )
        conn.execute(
            """
            UPDATE conversations
            SET last_message_at = ?, updated_at = ?
            WHERE id = ?
            """,
            (created_at, created_at, conversation_id),
        )
        should_reschedule_after_chat = str(role or "").strip().lower() == "user"
        if not should_reschedule_after_chat and str(role or "").strip().lower() == "creature":
            user_row = conn.execute(
                """
                SELECT 1
                FROM messages
                WHERE conversation_id = ?
                  AND role = 'user'
                LIMIT 1
                """,
                (conversation_id,),
            ).fetchone()
            should_reschedule_after_chat = user_row is not None
        if should_reschedule_after_chat:
            _reschedule_after_chat_habits(
                conn,
                creature_id=creature_id,
                activity_at=created_dt,
            )
        return conn.execute(
            "SELECT * FROM messages WHERE id = ?",
            (int(cursor.lastrowid),),
        ).fetchone()


def get_message(message_id: int) -> sqlite3.Row | None:
    with connect() as conn:
        return conn.execute(
            """
            SELECT *
            FROM messages
            WHERE id = ?
            """,
            (message_id,),
        ).fetchone()


def update_message_metadata(message_id: int, metadata: dict[str, Any] | None) -> None:
    metadata_json = json.dumps(metadata or {}, sort_keys=True)
    with connect() as conn:
        conn.execute(
            """
            UPDATE messages
            SET metadata_json = ?
            WHERE id = ?
            """,
            (metadata_json, message_id),
        )


def create_run_event(
    run_id: int,
    *,
    event_type: str,
    body: str = "",
    metadata: dict[str, Any] | None = None,
) -> sqlite3.Row:
    created_at = to_iso(utcnow())
    metadata_json = json.dumps(metadata or {}, sort_keys=True)
    with connect() as conn:
        cursor = conn.execute(
            """
            INSERT INTO run_events (run_id, event_type, body, metadata_json, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (run_id, event_type, body, metadata_json, created_at),
        )
        return conn.execute(
            "SELECT * FROM run_events WHERE id = ?",
            (int(cursor.lastrowid),),
        ).fetchone()


def update_message_body(message_id: int, body: str) -> None:
    with connect() as conn:
        conn.execute(
            """
            UPDATE messages
            SET body = ?
            WHERE id = ?
            """,
            (body, message_id),
        )


def delete_message(message_id: int) -> None:
    with connect() as conn:
        conn.execute(
            """
            DELETE FROM messages
            WHERE id = ?
            """,
            (message_id,),
        )


def list_messages(conversation_id: int, *, limit: int = 200) -> list[sqlite3.Row]:
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM messages
            WHERE conversation_id = ?
            ORDER BY created_at DESC, id DESC
            LIMIT ?
            """,
            (conversation_id, limit),
        ).fetchall()
    return list(reversed(rows))


def recent_messages(conversation_id: int, *, limit: int = 8) -> list[sqlite3.Row]:
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM messages
            WHERE conversation_id = ?
            ORDER BY created_at DESC, id DESC
            LIMIT ?
            """,
            (conversation_id, limit),
        ).fetchall()
    return list(reversed(rows))


def recent_runs(creature_id: int, *, limit: int = 15) -> list[sqlite3.Row]:
    with connect() as conn:
        return conn.execute(
            """
            SELECT *
            FROM runs
            WHERE creature_id = ?
            ORDER BY started_at DESC, id DESC
            LIMIT ?
            """,
            (creature_id, limit),
        ).fetchall()


def list_running_runs() -> list[sqlite3.Row]:
    with connect() as conn:
        return conn.execute(
            """
            SELECT *
            FROM runs
            WHERE status = 'running'
            ORDER BY started_at ASC, id ASC
            """
        ).fetchall()


def latest_run_for_conversation(conversation_id: int) -> sqlite3.Row | None:
    with connect() as conn:
        return conn.execute(
            """
            SELECT *
            FROM runs
            WHERE conversation_id = ?
            ORDER BY started_at DESC, id DESC
            LIMIT 1
            """,
            (conversation_id,),
        ).fetchone()


def list_run_events(run_id: int, *, after_id: int = 0, limit: int = 500) -> list[sqlite3.Row]:
    with connect() as conn:
        return conn.execute(
            """
            SELECT *
            FROM run_events
            WHERE run_id = ?
              AND id > ?
            ORDER BY id ASC
            LIMIT ?
            """,
            (run_id, after_id, limit),
        ).fetchall()


def get_run(run_id: int) -> sqlite3.Row | None:
    with connect() as conn:
        return conn.execute(
            "SELECT * FROM runs WHERE id = ?",
            (run_id,),
        ).fetchone()


def update_run_message_text(run_id: int, message_text: str) -> None:
    with connect() as conn:
        conn.execute(
            """
            UPDATE runs
            SET message_text = ?
            WHERE id = ?
            """,
            (message_text, run_id),
        )


def latest_running_run_for_creature(creature_id: int) -> sqlite3.Row | None:
    with connect() as conn:
        return conn.execute(
            """
            SELECT r.*
            FROM runs r
            INNER JOIN run_locks l ON l.run_id = r.id
            WHERE l.creature_id = ?
            ORDER BY r.started_at DESC, r.id DESC
            LIMIT 1
            """,
            (creature_id,),
        ).fetchone()


def latest_running_activity_run_for_creature(creature_id: int) -> sqlite3.Row | None:
    with connect() as conn:
        return conn.execute(
            """
            SELECT r.*
            FROM runs r
            INNER JOIN run_locks l ON l.run_id = r.id
            WHERE l.creature_id = ?
              AND l.run_scope = 'activity'
            ORDER BY r.started_at DESC, r.id DESC
            LIMIT 1
            """,
            (creature_id,),
        ).fetchone()


def latest_running_run_for_conversation(conversation_id: int) -> sqlite3.Row | None:
    with connect() as conn:
        return conn.execute(
            """
            SELECT r.*
            FROM runs r
            INNER JOIN run_locks l ON l.run_id = r.id
            WHERE l.run_scope = 'chat'
              AND l.conversation_id = ?
            ORDER BY r.started_at DESC, r.id DESC
            LIMIT 1
            """,
            (conversation_id,),
        ).fetchone()


def list_running_runs_for_creature(creature_id: int) -> list[sqlite3.Row]:
    with connect() as conn:
        return conn.execute(
            """
            SELECT r.*
            FROM runs r
            INNER JOIN run_locks l ON l.run_id = r.id
            WHERE l.creature_id = ?
            ORDER BY r.started_at DESC, r.id DESC
            """,
            (creature_id,),
        ).fetchall()


def get_state_surface_revision(creature_id: int, doc_key: str) -> int:
    now = to_iso(utcnow())
    with connect() as conn:
        conn.execute(
            f"""
            INSERT INTO {STATE_SURFACES_TABLE} (creature_id, doc_key, content_text, revision, updated_at)
            VALUES (?, ?, '', 0, ?)
            ON CONFLICT(creature_id, doc_key) DO NOTHING
            """,
            (creature_id, doc_key, now),
        )
        row = conn.execute(
            f"""
            SELECT revision
            FROM {STATE_SURFACES_TABLE}
            WHERE creature_id = ? AND doc_key = ?
            """,
            (creature_id, doc_key),
        ).fetchone()
        return int(row["revision"] or 0)


def increment_state_surface_revision(creature_id: int, doc_key: str, *, expected_revision: int | None = None) -> int:
    now = to_iso(utcnow())
    with connect() as conn:
        conn.execute(
            f"""
            INSERT INTO {STATE_SURFACES_TABLE} (creature_id, doc_key, content_text, revision, updated_at)
            VALUES (?, ?, '', 0, ?)
            ON CONFLICT(creature_id, doc_key) DO NOTHING
            """,
            (creature_id, doc_key, now),
        )
        row = conn.execute(
            f"""
            SELECT revision
            FROM {STATE_SURFACES_TABLE}
            WHERE creature_id = ? AND doc_key = ?
            """,
            (creature_id, doc_key),
        ).fetchone()
        current = int(row["revision"] or 0) if row is not None else 0
        if expected_revision is not None and current != expected_revision:
            raise RuntimeError(f"State surface revision conflict for {doc_key}: expected {expected_revision}, found {current}")
        next_revision = current + 1
        conn.execute(
            f"""
            UPDATE {STATE_SURFACES_TABLE}
            SET revision = ?, updated_at = ?
            WHERE creature_id = ? AND doc_key = ?
            """,
            (next_revision, now, creature_id, doc_key),
        )
        return next_revision


def get_state_surface_content(creature_id: int, doc_key: str) -> str:
    now = to_iso(utcnow())
    with connect() as conn:
        conn.execute(
            f"""
            INSERT INTO {STATE_SURFACES_TABLE} (creature_id, doc_key, content_text, revision, updated_at)
            VALUES (?, ?, '', 0, ?)
            ON CONFLICT(creature_id, doc_key) DO NOTHING
            """,
            (creature_id, doc_key, now),
        )
        row = conn.execute(
            f"""
            SELECT content_text
            FROM {STATE_SURFACES_TABLE}
            WHERE creature_id = ? AND doc_key = ?
            """,
            (creature_id, doc_key),
        ).fetchone()
        return str(row["content_text"] or "")


def update_state_surface(
    creature_id: int,
    doc_key: str,
    *,
    content: str,
    expected_revision: int | None = None,
) -> int:
    now = to_iso(utcnow())
    normalized = str(content or "")
    with connect() as conn:
        conn.execute(
            f"""
            INSERT INTO {STATE_SURFACES_TABLE} (creature_id, doc_key, content_text, revision, updated_at)
            VALUES (?, ?, '', 0, ?)
            ON CONFLICT(creature_id, doc_key) DO NOTHING
            """,
            (creature_id, doc_key, now),
        )
        row = conn.execute(
            f"""
            SELECT revision, content_text
            FROM {STATE_SURFACES_TABLE}
            WHERE creature_id = ? AND doc_key = ?
            """,
            (creature_id, doc_key),
        ).fetchone()
        current_revision = int(row["revision"] or 0) if row is not None else 0
        current_content = str(row["content_text"] or "") if row is not None else ""
        if expected_revision is not None and current_revision != expected_revision:
            raise RuntimeError(
                f"State surface revision conflict for {doc_key}: expected {expected_revision}, found {current_revision}"
            )
        if current_content == normalized:
            conn.execute(
                f"""
                UPDATE {STATE_SURFACES_TABLE}
                SET updated_at = ?
                WHERE creature_id = ? AND doc_key = ?
                """,
                (now, creature_id, doc_key),
            )
            return current_revision
        next_revision = current_revision + 1
        conn.execute(
            f"""
            UPDATE {STATE_SURFACES_TABLE}
            SET content_text = ?, revision = ?, updated_at = ?
            WHERE creature_id = ? AND doc_key = ?
            """,
            (normalized, next_revision, now, creature_id, doc_key),
        )
        return next_revision


def rewrite_state_surface_text(creature_id: int, *, doc_key: str, old_text: str, new_text: str) -> None:
    now = to_iso(utcnow())
    with connect() as conn:
        conn.execute(
            f"""
            UPDATE {STATE_SURFACES_TABLE}
            SET content_text = REPLACE(content_text, ?, ?),
                updated_at = ?
            WHERE creature_id = ?
              AND doc_key = ?
              AND content_text LIKE ?
            """,
            (old_text, new_text, now, creature_id, doc_key, f"%{old_text}%"),
        )


def list_agenda_items(creature_id: int, *, limit: int = 200) -> list[sqlite3.Row]:
    with connect() as conn:
        return conn.execute(
            """
            SELECT *
            FROM agenda_items
            WHERE creature_id = ?
            ORDER BY ordinal ASC, id ASC
            LIMIT ?
            """,
            (creature_id, limit),
        ).fetchall()


def replace_agenda_items(
    creature_id: int,
    items: list[dict[str, Any]],
    *,
    source_run_id: int | None = None,
    source_message_id: int | None = None,
) -> list[sqlite3.Row]:
    now = to_iso(utcnow())
    with connect() as conn:
        conn.execute(
            """
            DELETE FROM agenda_items
            WHERE creature_id = ?
            """,
            (creature_id,),
        )
        for index, item in enumerate(items):
            conn.execute(
                """
                INSERT INTO agenda_items (
                    creature_id, title, priority, details, spawn_conversation, ordinal,
                    source_message_id, source_run_id, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    creature_id,
                    str(item.get("title") or ""),
                    str(item.get("priority") or "medium"),
                    str(item.get("details") or ""),
                    1 if bool(item.get("spawn_conversation")) else 0,
                    index,
                    source_message_id,
                    source_run_id,
                    now,
                    now,
                ),
            )
        return conn.execute(
            """
            SELECT *
            FROM agenda_items
            WHERE creature_id = ?
            ORDER BY ordinal ASC, id ASC
            """,
            (creature_id,),
        ).fetchall()


def list_backlog_items(creature_id: int, *, limit: int = 200) -> list[sqlite3.Row]:
    with connect() as conn:
        return conn.execute(
            """
            SELECT *
            FROM backlog_items
            WHERE creature_id = ?
            ORDER BY ordinal ASC, id ASC
            LIMIT ?
            """,
            (creature_id, limit),
        ).fetchall()


def replace_backlog_items(
    creature_id: int,
    items: list[str],
    *,
    source_run_id: int | None = None,
    source_message_id: int | None = None,
) -> list[sqlite3.Row]:
    now = to_iso(utcnow())
    with connect() as conn:
        conn.execute(
            """
            DELETE FROM backlog_items
            WHERE creature_id = ?
            """,
            (creature_id,),
        )
        for index, body in enumerate(items):
            conn.execute(
                """
                INSERT INTO backlog_items (
                    creature_id, body, ordinal, source_message_id, source_run_id, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    creature_id,
                    str(body or ""),
                    index,
                    source_message_id,
                    source_run_id,
                    now,
                    now,
                ),
            )
        return conn.execute(
            """
            SELECT *
            FROM backlog_items
            WHERE creature_id = ?
            ORDER BY ordinal ASC, id ASC
            """,
            (creature_id,),
        ).fetchall()


def list_habits(creature_id: int, *, include_disabled: bool = True, limit: int = 200) -> list[sqlite3.Row]:
    query = """
        SELECT *
        FROM creature_habits
        WHERE creature_id = ?
    """
    params: list[Any] = [creature_id]
    if not include_disabled:
        query += " AND enabled = 1"
    query += " ORDER BY updated_at DESC, id DESC LIMIT ?"
    params.append(limit)
    with connect() as conn:
        return conn.execute(query, params).fetchall()


def get_habit(habit_id: int) -> sqlite3.Row | None:
    with connect() as conn:
        return conn.execute(
            """
            SELECT *
            FROM creature_habits
            WHERE id = ?
            """,
            (habit_id,),
        ).fetchone()


def get_habit_for_creature(creature_id: int, habit_id: int) -> sqlite3.Row | None:
    with connect() as conn:
        return conn.execute(
            """
            SELECT *
            FROM creature_habits
            WHERE creature_id = ? AND id = ?
            """,
            (creature_id, habit_id),
        ).fetchone()


def create_habit(
    creature_id: int,
    *,
    slug: str,
    title: str,
    instructions: str,
    schedule_kind: str,
    schedule_json: dict[str, Any] | None = None,
    enabled: bool = True,
    next_run_at: datetime | None = None,
) -> sqlite3.Row:
    now = utcnow()
    with connect() as conn:
        cursor = conn.execute(
            """
            INSERT INTO creature_habits (
                creature_id, slug, title, instructions, schedule_kind, schedule_json, enabled,
                next_run_at, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                creature_id,
                slug,
                title,
                instructions,
                schedule_kind,
                json.dumps(schedule_json or {}, sort_keys=True),
                1 if enabled else 0,
                to_iso(next_run_at) if next_run_at is not None else None,
                to_iso(now),
                to_iso(now),
            ),
        )
        return conn.execute(
            "SELECT * FROM creature_habits WHERE id = ?",
            (int(cursor.lastrowid),),
        ).fetchone()


def update_habit(
    habit_id: int,
    *,
    title: str,
    instructions: str,
    schedule_kind: str,
    schedule_json: dict[str, Any] | None,
    enabled: bool,
    next_run_at: datetime | None = None,
) -> None:
    now = utcnow()
    with connect() as conn:
        conn.execute(
            """
            UPDATE creature_habits
            SET title = ?,
                instructions = ?,
                schedule_kind = ?,
                schedule_json = ?,
                enabled = ?,
                next_run_at = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                title,
                instructions,
                schedule_kind,
                json.dumps(schedule_json or {}, sort_keys=True),
                1 if enabled else 0,
                to_iso(next_run_at) if next_run_at is not None else None,
                to_iso(now),
                habit_id,
            ),
        )


def update_habit_enabled(habit_id: int, *, enabled: bool, next_run_at: datetime | None = None) -> None:
    now = utcnow()
    with connect() as conn:
        conn.execute(
            """
            UPDATE creature_habits
            SET enabled = ?,
                next_run_at = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                1 if enabled else 0,
                to_iso(next_run_at) if next_run_at is not None else None,
                to_iso(now),
                habit_id,
            ),
        )


def pause_habit(habit_id: int) -> None:
    update_habit_enabled(habit_id, enabled=False, next_run_at=None)


def resume_habit(habit_id: int, *, next_run_at: datetime | None = None) -> None:
    update_habit_enabled(habit_id, enabled=True, next_run_at=next_run_at)


def touch_habit_now(habit_id: int) -> None:
    now = utcnow()
    with connect() as conn:
        conn.execute(
            """
            UPDATE creature_habits
            SET enabled = 1,
                next_run_at = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (to_iso(now), to_iso(now), habit_id),
        )


def delete_habit(habit_id: int) -> None:
    with connect() as conn:
        conn.execute(
            """
            DELETE FROM creature_habits
            WHERE id = ?
            """,
            (habit_id,),
        )


def record_habit_run_finish(
    habit_id: int,
    *,
    status: str,
    summary: str = "",
    error_text: str = "",
    next_run_at: datetime | None = None,
    report_path: str = "",
) -> None:
    now = utcnow()
    with connect() as conn:
        conn.execute(
            """
            UPDATE creature_habits
            SET last_run_at = ?,
                next_run_at = ?,
                last_status = ?,
                last_summary = ?,
                last_error = ?,
                last_report_path = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                to_iso(now),
                to_iso(next_run_at) if next_run_at is not None else None,
                status,
                summary,
                error_text,
                report_path,
                to_iso(now),
                habit_id,
            ),
        )


def due_habits() -> list[sqlite3.Row]:
    now = to_iso(utcnow())
    with connect() as conn:
        return conn.execute(
            """
            SELECT
                t.*,
                a.slug AS creature_slug,
                a.display_name AS creature_display_name
            FROM creature_habits t
            INNER JOIN creatures a ON a.id = t.creature_id
            WHERE t.enabled = 1
              AND t.next_run_at IS NOT NULL
              AND t.next_run_at <= ?
            ORDER BY t.next_run_at ASC, t.id ASC
            """,
            (now,),
        ).fetchall()


def list_memory_records(creature_id: int, *, include_inactive: bool = True, limit: int = 200) -> list[sqlite3.Row]:
    query = """
        SELECT *
        FROM memory_records
        WHERE creature_id = ?
    """
    params: list[Any] = [creature_id]
    if not include_inactive:
        query += " AND status = 'active'"
    query += " ORDER BY updated_at DESC, id DESC LIMIT ?"
    params.append(limit)
    with connect() as conn:
        return conn.execute(query, params).fetchall()


def get_memory_record(record_id: int) -> sqlite3.Row | None:
    with connect() as conn:
        return conn.execute(
            """
            SELECT *
            FROM memory_records
            WHERE id = ?
            """,
            (record_id,),
        ).fetchone()


def create_memory_event(
    creature_id: int,
    *,
    record_id: int | None,
    actor_type: str,
    action: str,
    reason: str = "",
    metadata: dict[str, Any] | None = None,
) -> sqlite3.Row:
    created_at = to_iso(utcnow())
    metadata_json = json.dumps(metadata or {}, sort_keys=True)
    with connect() as conn:
        cursor = conn.execute(
            """
            INSERT INTO memory_events (creature_id, record_id, actor_type, action, reason, metadata_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (creature_id, record_id, actor_type, action, reason, metadata_json, created_at),
        )
        return conn.execute(
            """
            SELECT *
            FROM memory_events
            WHERE id = ?
            """,
            (int(cursor.lastrowid),),
        ).fetchone()


def list_memory_events(creature_id: int, *, limit: int = 200) -> list[sqlite3.Row]:
    with connect() as conn:
        return conn.execute(
            """
            SELECT *
            FROM memory_events
            WHERE creature_id = ?
            ORDER BY created_at DESC, id DESC
            LIMIT ?
            """,
            (creature_id, limit),
        ).fetchall()


def create_memory_record(
    creature_id: int,
    *,
    kind: str,
    body: str,
    actor_type: str,
    reason: str = "",
    source_message_id: int | None = None,
    source_run_id: int | None = None,
    previous_record_id: int | None = None,
    metadata: dict[str, Any] | None = None,
    event_action: str = "remember",
) -> sqlite3.Row:
    now = to_iso(utcnow())
    metadata_json = json.dumps(metadata or {}, sort_keys=True)
    with connect() as conn:
        cursor = conn.execute(
            """
            INSERT INTO memory_records (
                creature_id, kind, body, status, source_message_id, source_run_id,
                previous_record_id, superseded_by_id, metadata_json, created_at, updated_at
            ) VALUES (?, ?, ?, 'active', ?, ?, ?, NULL, ?, ?, ?)
            """,
            (
                creature_id,
                kind,
                body,
                source_message_id,
                source_run_id,
                previous_record_id,
                metadata_json,
                now,
                now,
            ),
        )
        record_id = int(cursor.lastrowid)
        conn.execute(
            """
            INSERT INTO memory_events (creature_id, record_id, actor_type, action, reason, metadata_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (creature_id, record_id, actor_type, event_action, reason, metadata_json, now),
        )
        return conn.execute(
            """
            SELECT *
            FROM memory_records
            WHERE id = ?
            """,
            (record_id,),
        ).fetchone()


def update_memory_record_status(
    record_id: int,
    *,
    status: str,
    actor_type: str,
    action: str,
    reason: str = "",
    superseded_by_id: int | None = None,
    metadata: dict[str, Any] | None = None,
) -> sqlite3.Row | None:
    now = to_iso(utcnow())
    metadata_json = json.dumps(metadata or {}, sort_keys=True)
    with connect() as conn:
        record = conn.execute(
            """
            SELECT *
            FROM memory_records
            WHERE id = ?
            """,
            (record_id,),
        ).fetchone()
        if record is None:
            return None
        conn.execute(
            """
            UPDATE memory_records
            SET status = ?,
                superseded_by_id = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (status, superseded_by_id, now, record_id),
        )
        conn.execute(
            """
            INSERT INTO memory_events (creature_id, record_id, actor_type, action, reason, metadata_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (int(record["creature_id"]), record_id, actor_type, action, reason, metadata_json, now),
        )
        return conn.execute(
            """
            SELECT *
            FROM memory_records
            WHERE id = ?
            """,
            (record_id,),
        ).fetchone()


def update_memory_record_metadata(record_id: int, metadata: dict[str, Any] | None) -> sqlite3.Row | None:
    now = to_iso(utcnow())
    metadata_json = json.dumps(metadata or {}, sort_keys=True)
    with connect() as conn:
        record = conn.execute(
            """
            SELECT *
            FROM memory_records
            WHERE id = ?
            """,
            (record_id,),
        ).fetchone()
        if record is None:
            return None
        conn.execute(
            """
            UPDATE memory_records
            SET metadata_json = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (metadata_json, now, record_id),
        )
        return conn.execute(
            """
            SELECT *
            FROM memory_records
            WHERE id = ?
            """,
            (record_id,),
        ).fetchone()

def pending_conversation(creature_id: int) -> sqlite3.Row | None:
    with connect() as conn:
        return conn.execute(
            """
            SELECT c.*
            FROM conversations c
            WHERE c.creature_id = ?
              AND EXISTS (
                  SELECT 1
                  FROM messages mu
                  WHERE mu.conversation_id = c.id
                    AND mu.role = 'user'
                    AND NOT EXISTS (
                        SELECT 1
                        FROM messages ma
                        WHERE ma.conversation_id = c.id
                          AND ma.role = 'creature'
                          AND (
                              ma.created_at > mu.created_at
                              OR (ma.created_at = mu.created_at AND ma.id > mu.id)
                          )
                    )
              )
            ORDER BY COALESCE(c.last_message_at, c.updated_at) DESC, c.id DESC
            LIMIT 1
            """,
            (creature_id,),
        ).fetchone()


def pending_conversation_count(creature_id: int) -> int:
    with connect() as conn:
        row = conn.execute(
            """
            SELECT COUNT(*) AS count
            FROM conversations c
            WHERE c.creature_id = ?
              AND EXISTS (
                  SELECT 1
                  FROM messages mu
                  WHERE mu.conversation_id = c.id
                    AND mu.role = 'user'
                    AND NOT EXISTS (
                        SELECT 1
                        FROM messages ma
                        WHERE ma.conversation_id = c.id
                          AND ma.role = 'creature'
                          AND (
                              ma.created_at > mu.created_at
                              OR (ma.created_at = mu.created_at AND ma.id > mu.id)
                          )
                    )
              )
            """,
            (creature_id,),
        ).fetchone()
        return int(row["count"] or 0)


def mark_creature_error(creature_id: int, error_text: str) -> None:
    now = to_iso(utcnow())
    with connect() as conn:
        conn.execute(
            """
            UPDATE creatures
            SET status = 'error',
                last_error = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (error_text, now, creature_id),
        )


def clear_creature_runtime_error(creature_id: int, *, next_run_at: datetime | None = None) -> None:
    now = utcnow()
    scheduled = next_run_at or now
    with connect() as conn:
        conn.execute(
            """
            UPDATE creatures
            SET status = 'idle',
                last_error = NULL,
                next_run_at = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (to_iso(scheduled), to_iso(now), creature_id),
        )

def set_creature_owner_reference_override(creature_id: int, owner_reference_override: str | None) -> None:
    now = to_iso(utcnow())
    with connect() as conn:
        conn.execute(
            """
            UPDATE creatures
            SET owner_reference_override = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (owner_reference_override, now, creature_id),
        )


def set_creature_thinking_overrides(
    creature_id: int,
    *,
    model_override: str | None,
    reasoning_effort_override: str | None,
) -> None:
    now = to_iso(utcnow())
    with connect() as conn:
        conn.execute(
            """
            UPDATE creatures
            SET model_override = ?,
                reasoning_effort_override = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (model_override, reasoning_effort_override, now, creature_id),
        )


def set_creature_identity_policy(
    creature_id: int,
    *,
    is_pinned: bool | None = None,
    can_delete: bool | None = None,
) -> None:
    now = to_iso(utcnow())
    with connect() as conn:
        conn.execute(
            """
            UPDATE creatures
            SET is_pinned = COALESCE(?, is_pinned),
                can_delete = COALESCE(?, can_delete),
                updated_at = ?
            WHERE id = ?
            """,
            (
                None if is_pinned is None else (1 if is_pinned else 0),
                None if can_delete is None else (1 if can_delete else 0),
                now,
                creature_id,
            ),
        )


def delete_creature(creature_id: int) -> None:
    with connect() as conn:
        conn.execute("DELETE FROM creatures WHERE id = ?", (creature_id,))


def set_run_notes_markdown(run_id: int, notes_markdown: str, *, clear_notes_path: bool = False) -> None:
    with connect() as conn:
        if clear_notes_path:
            conn.execute(
                """
                UPDATE runs
                SET notes_markdown = ?,
                    notes_path = NULL
                WHERE id = ?
                """,
                (notes_markdown, run_id),
            )
        else:
            conn.execute(
                """
                UPDATE runs
                SET notes_markdown = ?
                WHERE id = ?
                """,
                (notes_markdown, run_id),
            )

def update_memory_record_body(record_id: int, body: str) -> None:
    with connect() as conn:
        conn.execute(
            """
            UPDATE memory_records
            SET body = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (body, to_iso(utcnow()), record_id),
        )


def find_conversation_by_title(creature_id: int, title: str) -> sqlite3.Row | None:
    with connect() as conn:
        return conn.execute(
            """
            SELECT *
            FROM conversations
            WHERE creature_id = ? AND title = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (creature_id, title),
        ).fetchone()


def find_conversation_by_source_run(source_run_id: int) -> sqlite3.Row | None:
    with connect() as conn:
        return conn.execute(
            """
            SELECT *
            FROM conversations
            WHERE source_run_id = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (source_run_id,),
        ).fetchone()


def get_meta(key: str) -> str | None:
    with connect() as conn:
        row = conn.execute("SELECT value FROM app_meta WHERE key = ?", (key,)).fetchone()
        return str(row["value"]) if row is not None else None


def set_meta(key: str, value: str) -> None:
    with connect() as conn:
        conn.execute(
            """
            INSERT INTO app_meta(key, value)
            VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (key, value),
        )


def delete_meta(key: str) -> None:
    with connect() as conn:
        conn.execute("DELETE FROM app_meta WHERE key = ?", (key,))
