from __future__ import annotations

import asyncio
import ipaddress
import json
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode

from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from . import config
from . import service

SERVER_SOURCE_REVISION = str(os.getenv("CREATURE_OS_SOURCE_REVISION") or config.server_source_revision()).strip()
STATIC_VERSION = str(os.getenv("CREATURE_OS_STATIC_VERSION") or config.static_version(SERVER_SOURCE_REVISION)).strip()
SERVER_BOOTED_AT = str(os.getenv("CREATURE_OS_SERVER_BOOTED_AT") or "").strip()
SERVER_SUPERVISOR_PID = str(os.getenv("CREATURE_OS_SUPERVISOR_PID") or "").strip()
BACKGROUND_DUE_RUN_INTERVAL_SECONDS = 20.0
_TAILSCALE_NETWORKS = (
    ipaddress.ip_network("100.64.0.0/10"),
    ipaddress.ip_network("fd7a:115c:a1e0::/48"),
)


def _server_ready_payload() -> dict[str, object]:
    return {
        "status": "ready",
        "worker_pid": os.getpid(),
        "source_revision": SERVER_SOURCE_REVISION,
        "booted_at": SERVER_BOOTED_AT,
        "ready_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
    }


def _write_server_ready_file() -> None:
    config.server_ready_path().write_text(
        json.dumps(_server_ready_payload(), sort_keys=True),
        encoding="utf-8",
    )


def _clear_server_ready_file() -> None:
    path = config.server_ready_path()
    if not path.exists():
        return
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        payload = {}
    if int(payload.get("worker_pid") or 0) in {0, os.getpid()}:
        path.unlink(missing_ok=True)

def _is_trusted_client(request: Request) -> bool:
    forwarded_headers = {"x-forwarded-for", "x-forwarded-host", "x-forwarded-proto", "forwarded"}
    if any(header in request.headers for header in forwarded_headers):
        return False
    client_host = (request.client.host if request.client else "") or ""
    if client_host == "testclient":
        return True
    try:
        ip = ipaddress.ip_address(client_host)
    except ValueError:
        return False
    return bool(ip.is_loopback or any(ip in network for network in _TAILSCALE_NETWORKS))


def _index_url(
    *,
    creature: str | None = None,
    view: str | None = None,
    notice: str | None = None,
    open_intro: bool = False,
    draft_chat: bool = False,
    conversation_id: int | None = None,
    run_id: int | None = None,
    habit: str | None = None,
) -> str:
    params: dict[str, str] = {}
    if creature:
        params["creature"] = creature
    if view:
        params["view"] = view
    if notice:
        params["notice"] = notice
    if open_intro:
        params["intro"] = "1"
    if draft_chat:
        params["new_chat"] = "1"
    if conversation_id is not None:
        params["chat"] = str(conversation_id)
    if run_id is not None:
        params["run"] = str(run_id)
    if habit:
        params["habit"] = str(habit)
    query = urlencode(params)
    return f"/?{query}" if query else "/"


async def _background_initial_sync() -> None:
    try:
        await asyncio.to_thread(service.ensure_initial_creatures)
    except Exception:
        return


async def _background_due_runner() -> None:
    while True:
        try:
            await asyncio.to_thread(service.run_due_creatures, force_message=False)
        except asyncio.CancelledError:
            raise
        except Exception:
            pass
        await asyncio.sleep(BACKGROUND_DUE_RUN_INTERVAL_SECONDS)


@asynccontextmanager
async def lifespan(app: FastAPI):
    _clear_server_ready_file()
    await asyncio.to_thread(service.ensure_runtime_ready)
    await asyncio.to_thread(service.reconcile_stranded_runs)
    await asyncio.to_thread(service.prewarm_onboarding_assets)
    app.state.initial_sync_job = asyncio.create_task(_background_initial_sync())
    app.state.due_runner_job = asyncio.create_task(_background_due_runner())
    _write_server_ready_file()
    try:
        yield
    finally:
        _clear_server_ready_file()
        initial_sync_job = getattr(app.state, "initial_sync_job", None)
        if initial_sync_job is not None and not initial_sync_job.done():
            initial_sync_job.cancel()
        due_runner_job = getattr(app.state, "due_runner_job", None)
        if due_runner_job is not None and not due_runner_job.done():
            due_runner_job.cancel()


app = FastAPI(title="CreatureOS", lifespan=lifespan)
templates = Jinja2Templates(directory=str(config.template_dir()))
app.mount("/static", StaticFiles(directory=str(config.static_dir())), name="static")


@app.middleware("http")
async def local_only_middleware(request: Request, call_next):
    if not _is_trusted_client(request):
        return HTMLResponse("Not Found", status_code=404)
    return await call_next(request)


@app.middleware("http")
async def no_store_middleware(request: Request, call_next):
    response = await call_next(request)
    if request.method == "GET" and request.url.path != "/healthz" and not request.url.path.startswith("/static/"):
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    if request.method == "GET" and request.url.path.startswith("/static/"):
        suffix = Path(request.url.path).suffix.lower()
        if suffix in {".js", ".css"}:
            response.headers["Cache-Control"] = "no-cache, max-age=0, must-revalidate"
    return response


@app.get("/", response_class=HTMLResponse)
async def index(
    request: Request,
    creature: str | None = None,
    view: str = "creature",
    notice: str | None = None,
    intro: str | None = None,
    new_chat: str | None = None,
    chat: int | None = None,
    conversation: int | None = None,
    run: int | None = None,
    habit: str | None = None,
) -> HTMLResponse:
    active_chat = chat if chat is not None else conversation
    canonical_creature = service.canonical_creature_slug(creature)
    if creature and canonical_creature != creature:
        return RedirectResponse(
            url=_index_url(
                creature=canonical_creature,
                view=view,
                notice=notice,
                open_intro=str(intro or "").strip() == "1",
                draft_chat=str(new_chat or "").strip() == "1",
                conversation_id=active_chat,
                run_id=run,
                habit=habit,
            ),
            status_code=303,
        )
    if canonical_creature:
        await asyncio.to_thread(service.record_last_viewed_creature, canonical_creature)
        if habit is not None and str(habit).strip().isdigit():
            await asyncio.to_thread(service.record_last_viewed_habit, canonical_creature, int(str(habit).strip()))
    if canonical_creature and str(intro or "").strip() == "1":
        intro_conversation = await asyncio.to_thread(service.ensure_intro_conversation, canonical_creature)
        if intro_conversation is not None:
            await asyncio.to_thread(service.mark_intro_surfaced, canonical_creature)
            return RedirectResponse(
                url=_index_url(
                    creature=canonical_creature,
                    view="chats",
                    notice=notice,
                    conversation_id=int(intro_conversation["id"]),
                    run_id=run,
                ),
                status_code=303,
            )
    state = await asyncio.to_thread(
        service.dashboard_state,
        selected_slug=canonical_creature,
        view=view,
        notice=notice,
        conversation_id=active_chat,
        run_id=run,
        habit_target=habit,
        draft_chat=str(new_chat or "").strip() == "1",
    )
    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "request": request,
            "static_version": STATIC_VERSION,
            "action_notice": str(notice or "").strip(),
            **state,
        },
    )


@app.post("/creatures/{slug}/run")
async def run_creature(
    slug: str,
    conversation_id: int | None = Form(None),
    view: str = Form("chats"),
    run_id: int | None = Form(None),
) -> RedirectResponse:
    slug = str(service.canonical_creature_slug(slug))
    await asyncio.to_thread(
        service.start_background_run,
        slug,
        trigger_type="manual",
        force_message=True,
        conversation_id=conversation_id,
        allow_code_changes=True,
        run_scope=service.RUN_SCOPE_ACTIVITY,
    )
    return RedirectResponse(
        url=_index_url(creature=slug, view=view, conversation_id=conversation_id, run_id=run_id),
        status_code=303,
    )


@app.post("/creatures/run-due")
async def run_due_creatures(
    creature: str | None = Form(None),
    view: str = Form("chats"),
    conversation_id: int | None = Form(None),
    run_id: int | None = Form(None),
) -> RedirectResponse:
    await asyncio.to_thread(service.run_due_creatures, force_message=True)
    return RedirectResponse(
        url=_index_url(creature=creature, view=view, conversation_id=conversation_id, run_id=run_id),
        status_code=303,
    )


@app.post("/creatures/{slug}/chats")
@app.post("/creatures/{slug}/conversations")
async def create_conversation(
    slug: str,
    title: str = Form(""),
) -> RedirectResponse:
    slug = str(service.canonical_creature_slug(slug))
    conversation = await asyncio.to_thread(service.create_conversation, slug, title=title or None)
    return RedirectResponse(
        url=_index_url(creature=slug, view="chats", conversation_id=int(conversation["id"])),
        status_code=303,
    )


@app.post("/creatures/{slug}/habits/new-chat")
async def create_habit_teaching_conversation(
    slug: str,
) -> RedirectResponse:
    slug = str(service.canonical_creature_slug(slug))
    conversation = await asyncio.to_thread(service.create_habit_teaching_conversation, slug)
    return RedirectResponse(
        url=_index_url(creature=slug, view="chats", conversation_id=int(conversation["id"])),
        status_code=303,
    )


@app.post("/creatures/{slug}/chats/{conversation_id}/rename")
@app.post("/creatures/{slug}/conversations/{conversation_id}/rename")
async def rename_conversation(
    request: Request,
    slug: str,
    conversation_id: int,
    title: str = Form(""),
) -> Response:
    slug = str(service.canonical_creature_slug(slug))
    updated = None
    cleaned_title = " ".join(title.strip().split())
    if cleaned_title:
        updated = await asyncio.to_thread(service.rename_conversation, slug, conversation_id, title=cleaned_title)
    redirect_url = _index_url(creature=slug, view="chats", conversation_id=conversation_id)
    if request.headers.get("x-creatureos-ajax") == "1":
        return JSONResponse({"redirect_url": redirect_url, "title": str(updated["title"] if updated else cleaned_title)})
    return RedirectResponse(url=redirect_url, status_code=303)


@app.post("/creatures/{slug}/chats/{conversation_id}/delete")
@app.post("/creatures/{slug}/conversations/{conversation_id}/delete")
async def delete_conversation(
    request: Request,
    slug: str,
    conversation_id: int,
    current_conversation_id: int | None = Form(None),
) -> Response:
    slug = str(service.canonical_creature_slug(slug))
    try:
        deleted = await asyncio.to_thread(
            service.delete_conversation,
            slug,
            conversation_id,
            current_conversation_id=current_conversation_id,
        )
        redirect_url = _index_url(
            creature=slug,
            view="chats",
            conversation_id=int(deleted["redirect_conversation_id"]) if deleted["redirect_conversation_id"] is not None else None,
        )
        if request.headers.get("x-creatureos-ajax") == "1":
            return JSONResponse({"redirect_url": redirect_url, "deleted_conversation_id": int(conversation_id)})
        return RedirectResponse(url=redirect_url, status_code=303)
    except ValueError:
        redirect_url = _index_url(
            creature=slug,
            view="chats",
            conversation_id=current_conversation_id or conversation_id,
        )
        if request.headers.get("x-creatureos-ajax") == "1":
            return JSONResponse({"redirect_url": redirect_url, "deleted_conversation_id": None}, status_code=400)
        return RedirectResponse(url=redirect_url, status_code=303)


@app.post("/settings/thinking")
async def update_default_thinking(
    request: Request,
    model: str = Form(""),
    reasoning_effort: str = Form(""),
) -> Response:
    if await asyncio.to_thread(service.onboarding_required):
        return RedirectResponse(url="/", status_code=303)
    await asyncio.to_thread(
        service.set_default_creature_thinking_settings,
        model=model,
        reasoning_effort=reasoning_effort,
    )
    if request.headers.get("x-creatureos-ajax") == "1":
        return JSONResponse(
            {
                "ok": True,
                "model": model,
                "reasoning_effort": reasoning_effort,
            }
        )
    return RedirectResponse(url=_index_url(view="settings"), status_code=303)


@app.post("/settings/ecosystem")
async def update_ecosystem(
    request: Request,
    ecosystem_choice: str = Form(""),
) -> Response:
    if await asyncio.to_thread(service.onboarding_required):
        return RedirectResponse(url="/", status_code=303)
    await asyncio.to_thread(service.set_ecosystem, choice=ecosystem_choice)
    if request.headers.get("x-creatureos-ajax") == "1":
        return JSONResponse({"ok": True, "ecosystem_choice": ecosystem_choice})
    return RedirectResponse(url=_index_url(view="settings"), status_code=303)


@app.post("/settings/timezone")
async def update_display_timezone(
    request: Request,
    timezone_choice: str = Form(""),
) -> Response:
    if await asyncio.to_thread(service.onboarding_required):
        return RedirectResponse(url="/", status_code=303)
    await asyncio.to_thread(service.set_display_timezone, choice=timezone_choice)
    if request.headers.get("x-creatureos-ajax") == "1":
        return JSONResponse({"ok": True, "timezone_choice": timezone_choice})
    return RedirectResponse(url=_index_url(view="settings"), status_code=303)


@app.post("/settings/owner-reference")
async def update_owner_reference(
    request: Request,
    owner_reference_choice: str = Form(""),
    owner_reference_custom: str = Form(""),
) -> Response:
    if await asyncio.to_thread(service.onboarding_required):
        return RedirectResponse(url="/", status_code=303)
    await asyncio.to_thread(
        service.set_owner_reference,
        choice=owner_reference_choice,
        custom_value=owner_reference_custom,
    )
    if request.headers.get("x-creatureos-ajax") == "1":
        return JSONResponse(
            {
                "ok": True,
                "owner_reference_choice": owner_reference_choice,
                "owner_reference_custom": owner_reference_custom,
            }
        )
    return RedirectResponse(url=_index_url(view="settings"), status_code=303)


@app.post("/settings/start-fresh")
async def start_fresh() -> RedirectResponse:
    await asyncio.to_thread(service.reset_ecosystem)
    return RedirectResponse(url="/", status_code=303)


@app.post("/onboarding/restart")
async def restart_onboarding() -> RedirectResponse:
    await asyncio.to_thread(service.restart_onboarding)
    return RedirectResponse(url="/", status_code=303)


@app.post("/onboarding/ecosystem")
async def confirm_onboarding_ecosystem(
    ecosystem_choice: str = Form(""),
) -> RedirectResponse:
    await asyncio.to_thread(service.confirm_onboarding_ecosystem, ecosystem_choice=ecosystem_choice)
    return RedirectResponse(url="/", status_code=303)


@app.post("/onboarding/starter")
async def generate_onboarding_starter(request: Request) -> Response:
    try:
        result = await asyncio.to_thread(service.generate_onboarding_starter_creatures)
    except ValueError as exc:
        redirect_url = "/"
        if request.headers.get("x-creatureos-ajax") == "1":
            return JSONResponse({"detail": str(exc), "redirect_url": redirect_url}, status_code=400)
        return RedirectResponse(url=redirect_url, status_code=303)
    if bool(result.get("waiting")):
        redirect_url = _index_url(
            creature=str(result.get("keeper_slug") or "") or None,
            view="chats",
            conversation_id=(
                int(result["welcome_conversation_id"])
                if result.get("welcome_conversation_id") is not None
                else None
            ),
        )
    elif str(result.get("mode") or "") == "onboarding":
        notice = "starter-creatures-creating"
        redirect_url = _index_url(
            creature=str(result.get("keeper_slug") or "") or None,
            view="chats",
            conversation_id=(
                int(result["welcome_conversation_id"])
                if result.get("welcome_conversation_id") is not None
                else None
            ),
            notice=notice,
        )
    else:
        notice = "creatures-creating" if int(result.get("count") or 0) != 1 else "creature-creating"
        redirect_url = _index_url(view="creature", notice=notice)
    if request.headers.get("x-creatureos-ajax") == "1":
        return JSONResponse({"redirect_url": redirect_url})
    return RedirectResponse(url=redirect_url, status_code=303)


@app.post("/onboarding/chat")
async def send_onboarding_chat_message(
    request: Request,
    body: str = Form(""),
    files: list[UploadFile] | None = File(None),
) -> Response:
    attachment_payloads: list[dict[str, object]] = []
    for upload in files or []:
        filename = str(getattr(upload, "filename", "") or "").strip()
        if not filename:
            continue
        content = await upload.read()
        await upload.close()
        attachment_payloads.append(
            {
                "filename": filename,
                "content_type": str(getattr(upload, "content_type", "") or "").strip(),
                "size_bytes": len(content),
                "content": content,
            }
        )
    try:
        response = await asyncio.to_thread(
            service.send_onboarding_chat_message,
            body,
            attachments=attachment_payloads,
        )
    except ValueError as exc:
        if request.headers.get("x-creatureos-ajax") == "1":
            return JSONResponse({"detail": str(exc)}, status_code=400)
        return RedirectResponse(url="/", status_code=303)
    if request.headers.get("x-creatureos-ajax") == "1":
        return JSONResponse(response)
    return RedirectResponse(url="/", status_code=303)


@app.get("/onboarding/feed")
async def onboarding_chat_feed() -> JSONResponse:
    return JSONResponse(await asyncio.to_thread(service.onboarding_chat_feed_state))


@app.get("/onboarding/starter/feed")
async def onboarding_starter_feed() -> JSONResponse:
    return JSONResponse(await asyncio.to_thread(service.onboarding_starter_feed_state))


@app.post("/creatures/{slug}/owner-reference")
async def update_creature_owner_reference(
    slug: str,
    owner_reference_choice: str = Form(""),
    owner_reference_custom: str = Form(""),
    view: str = Form("creature"),
) -> RedirectResponse:
    slug = str(service.canonical_creature_slug(slug))
    await asyncio.to_thread(
        service.set_creature_owner_reference,
        slug,
        choice=owner_reference_choice,
        custom_value=owner_reference_custom,
    )
    return RedirectResponse(url=_index_url(creature=slug, view=view), status_code=303)


@app.post("/creatures/{slug}/thinking")
async def update_creature_thinking(
    request: Request,
    slug: str,
    model_override: str = Form(""),
    reasoning_effort_override: str = Form(""),
    view: str = Form("creature"),
) -> Response:
    slug = str(service.canonical_creature_slug(slug))
    state = await asyncio.to_thread(
        service.set_creature_thinking_settings,
        slug,
        model_override=model_override,
        reasoning_effort_override=reasoning_effort_override,
    )
    if request.headers.get("x-creatureos-ajax") == "1":
        return JSONResponse(state)
    return RedirectResponse(url=_index_url(creature=slug, view=view), status_code=303)


@app.post("/creatures/{slug}/conversations/{conversation_id}/thinking")
@app.post("/creatures/{slug}/chats/{conversation_id}/thinking")
async def update_conversation_thinking(
    request: Request,
    slug: str,
    conversation_id: int,
    model_override: str = Form(""),
    reasoning_effort_override: str = Form(""),
    view: str = Form("chats"),
) -> Response:
    slug = str(service.canonical_creature_slug(slug))
    state = await asyncio.to_thread(
        service.set_conversation_thinking_settings,
        slug,
        conversation_id,
        model_override=model_override,
        reasoning_effort_override=reasoning_effort_override,
    )
    if request.headers.get("x-creatureos-ajax") == "1":
        return JSONResponse(state)
    return RedirectResponse(url=_index_url(creature=slug, view=view, conversation_id=conversation_id), status_code=303)


@app.post("/creatures/{slug}/habits")
async def create_creature_habit(
    slug: str,
    title: str = Form(...),
    instructions: str = Form(...),
    schedule_kind: str = Form("interval"),
    every_minutes: int = Form(30),
    after_minutes: int = Form(120),
    daily_time: str = Form("08:00"),
    times_per_day: int = Form(3),
    window_start: str = Form("06:00"),
    window_end: str = Form("20:00"),
    enabled: str = Form("on"),
    view: str = Form("activity"),
) -> RedirectResponse:
    slug = str(service.canonical_creature_slug(slug))
    habit = await asyncio.to_thread(
        service.create_creature_habit,
        slug,
        title=title,
        instructions=instructions,
        schedule_kind=schedule_kind,
        every_minutes=every_minutes,
        after_minutes=after_minutes,
        daily_time=daily_time,
        times_per_day=times_per_day,
        window_start=window_start,
        window_end=window_end,
        enabled=str(enabled or "").strip().lower() not in {"0", "false", "off", "disabled"},
    )
    habit_id = int(habit.get("id") or 0)
    redirect_url = _index_url(creature=slug, view=view, habit=str(habit_id) if habit_id > 0 else "new")
    if habit_id > 0:
        redirect_url += f"#habit-{habit_id}"
    else:
        redirect_url += "#habit-form"
    return RedirectResponse(url=redirect_url, status_code=303)


@app.post("/creatures/{slug}/habits/{habit_id}/pause")
async def pause_creature_habit(
    slug: str,
    habit_id: int,
    view: str = Form("activity"),
) -> RedirectResponse:
    slug = str(service.canonical_creature_slug(slug))
    await asyncio.to_thread(service.set_creature_habit_enabled, slug, habit_id, enabled=False)
    return RedirectResponse(url=_index_url(creature=slug, view=view, habit=str(habit_id)) + f"#habit-{habit_id}", status_code=303)


@app.post("/creatures/{slug}/habits/{habit_id}/resume")
async def resume_creature_habit(
    slug: str,
    habit_id: int,
    view: str = Form("activity"),
) -> RedirectResponse:
    slug = str(service.canonical_creature_slug(slug))
    await asyncio.to_thread(service.set_creature_habit_enabled, slug, habit_id, enabled=True)
    return RedirectResponse(url=_index_url(creature=slug, view=view, habit=str(habit_id)) + f"#habit-{habit_id}", status_code=303)


@app.post("/creatures/{slug}/habits/{habit_id}/run")
async def run_creature_habit_now(
    slug: str,
    habit_id: int,
    view: str = Form("activity"),
) -> RedirectResponse:
    slug = str(service.canonical_creature_slug(slug))
    await asyncio.to_thread(service.run_creature_habit_now, slug, habit_id)
    return RedirectResponse(url=_index_url(creature=slug, view=view, habit=str(habit_id)) + f"#habit-{habit_id}", status_code=303)


@app.post("/creatures/{slug}/habits/{habit_id}/delete")
async def delete_creature_habit(
    slug: str,
    habit_id: int,
    view: str = Form("activity"),
) -> RedirectResponse:
    slug = str(service.canonical_creature_slug(slug))
    await asyncio.to_thread(service.delete_creature_habit, slug, habit_id)
    return RedirectResponse(url=_index_url(creature=slug, view=view), status_code=303)


@app.post("/creatures/{slug}/rename")
async def rename_creature(
    slug: str,
    display_name: str = Form(...),
    view: str = Form("creature"),
) -> RedirectResponse:
    slug = str(service.canonical_creature_slug(slug))
    await asyncio.to_thread(service.rename_creature_display_name, slug, display_name=display_name)
    return RedirectResponse(url=_index_url(creature=slug, view=view), status_code=303)


@app.post("/creatures/{slug}/delete")
async def delete_creature(slug: str) -> RedirectResponse:
    slug = str(service.canonical_creature_slug(slug))
    try:
        await asyncio.to_thread(service.delete_creature, slug)
    except ValueError as exc:
        return RedirectResponse(url=_index_url(creature=slug, view="creature", notice=str(exc)), status_code=303)
    return RedirectResponse(url="/", status_code=303)


@app.post("/creatures/{slug}/runs/{run_id}/spawn-chat")
@app.post("/creatures/{slug}/runs/{run_id}/spawn-conversation")
async def spawn_conversation_from_run(
    slug: str,
    run_id: int,
    body_override: str = Form(""),
) -> RedirectResponse:
    slug = str(service.canonical_creature_slug(slug))
    conversation = await asyncio.to_thread(
        service.spawn_conversation_from_run,
        slug,
        run_id,
        body_override=body_override,
    )
    return RedirectResponse(
        url=_index_url(creature=slug, view="chats", conversation_id=int(conversation["id"])),
        status_code=303,
    )


@app.post("/creatures/{slug}/messages")
async def post_message(
    request: Request,
    slug: str,
    body: str = Form(""),
    conversation_id: int | None = Form(None),
    draft_chat: str = Form(""),
    view: str = Form("chats"),
    spawn_run_id: int | None = Form(None),
    spawn_body_override: str = Form(""),
    model_override: str = Form(""),
    reasoning_effort_override: str = Form(""),
    busy_action: str = Form("queue"),
    files: list[UploadFile] | None = File(None),
) -> Response:
    slug = str(service.canonical_creature_slug(slug))
    attachment_payloads: list[dict[str, object]] = []
    for upload in files or []:
        filename = str(getattr(upload, "filename", "") or "").strip()
        if not filename:
            continue
        content = await upload.read()
        await upload.close()
        attachment_payloads.append(
            {
                "filename": filename,
                "content_type": str(getattr(upload, "content_type", "") or "").strip(),
                "size_bytes": len(content),
                "content": content,
            }
        )
    if conversation_id is None and spawn_run_id is not None:
        spawned = await asyncio.to_thread(
            service.spawn_conversation_from_run,
            slug,
            int(spawn_run_id),
            body_override=spawn_body_override,
        )
        conversation_id = int(spawned["id"])
    try:
        message_result = await asyncio.to_thread(
            service.send_user_message,
            slug,
            conversation_id,
            body,
            model_override=model_override,
            reasoning_effort_override=reasoning_effort_override,
            attachments=attachment_payloads,
        )
    except ValueError as exc:
        detail = str(exc).strip() or "Could not send that message."
        if request.headers.get("x-creatureos-ajax") == "1":
            return JSONResponse({"detail": detail}, status_code=400)
        return RedirectResponse(
            url=_index_url(
                creature=slug,
                view=view,
                draft_chat=(str(draft_chat or "").strip() == "1"),
                conversation_id=conversation_id,
            ),
            status_code=303,
        )
    conversation_id = int(message_result["conversation_id"])
    run = {
        "run_id": None,
        "sandbox_mode": "read-only",
        "status": str(message_result.get("status") or ""),
        "busy_action": "",
        "run_scope": service.RUN_SCOPE_CHAT,
        "deferred_scope": "",
    }
    if str(message_result.get("status") or "") != "waiting":
        run = await asyncio.to_thread(
            service.start_background_run,
            slug,
            trigger_type="user_reply",
            force_message=True,
            conversation_id=conversation_id,
            allow_code_changes=True,
            run_scope=service.RUN_SCOPE_CHAT,
            busy_action=busy_action,
        )
    redirect_url = _index_url(creature=slug, view=view, conversation_id=conversation_id)
    if request.headers.get("x-creatureos-ajax") == "1":
        return JSONResponse(
            {
                "redirect_url": redirect_url,
                "conversation_id": conversation_id,
                "run_id": int(run["run_id"]) if run.get("run_id") is not None else None,
                "sandbox_mode": str(run.get("sandbox_mode") or "workspace-write"),
                "stream_url": (
                    f"/creatures/{slug}/runs/{int(run['run_id'])}/stream"
                    if run.get("run_id") is not None
                    else ""
                ),
                "status": str(run.get("status") or "running"),
                "busy_action": str(run.get("busy_action") or ""),
                "run_scope": str(run.get("run_scope") or ""),
                "deferred_scope": str(run.get("deferred_scope") or ""),
                "waiting_message": str(message_result.get("waiting_message") or run.get("waiting_message") or ""),
            }
        )
    return RedirectResponse(url=redirect_url, status_code=303)


@app.post("/creatures/{slug}/summon")
async def summon_creature_from_keeper(
    request: Request,
    slug: str,
    conversation_id: int | None = Form(None),
    view: str = Form("creature"),
) -> Response:
    slug = str(service.canonical_creature_slug(slug))
    try:
        summon_result = await asyncio.to_thread(
            service.summon_creature_from_keeper,
            slug,
            conversation_id,
        )
    except ValueError as exc:
        detail = str(exc).strip() or "Could not summon a creature right now."
        if request.headers.get("x-creatureos-ajax") == "1":
            return JSONResponse({"detail": detail}, status_code=400)
        return RedirectResponse(
            url=_index_url(creature=slug, view=view, conversation_id=conversation_id),
            status_code=303,
        )
    conversation_id = int(summon_result["conversation_id"])
    redirect_url = _index_url(creature=slug, view=view, conversation_id=conversation_id)
    if request.headers.get("x-creatureos-ajax") == "1":
        return JSONResponse(
            {
                "redirect_url": redirect_url,
                "status": str(summon_result.get("status") or "summoned"),
                "conversation_id": conversation_id,
                "assistant_body": str(summon_result.get("assistant_body") or ""),
                "created_creatures": list(summon_result.get("created_creatures") or []),
                "waiting_message": str(summon_result.get("waiting_message") or ""),
            }
        )
    return RedirectResponse(url=redirect_url, status_code=303)


@app.get("/messages/{message_id}/attachments/{attachment_id}")
async def message_attachment(message_id: int, attachment_id: str) -> FileResponse:
    payload = await asyncio.to_thread(service.get_message_attachment, message_id, attachment_id)
    return FileResponse(
        path=payload["disk_path"],
        media_type=payload["content_type"] or None,
        filename=payload["filename"],
    )

@app.get("/creatures/{slug}/runs/{run_id}/stream")
async def stream_run_feed(request: Request, slug: str, run_id: int) -> StreamingResponse:
    slug = str(service.canonical_creature_slug(slug))

    async def event_source():
        raw_last_event_id = request.headers.get("last-event-id") or request.query_params.get("last_event_id") or "0"
        try:
            after_id = max(0, int(raw_last_event_id))
        except ValueError:
            after_id = 0
        while True:
            snapshot = await asyncio.to_thread(service.list_run_events, slug, run_id, after_id=after_id)
            run = snapshot["run"]
            events = snapshot["events"]
            for item in events:
                after_id = int(item["id"])
                payload = {
                    "id": int(item["id"]),
                    "type": str(item["event_type"]),
                    "body": str(item["body"] or ""),
                    "display_body": str(item.get("display_body") or ""),
                    "created_at": str(item["created_at"] or ""),
                    "metadata": item.get("metadata") or {},
                    "run_status": str(run["status"] or ""),
                }
                yield f"id: {after_id}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"
            if str(run["status"] or "") != "running":
                break
            if await request.is_disconnected():
                break
            await asyncio.sleep(0.35)

    return StreamingResponse(
        event_source(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/healthz")
async def healthz() -> Response:
    try:
        snapshot = await asyncio.to_thread(service.health_snapshot)
    except Exception as exc:
        return JSONResponse(
            {
                "status": "error",
                "app": "creature-os",
                "error": str(exc),
            },
            status_code=503,
        )
    snapshot["version"] = STATIC_VERSION
    snapshot["source_revision"] = SERVER_SOURCE_REVISION
    snapshot["booted_at"] = SERVER_BOOTED_AT
    snapshot["worker_pid"] = os.getpid()
    snapshot["supervisor_pid"] = SERVER_SUPERVISOR_PID
    return JSONResponse(snapshot)
