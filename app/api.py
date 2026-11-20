"""REST API 路由。

- GET  /health                     健康检查
- GET  /api/info                   运行配置摘要
- POST /api/chat                   发消息并 SSE 流式返回（支持 session 上下文）
- GET  /api/sessions               会话列表
- GET  /api/sessions/{key}/messages 最近消息
- POST /api/sessions/{key}/reset   重置会话（下一条消息新建 issue）
- GET  /api/workspaces             workspace 列表
- GET  /api/agents                 agent 列表（?workspace_id= 指定工作区，支持别名）

管理接口（X-Admin-Token 鉴权，token 启动时生成于 data/admin_token，或配 api.admin_token）：
- GET    /api/admin/bots           机器人列表（含连接状态）
- PUT    /api/admin/bots/{slug}    新增/更新机器人（热生效，写 data/bots.yaml）
- DELETE /api/admin/bots/{slug}    移除机器人（热生效）
"""

from __future__ import annotations

import json
import secrets
from typing import List, Optional

from fastapi import APIRouter, Header, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from .conversation import ConversationManager
from .dingtalk_bot import DingTalkBot
from .multica_client import MulticaClient
from .settings import Settings, build_bot_cfg, load_bots_file, save_bots_file

router = APIRouter()


def _auth(settings: Settings, x_api_key: Optional[str]) -> None:
    if settings.api.api_key and x_api_key != settings.api.api_key:
        raise HTTPException(status_code=401, detail="invalid api key")


class ChatRequest(BaseModel):
    message: str = Field(..., min_length=1, description="用户消息")
    session_id: Optional[str] = Field(
        default="default", description="会话 ID，同一 ID 内保持上下文"
    )
    user: Optional[str] = Field(default="api-user", description="调用方标识（仅记录用）")
    new_session: bool = Field(default=False, description="为该 session 新开一个 issue")
    workspace_id: Optional[str] = Field(
        default=None,
        description="目标工作区（别名或 UUID）；指定后本会话路由到它，随消息新建 issue",
    )
    agent_id: Optional[str] = Field(
        default=None,
        description="目标 agent（别名或 UUID，须属于目标工作区）；指定后本会话路由到它",
    )


def _get_deps(request: Request):
    settings: Settings = request.app.state.settings
    manager: ConversationManager = request.app.state.manager
    client: MulticaClient = request.app.state.multica
    return settings, manager, client


@router.get("/health")
async def health(request: Request):
    settings: Settings = request.app.state.settings
    bot = getattr(request.app.state, "dingtalk_bot", None)
    bots: dict = getattr(request.app.state, "dingtalk_bots", {}) or {}
    return {
        "status": "ok",
        "dingtalk": {  # 默认机器人（兼容已有监控）
            "enabled": settings.dingtalk.enabled,
            "connected": bool(bot and bot.connected),
        },
        "bots": {slug: {"connected": b.connected} for slug, b in bots.items()},
        "agent_id": settings.multica.agent_id,
    }


@router.get("/api/info")
async def info(request: Request, x_api_key: Optional[str] = Header(default=None)):
    settings, _, _ = _get_deps(request)
    _auth(settings, x_api_key)
    d = settings.dingtalk
    m = settings.multica
    return {
        "multica": {
            "profile": m.profile,
            "server_url": m.server_url or "(profile 默认)",
            "workspace_id": m.workspace_id,
            "agent_id": m.agent_id,
            "workspace_aliases": m.workspaces,
            "agent_aliases": m.agents,
        },
        "dingtalk": {
            "enabled": d.enabled,
            "card_template": d.card_template_id or "SDK 内置默认模板",
            "whitelist_size": len(d.allowed_staff_ids) + len(d.allowed_user_ids) + len(d.allowed_nicks),
        },
        "bots": {
            slug: {
                "whitelist_size": (
                    len(b.settings.allowed_staff_ids)
                    + len(b.settings.allowed_user_ids)
                    + len(b.settings.allowed_nicks)
                ),
                "workspace_id": b.manager.settings.workspace_id,
                "agent_id": b.manager.settings.agent_id,
            }
            for slug, b in (getattr(request.app.state, "dingtalk_bots", {}) or {}).items()
        },
    }


@router.post("/api/chat")
async def chat(
    body: ChatRequest,
    request: Request,
    x_api_key: Optional[str] = Header(default=None),
):
    settings, manager, _ = _get_deps(request)
    _auth(settings, x_api_key)

    session_key = f"api:{body.session_id or 'default'}"
    m = settings.multica
    stored = manager.store.get_session(session_key) or {}
    if not (body.agent_id or m.agent_id or stored.get("agent_id")):
        raise HTTPException(
            status_code=400,
            detail="未指定 agent：body 带 agent_id（GET /api/agents 查询，支持别名）"
                   "或配置 multica.agent_id",
        )
    # 别名 -> UUID（未知别名按原串传给 CLI，由 converse 的校验兜底）
    ws_param = m.resolve_workspace(body.workspace_id) if body.workspace_id else None
    agent_param = m.resolve_agent(body.agent_id) if body.agent_id else None

    async def event_stream():
        try:
            async for event in manager.converse(
                session_key,
                "api",
                body.user or "api-user",
                body.user or "api-user",
                body.message,
                force_new=body.new_session,
                workspace_id=ws_param,
                agent_id=agent_param,
            ):
                yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
        except Exception as e:  # noqa: BLE001
            yield f"data: {json.dumps({'type': 'error', 'message': str(e)}, ensure_ascii=False)}\n\n"

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.get("/api/sessions")
async def sessions(request: Request, x_api_key: Optional[str] = Header(default=None)):
    settings, manager, _ = _get_deps(request)
    _auth(settings, x_api_key)
    return manager.store.list_sessions()


@router.get("/api/sessions/{key}/messages")
async def session_messages(
    key: str, request: Request, x_api_key: Optional[str] = Header(default=None)
):
    settings, manager, _ = _get_deps(request)
    _auth(settings, x_api_key)
    if not manager.store.get_session(key):
        raise HTTPException(status_code=404, detail="session not found")
    return manager.store.recent_messages(key)


@router.post("/api/sessions/{key}/reset")
async def session_reset(
    key: str, request: Request, x_api_key: Optional[str] = Header(default=None)
):
    settings, manager, _ = _get_deps(request)
    _auth(settings, x_api_key)
    ok = await manager.reset_session(key)
    if not ok:
        raise HTTPException(status_code=404, detail="session not found")
    return {"ok": True}


@router.get("/api/workspaces")
async def workspaces(request: Request, x_api_key: Optional[str] = Header(default=None)):
    settings, _, client = _get_deps(request)
    _auth(settings, x_api_key)
    try:
        rows = await client.list_workspaces()
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=502, detail=f"multica 调用失败: {e}") from e
    m = settings.multica
    alias_by_id = {wid: alias for alias, wid in m.workspaces.items()}
    return [
        {
            "id": w.get("id"),
            "name": w.get("name"),
            "alias": alias_by_id.get(w.get("id")),
            "is_default": w.get("id") == m.workspace_id,
        }
        for w in rows
    ]


@router.get("/api/agents")
async def agents(
    request: Request,
    workspace_id: Optional[str] = Query(
        default=None, description="工作区别名或 UUID；留空用默认工作区"
    ),
    x_api_key: Optional[str] = Header(default=None),
):
    settings, _, client = _get_deps(request)
    _auth(settings, x_api_key)
    m = settings.multica
    try:
        rows = await client.list_agents(
            workspace_id=m.resolve_workspace(workspace_id) if workspace_id else None
        )
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=502, detail=f"multica 调用失败: {e}") from e
    alias_by_id = {aid: alias for alias, aid in m.agents.items()}
    return [
        {
            "id": a.get("id"),
            "name": a.get("name"),
            "alias": alias_by_id.get(a.get("id")),
            "model": a.get("model"),
            "status": a.get("status"),
            "runtime_mode": a.get("runtime_mode"),
        }
        for a in rows
        if not a.get("archived_at")
    ]


# ----------------------------------------------------------------------
# 管理接口：运行时增删机器人（写 data/bots.yaml，热生效；default 机器人仍由
# config.yaml 管理）。鉴权用 X-Admin-Token，token 见启动日志 / data/admin_token。
def _admin_auth(request: Request, x_admin_token: Optional[str]) -> None:
    token = getattr(request.app.state, "admin_token", "")
    if not token or not x_admin_token or not secrets.compare_digest(token, x_admin_token):
        raise HTTPException(status_code=401, detail="invalid admin token")


class BotConfigIn(BaseModel):
    client_id: str = Field(..., min_length=1, description="钉钉应用 AppKey")
    client_secret: str = Field(..., min_length=1, description="钉钉应用 AppSecret")
    card_template_id: str = ""
    card_title: str = ""
    show_process: bool = False
    stream_update_interval: float = 1.2
    allowed_staff_ids: List[str] = Field(default_factory=list)
    allowed_user_ids: List[str] = Field(default_factory=list)
    allowed_nicks: List[str] = Field(default_factory=list)
    multica: Optional[dict] = Field(
        default=None,
        description="本机器人独立的默认路由（workspace_id/agent_id/workspaces/agents）",
    )


@router.get("/api/admin/bots")
async def admin_list_bots(
    request: Request, x_admin_token: Optional[str] = Header(default=None)
):
    _admin_auth(request, x_admin_token)
    runtime = load_bots_file(request.app.state.bots_path)
    out = []
    for slug, b in (getattr(request.app.state, "dingtalk_bots", {}) or {}).items():
        s, ms = b.settings, b.manager.settings
        out.append(
            {
                "slug": slug,
                "client_id": s.client_id,
                "card_title": s.card_title,
                "connected": b.connected,
                "whitelist_size": (
                    len(s.allowed_staff_ids)
                    + len(s.allowed_user_ids)
                    + len(s.allowed_nicks)
                ),
                "workspace_id": ms.workspace_id,
                "agent_id": ms.agent_id,
                "runtime_managed": slug in runtime,  # True=可经管理接口改，False=config.yaml
            }
        )
    return out


@router.put("/api/admin/bots/{slug}")
async def admin_put_bot(
    slug: str,
    body: BotConfigIn,
    request: Request,
    x_admin_token: Optional[str] = Header(default=None),
):
    """新增/更新机器人：校验 -> 持久化 data/bots.yaml -> 热启动（无需重启容器）。"""
    _admin_auth(request, x_admin_token)
    if slug == "default":
        raise HTTPException(
            status_code=400,
            detail="default 机器人由 config.yaml 管理，修改后需 docker compose restart",
        )
    cfg = body.model_dump(exclude_none=True)
    try:
        dt, ms = build_bot_cfg(slug, cfg, request.app.state.settings.multica)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    for other, b in (request.app.state.dingtalk_bots or {}).items():
        if other != slug and b.settings.client_id == dt.client_id:
            raise HTTPException(
                status_code=400, detail=f"client_id 与机器人 {other!r} 重复，会互相抢消息"
            )
    bots = load_bots_file(request.app.state.bots_path)
    bots[slug] = cfg
    save_bots_file(request.app.state.bots_path, bots)

    manager = ConversationManager(
        request.app.state.multica,
        request.app.state.store,
        ms,
        shared_sem=request.app.state.shared_sem,
    )
    await request.app.state.stop_bot(slug)  # 已存在则先停（更新场景），其他机器人不受影响
    request.app.state.start_bot(slug, DingTalkBot(dt, manager, slug=slug))
    return {"ok": True, "slug": slug, "started": True}


@router.delete("/api/admin/bots/{slug}")
async def admin_delete_bot(
    slug: str,
    request: Request,
    x_admin_token: Optional[str] = Header(default=None),
):
    """移除机器人（热生效）：删 data/bots.yaml 条目并停掉对应 Stream 任务。"""
    _admin_auth(request, x_admin_token)
    if slug == "default":
        raise HTTPException(
            status_code=400, detail="default 机器人由 config.yaml 管理，不能经此接口删除"
        )
    bots = load_bots_file(request.app.state.bots_path)
    online = request.app.state.dingtalk_bots or {}
    if slug not in bots and slug not in online:
        raise HTTPException(status_code=404, detail=f"机器人 {slug!r} 不存在")
    if slug in bots:
        del bots[slug]
        save_bots_file(request.app.state.bots_path, bots)
    await request.app.state.stop_bot(slug)
    return {"ok": True, "slug": slug, "stopped": True}
