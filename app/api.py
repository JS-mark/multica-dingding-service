"""REST API 路由。

- GET  /health                     健康检查
- GET  /api/info                   运行配置摘要
- POST /api/chat                   发消息并 SSE 流式返回（支持 session 上下文）
- GET  /api/sessions               会话列表
- GET  /api/sessions/{key}/messages 最近消息
- POST /api/sessions/{key}/reset   重置会话（下一条消息新建 issue）
- GET  /api/workspaces             workspace 列表
- GET  /api/agents                 agent 列表（?workspace_id= 指定工作区，支持别名）
"""

from __future__ import annotations

import json
from typing import Optional

from fastapi import APIRouter, Header, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from .conversation import ConversationManager
from .multica_client import MulticaClient
from .settings import Settings

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
    return {
        "status": "ok",
        "dingtalk": {
            "enabled": settings.dingtalk.enabled,
            "connected": bool(bot and bot.connected),
        },
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
