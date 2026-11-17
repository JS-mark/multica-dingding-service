"""REST API 路由。

- GET  /health                     健康检查
- GET  /api/info                   运行配置摘要
- POST /api/chat                   发消息并 SSE 流式返回（支持 session 上下文）
- GET  /api/sessions               会话列表
- GET  /api/sessions/{key}/messages 最近消息
- POST /api/sessions/{key}/reset   重置会话（下一条消息新建 issue）
- GET  /api/agents                 workspace agent 列表（便于查 agent_id）
"""

from __future__ import annotations

import json
from typing import Optional

from fastapi import APIRouter, Depends, Header, HTTPException, Request
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
    return {
        "multica": {
            "profile": settings.multica.profile,
            "server_url": settings.multica.server_url or "(profile 默认)",
            "workspace_id": settings.multica.workspace_id,
            "agent_id": settings.multica.agent_id,
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
    if not settings.multica.agent_id:
        raise HTTPException(status_code=503, detail="未配置 multica.agent_id")

    session_key = f"api:{body.session_id or 'default'}"

    async def event_stream():
        try:
            async for event in manager.converse(
                session_key,
                "api",
                body.user or "api-user",
                body.user or "api-user",
                body.message,
                force_new=body.new_session,
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


@router.get("/api/agents")
async def agents(request: Request, x_api_key: Optional[str] = Header(default=None)):
    settings, _, client = _get_deps(request)
    _auth(settings, x_api_key)
    try:
        rows = await client.list_agents()
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=502, detail=f"multica 调用失败: {e}") from e
    return [
        {
            "id": a.get("id"),
            "name": a.get("name"),
            "model": a.get("model"),
            "status": a.get("status"),
            "runtime_mode": a.get("runtime_mode"),
        }
        for a in rows
        if not a.get("archived_at")
    ]
