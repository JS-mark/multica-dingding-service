"""服务入口：FastAPI 应用 + 钉钉 Stream 客户端生命周期管理。"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import secrets
import threading
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import urlparse

from fastapi import FastAPI

from . import api
from .conversation import ConversationManager
from .dingtalk_bot import DingTalkBot
from .logging_setup import setup_logging
from .multica_client import MulticaClient
from .settings import (
    Settings,
    bots_file_path,
    iter_bots,
    load_bots_file,
)
from .store import SessionStore

logger = logging.getLogger("multica_dingding")


def _ensure_admin_token(settings: Settings) -> str:
    """管理接口 token：api.admin_token 显式配置优先；否则生成一次并持久化到
    data/admin_token（重启不变），仅首次生成时打印到日志。"""
    if settings.api.admin_token:
        return settings.api.admin_token
    path = Path(settings.session.db_path).parent / "admin_token"
    token = secrets.token_urlsafe(24)
    try:
        if path.exists():
            saved = path.read_text(encoding="utf-8").strip()
            if saved:
                logger.debug("管理接口 token 已存在: %s", path)
                return saved
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(token, encoding="utf-8")
        os.chmod(path, 0o600)
    except OSError as e:
        logger.warning("管理 token 持久化失败（仅本次运行有效）: %s", e)
    logger.info("已生成管理接口 token（保存在 %s，用 X-Admin-Token 头调用 /api/admin/*）: %s", path, token)
    return token


def start_bot_task(app: FastAPI, slug: str, bot: DingTalkBot) -> None:
    """启动（或替换）一个机器人：独立线程跑 SDK 客户端（与主 loop 隔离，
    避免其内部同步 requests 在凭证/网络异常时拖垮服务）。"""
    app.state.dingtalk_bots[slug] = bot
    bot.loop = asyncio.get_running_loop()  # handler 经它把异步处理投递回主 loop
    t = threading.Thread(target=bot.run_forever, name=f"dingtalk-{slug}", daemon=True)
    app.state.bot_tasks[slug] = t
    t.start()


async def stop_bot_task(app: FastAPI, slug: str) -> None:
    """停掉一个机器人：停 SDK 线程 + 释放 HTTP 客户端；不影响其他机器人。"""
    thread = app.state.bot_tasks.pop(slug, None)
    bot = app.state.dingtalk_bots.pop(slug, None)
    if bot:
        bot.shutdown()
    if thread:
        await asyncio.to_thread(thread.join, 10)  # join 放线程池，不阻塞主 loop
        if thread.is_alive():
            logger.warning("机器人[%s] 线程 10s 内未退出，放弃等待", slug)
    if bot:
        await bot.aclose()


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings: Settings = app.state.settings
    bots: dict = app.state.dingtalk_bots
    for slug, bot in list(bots.items()):
        d = bot.settings
        if not (d.allowed_staff_ids or d.allowed_user_ids or d.allowed_nicks):
            logger.warning("机器人[%s]：白名单为空，将拒绝所有人（请填写 allowed_staff_ids）", slug)
        start_bot_task(app, slug, bot)
    if bots:
        logger.info("钉钉机器人已启动: %s", ", ".join(bots))
    if not settings.multica.agent_id:
        logger.warning(
            "未配置 multica.agent_id 默认值：会话需先 /agent 选择（或 REST 请求带 agent_id）"
        )
    yield
    for slug in list(app.state.bot_tasks):
        await stop_bot_task(app, slug)


def _bootstrap_multica_auth(settings: Settings) -> None:
    """把 multica.token 写入 CLI 认证文件 ~/.multica/profiles/<profile>/config.json。

    用于无法从宿主机挂载 multica-home 目录的 Docker 运行时（named volume 模式）：
    首次启动时经环境变量注入 token，之后认证随卷持久化，token 可从环境移除。
    认证文件已存在（如挂载进来的 multica-home）则不覆盖。
    """
    m = settings.multica
    if not m.token:
        return
    profile = m.profile or (
        urlparse(m.server_url).hostname if m.server_url else "desktop-api.multica.ai"
    )
    cfg = Path.home() / ".multica" / "profiles" / profile / "config.json"
    if cfg.exists():
        if not m.profile:
            m.profile = profile
        return
    try:
        cfg.parent.mkdir(parents=True, exist_ok=True)
        cfg.write_text(
            json.dumps(
                {"server_url": m.server_url or "https://api.multica.ai", "token": m.token}
            ),
            encoding="utf-8",
        )
    except OSError as e:
        raise RuntimeError(
            f"无法写入 multica 认证文件 {cfg}（{e}）。"
            "请检查 multica-home 挂载是否可写（属主需为运行用户 uid 10001）"
        ) from e
    if not m.profile:
        m.profile = profile
    logger.info("已根据 multica.token 初始化认证文件: %s", cfg)


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings.load()
    setup_logging(settings.log_level)
    _bootstrap_multica_auth(settings)

    app = FastAPI(title="multica-dingding-service", version="1.0.0", lifespan=lifespan)
    app.state.settings = settings
    app.state.multica = MulticaClient(
        binary=settings.multica.binary,
        profile=settings.multica.profile,
        workspace_id=settings.multica.workspace_id,
        server_url=settings.multica.server_url,
    )
    store = SessionStore(settings.session.db_path)
    # 全局并发预算：所有机器人（各持独立 manager）共享同一把信号量
    shared_sem = asyncio.Semaphore(max(1, settings.multica.max_concurrent_rounds))

    # 运行时机器人配置（data/bots.yaml，管理接口维护）叠加到 config.yaml 的 dingtalk_bots 上
    bots_path = bots_file_path(settings.session.db_path)
    runtime_bots = load_bots_file(bots_path)
    if runtime_bots:
        settings.dingtalk_bots = {**settings.dingtalk_bots, **runtime_bots}

    bots: dict = {}
    for slug, dt_settings, ms in iter_bots(settings):
        manager = ConversationManager(app.state.multica, store, ms, shared_sem=shared_sem)
        bots[slug] = DingTalkBot(dt_settings, manager, slug=slug)
    app.state.dingtalk_bots = bots
    app.state.bot_tasks = {}
    # 管理接口用的动态启停句柄（避免 api -> main 循环导入）
    app.state.start_bot = lambda slug, bot: start_bot_task(app, slug, bot)
    app.state.stop_bot = lambda slug: stop_bot_task(app, slug)
    app.state.store = store
    app.state.shared_sem = shared_sem
    app.state.bots_path = bots_path
    app.state.admin_token = _ensure_admin_token(settings)

    # 兼容：REST 与 /health 走 default bot 的 manager；无 default bot 时兜底全局配置
    app.state.manager = bots["default"].manager if "default" in bots else ConversationManager(
        app.state.multica, store, settings.multica, shared_sem=shared_sem
    )
    app.state.dingtalk_bot = bots.get("default")
    app.include_router(api.router)
    return app


app = create_app()


def main() -> None:
    import uvicorn

    settings = app.state.settings
    uvicorn.run(
        "app.main:app",
        host=settings.api.host,
        port=settings.api.port,
        log_config=None,
    )


if __name__ == "__main__":
    main()
