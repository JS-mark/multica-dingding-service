"""服务入口：FastAPI 应用 + 钉钉 Stream 客户端生命周期管理。"""

from __future__ import annotations

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import urlparse

from fastapi import FastAPI

from . import api
from .conversation import ConversationManager
from .dingtalk_bot import DingTalkBot
from .logging_setup import setup_logging
from .multica_client import MulticaClient
from .settings import Settings
from .store import SessionStore

logger = logging.getLogger("multica_dingding")


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings: Settings = app.state.settings
    bot: DingTalkBot = app.state.dingtalk_bot
    dtask = None
    if settings.dingtalk.enabled:
        if not settings.dingtalk.client_id or not settings.dingtalk.client_secret:
            logger.warning("dingtalk.enabled=true 但缺少 client_id/client_secret，跳过启动")
        else:
            if (
                not settings.dingtalk.allowed_staff_ids
                and not settings.dingtalk.allowed_user_ids
                and not settings.dingtalk.allowed_nicks
            ):
                logger.warning("钉钉白名单为空：机器人将拒绝所有人（请在配置中填写 allowed_staff_ids）")
            dtask = asyncio.create_task(_run_dingtalk(bot))
    if not settings.multica.agent_id:
        logger.warning("未配置 multica.agent_id：/api/chat 与钉钉对话不可用")
    yield
    if dtask:
        dtask.cancel()
        try:
            await dtask
        except (asyncio.CancelledError, Exception):  # noqa: BLE001
            pass
    await bot.aclose()


async def _run_dingtalk(bot: DingTalkBot) -> None:
    retry = 0
    while True:
        try:
            await bot.start()
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            retry += 1
            wait = min(30, 2 ** min(retry, 5))
            logger.exception("钉钉 Stream 断开，%ss 后重连（第 %d 次）", wait, retry)
            await asyncio.sleep(wait)
        else:
            retry = 0
            await asyncio.sleep(1)


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
    manager = ConversationManager(app.state.multica, store, settings.multica)
    app.state.manager = manager
    app.state.dingtalk_bot = DingTalkBot(settings.dingtalk, manager)
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
