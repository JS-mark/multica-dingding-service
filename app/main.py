"""服务入口：FastAPI 应用 + 钉钉 Stream 客户端生命周期管理。"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager

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


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings.load()
    setup_logging(settings.log_level)

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
