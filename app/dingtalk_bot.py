"""钉钉机器人：Stream 模式接收消息 + AI 卡片流式打字机回复。

卡片层使用 app.dingtalk_card 的全异步实现（所有钉钉 API 调用带超时），
避免 SDK 同步无超时调用在网络挂起时冻结回复管线。
卡片默认使用 SDK 内置通用 AI Markdown 模板，可在 config 覆盖模板 ID。
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from pathlib import Path
from typing import List, Optional, Tuple

import dingtalk_stream
import httpx

from .conversation import ConversationManager
from .dingtalk_card import (
    AiCardSession,
    DEFAULT_AI_MARKDOWN_TEMPLATE,
    DingTalkAsyncClient,
)
from .settings import DingTalkSettings

logger = logging.getLogger("multica_dingding.dingtalk")


class DingTalkBot:
    def __init__(self, settings: DingTalkSettings, manager: ConversationManager):
        self.settings = settings
        self.manager = manager
        self.client: Optional[dingtalk_stream.DingTalkStreamClient] = None
        self.connected = False
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self.dtk = DingTalkAsyncClient(settings.client_id, settings.client_secret)

    async def aclose(self) -> None:
        try:
            await self.dtk.aclose()
        except Exception:  # noqa: BLE001
            pass

    # ------------------------------------------------------------------
    def is_allowed(self, incoming_message: dingtalk_stream.ChatbotMessage) -> bool:
        """固定人员权限：白名单为空 = 拒绝所有人（安全默认）。

        staff ↔ payload senderStaffId；user ↔ senderId（钉钉加密用户 id）。
        """
        s = self.settings
        if not (s.allowed_staff_ids or s.allowed_user_ids or s.allowed_nicks):
            return False
        staff = (incoming_message.sender_staff_id or "").strip()
        user = (incoming_message.sender_id or "").strip()
        nick = (incoming_message.sender_nick or "").strip()
        if staff and staff in s.allowed_staff_ids:
            return True
        if user and user in s.allowed_user_ids:
            return True
        if nick and nick in s.allowed_nicks:
            return True
        return False

    async def start(self) -> None:
        """启动 WebSocket 长连接（阻塞运行，由 lifespan 以 task 方式拉起）。"""
        self.loop = asyncio.get_running_loop()
        credential = dingtalk_stream.Credential(
            self.settings.client_id, self.settings.client_secret
        )
        self.client = dingtalk_stream.DingTalkStreamClient(credential)
        self.client.register_callback_handler(
            dingtalk_stream.chatbot.ChatbotMessage.TOPIC, _BotHandler(self)
        )
        self.connected = True
        logger.info("钉钉 Stream 客户端已启动")
        try:
            await self.client.start()
        finally:
            self.connected = False


class _BotHandler(dingtalk_stream.AsyncChatbotHandler):
    """注意：AsyncChatbotHandler.process 必须是【同步】方法（SDK 在线程池中调用），
    异步处理通过 run_coroutine_threadsafe 转发到主 event loop。"""

    def __init__(self, bot: DingTalkBot):
        super().__init__()
        self.bot = bot

    def process(self, callback: dingtalk_stream.CallbackMessage):
        incoming_message = dingtalk_stream.ChatbotMessage.from_dict(callback.data)
        text = self._extract_text(incoming_message)

        # 消息信封诊断日志（排查群聊问题用）
        at_users = []
        try:
            at_users = [getattr(u, "staff_id", None) or getattr(u, "dingtalk_id", None)
                        for u in (incoming_message.at_users or [])]
        except Exception:  # noqa: BLE001
            pass
        logger.info(
            "收到消息: type=%s conv_type=%s conv_id=%s staff=%s nick=%s atUsers=%s text=%r",
            incoming_message.message_type,
            incoming_message.conversation_type,
            (incoming_message.conversation_id or "")[:16],
            incoming_message.sender_staff_id,
            incoming_message.sender_nick,
            at_users,
            text[:50],
        )

        # 固定人员权限校验
        if not self.bot.is_allowed(incoming_message):
            logger.warning(
                "拒绝未授权用户: staff=%s senderId=%s nick=%s",
                incoming_message.sender_staff_id,
                incoming_message.sender_id,
                incoming_message.sender_nick,
            )
            return

        if not text:
            logger.warning("消息无可提取文字（type=%s），忽略", incoming_message.message_type)
            return

        # 每条消息独立协程处理，不阻塞回调线程
        asyncio.run_coroutine_threadsafe(
            self._handle(incoming_message, text), self.bot.loop
        )

    # ------------------------------------------------------------------
    @staticmethod
    def _extract_text(msg: dingtalk_stream.ChatbotMessage) -> str:
        """纯文本 / 富文本（图文混合）/ 纯图片消息统一提取文字部分。

        群聊 @机器人 时文本常带 "@机器人名 " 前缀，去掉（仅当消息确实带 atUsers）。
        """
        try:
            parts = [t for t in (msg.get_text_list() or []) if t and t.strip()]
        except Exception:  # noqa: BLE001
            parts = []
        text = "\n".join(parts).strip()
        try:
            if text.startswith("@") and (msg.at_users or []):
                tokens = text.split(None, 1)
                text = tokens[1] if len(tokens) > 1 else ""
        except Exception:  # noqa: BLE001
            pass
        return text.strip()

    async def _download_images(
        self, msg: dingtalk_stream.ChatbotMessage
    ) -> Tuple[List[str], List[str]]:
        """下载消息里的图片到临时目录。返回 (文件路径列表, 失败说明列表)。"""
        codes: List[str] = []
        try:
            codes = msg.get_image_list() or []
        except Exception:  # noqa: BLE001
            codes = []
        if not codes:
            return [], []

        tmp_dir = Path("/tmp") / f"mds-attach-{uuid.uuid1().hex[:12]}"
        tmp_dir.mkdir(parents=True, exist_ok=True)
        paths: List[str] = []
        errors: List[str] = []
        for i, code in enumerate(codes):
            try:
                url = await self.bot.dtk.get_image_download_url(code)
                if not url:
                    raise RuntimeError("获取下载链接失败")
                async with httpx.AsyncClient(timeout=30, follow_redirects=True) as hc:
                    resp = await hc.get(url)
                    resp.raise_for_status()
                ext = ".png"
                ctype = resp.headers.get("content-type", "")
                if "jpeg" in ctype or "jpg" in ctype:
                    ext = ".jpg"
                elif "gif" in ctype:
                    ext = ".gif"
                elif "webp" in ctype:
                    ext = ".webp"
                p = tmp_dir / f"image-{i + 1}{ext}"
                p.write_bytes(resp.content)
                paths.append(str(p))
            except Exception as e:  # noqa: BLE001
                logger.warning("图片下载失败(%d/%d): %s", i + 1, len(codes), e)
                errors.append(f"图片{i + 1}下载失败")
        if not paths:
            # 一张都没下成：不留临时目录
            import shutil

            shutil.rmtree(tmp_dir, ignore_errors=True)
        return paths, errors

    # ------------------------------------------------------------------
    async def _handle(
        self, incoming_message: dingtalk_stream.ChatbotMessage, text: str
    ) -> None:
        settings = self.bot.settings
        manager = self.bot.manager
        uid = incoming_message.sender_staff_id or incoming_message.sender_id
        session_key = f"dingtalk:{uid}"
        user_name = incoming_message.sender_nick or uid
        is_group = incoming_message.conversation_type == "2"
        title = settings.card_title or "Multica Agent"

        # 会话内命令
        low = text.lower()
        if low in ("/new", "/reset", "新会话"):
            await manager.reset_session(session_key)
            await self._reply(incoming_message, title, "✅ 已开启新会话，发送下一条消息开始。")
            return
        if low in ("/help", "帮助"):
            await self._reply(
                incoming_message,
                title,
                "**可用命令**\n- 直接发消息与我对话（支持图片/图文混合，图片会作为附件转给 agent）\n- `/new` 开始新会话（清空上下文）\n- `/issue` 查看当前会话绑定的 issue",
            )
            return
        if low == "/issue":
            s = manager.get_session(session_key)
            issue_key = (s or {}).get("issue_key") or "无"
            await self._reply(incoming_message, title, f"当前会话 issue：`{issue_key}`")
            return

        # 创建 AI 卡片（占位）。卡片 API 全部带超时；失败降级为普通消息回复。
        card: Optional[AiCardSession] = None
        try:
            candidate = AiCardSession(
                self.bot.dtk,
                incoming_message,
                template_id=settings.card_template_id or DEFAULT_AI_MARKDOWN_TEMPLATE,
                title=settings.card_title,
                at_sender=is_group,  # 群聊回复 @ 提问人
            )
            if await candidate.start("⏳ 正在思考…"):
                card = candidate
            else:
                logger.warning("AI 卡片未创建成功（无实例ID），降级为普通消息")
        except Exception as e:  # noqa: BLE001
            logger.error("创建 AI 卡片失败，降级为普通消息: %s", e)
            card = None

        # 图片：下载到临时目录作为附件随消息上传给 agent
        attachments, img_errors = await self._download_images(incoming_message)
        if attachments:
            logger.info("随消息上传 %d 张图片附件", len(attachments))
        msg_text = text
        if img_errors:
            msg_text += ("\n\n" if msg_text else "") + "（" + "、".join(img_errors) + "）"

        shown_status = ""
        answer = ""
        queued_secs = 0
        started = time.monotonic()
        last_push = 0.0
        try:
            async for event in manager.converse(
                session_key, "dingtalk", uid, user_name, msg_text,
                attachments=attachments,
            ):
                etype = event.get("type")
                if etype == "wait":
                    # 排队进度（会话串行/全局并发/等待 run 出现），始终可见
                    shown_status = event.get("text") or (
                        f"排队等待 agent… 已等 {event.get('elapsed', 0)}s"
                    )
                    queued_secs = max(queued_secs, event.get("elapsed", 0))
                elif etype == "status":
                    # 卡片默认只显示「正在思考」；show_process 开启时才透出过程细节
                    shown_status = event.get("text", "") if settings.show_process else ""
                elif etype == "delta":
                    answer += event.get("text", "")
                elif etype == "error":
                    answer += f"\n\n⚠️ {event.get('message', '')}"
                    break
                elif etype == "done":
                    answer = event.get("answer") or answer
                    queued_secs = event.get("queued_secs", queued_secs)

                now = time.monotonic()
                finished = etype in ("done", "error")
                if card and (
                    finished or now - last_push >= settings.stream_update_interval
                ):
                    last_push = now
                    content = self._card_content(
                        shown_status, answer, finished,
                        elapsed=int(now - started), queued_secs=queued_secs,
                    )
                    await card.push(content)
            if card:
                await card.finish(answer.strip() or "（agent 没有产生回复）")
            else:
                # 无卡片（创建失败被降级）：把最终答案以普通消息送达（带 @）
                await self._reply(
                    incoming_message, title,
                    answer.strip() or "（agent 没有产生回复）",
                )
        except Exception as e:  # noqa: BLE001
            logger.exception("处理钉钉消息失败")
            if card:
                try:
                    await card.fail(f"⚠️ 处理失败: {e}")
                except Exception:  # noqa: BLE001
                    pass
            await self._reply(incoming_message, title, f"⚠️ 处理失败: {e}")
        finally:
            # CLI 已在命令执行时上传附件，本地临时文件直接清理
            if attachments:
                await asyncio.to_thread(self._cleanup_attachments, attachments)

    async def _reply(
        self, incoming_message: dingtalk_stream.ChatbotMessage, title: str, text: str
    ) -> None:
        """sessionWebhook 普通消息回复（带真 @；失败仅记日志）。"""
        webhook = incoming_message.session_webhook
        if not webhook:
            logger.error("sessionWebhook 缺失，无法回复")
            return
        try:
            await self.bot.dtk.reply_markdown(
                webhook, title, text,
                at_staff_id=incoming_message.sender_staff_id or "",
            )
        except Exception:  # noqa: BLE001
            logger.exception("回复 markdown 失败")

    @staticmethod
    def _cleanup_attachments(paths: List[str]) -> None:
        import shutil

        try:
            dirs = {str(Path(p).parent) for p in paths}
            for d in dirs:
                shutil.rmtree(d, ignore_errors=True)
        except Exception:  # noqa: BLE001
            logger.debug("清理附件临时目录失败", exc_info=True)

    @staticmethod
    def _card_content(status: str, answer: str, finished: bool, elapsed: int = 0, queued_secs: int = 0) -> str:
        """卡片正文：流式阶段 = 状态引用块 + 已产出内容；完成态 = 正文（+排队记录）。"""
        if finished:
            text = answer.strip() or "（无回复）"
            return f"> ⏱ 本条消息排队 {queued_secs}s 后处理\n\n{text}"

        # 流式：状态行（引用块）+ 已生成的正文
        status_line = f"⏳ {status}" if status else "⏳ 正在思考…"
        if elapsed:
            status_line += f" · {elapsed}s"
        parts = [f"> {status_line}"]
        if answer.strip():
            parts.append(answer.strip())
        return "\n\n".join(parts)
