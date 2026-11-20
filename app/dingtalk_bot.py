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
from .settings import DingTalkSettings, MulticaSettings

logger = logging.getLogger("multica_dingding.dingtalk")


class DingTalkBot:
    def __init__(
        self,
        settings: DingTalkSettings,
        manager: ConversationManager,
        slug: str = "default",
    ):
        self.settings = settings
        self.manager = manager
        # 机器人标识（多机器人配置里的 slug；default 的会话键保持老格式）
        self.slug = slug
        self.client: Optional[dingtalk_stream.DingTalkStreamClient] = None
        self.connected = False
        # 主 event loop：SDK 回调线程把异步处理投递回这里（会话/卡片都在主 loop）
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        # SDK 专用 loop（run_forever 线程内创建），shutdown 用
        self._sdk_loop: Optional[asyncio.AbstractEventLoop] = None
        self._stopped = False
        self.dtk = DingTalkAsyncClient(settings.client_id, settings.client_secret)

    def session_key(self, uid: str) -> str:
        """会话键按机器人隔离：default 老格式 dingtalk:{uid}，其余 dingtalk:{slug}:{uid}。"""
        if self.slug == "default":
            return f"dingtalk:{uid}"
        return f"dingtalk:{self.slug}:{uid}"

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

    async def _start_client(self) -> None:
        """建立 Stream WebSocket 长连接并保持（跑在 SDK 专用 loop，见 run_forever）。"""
        credential = dingtalk_stream.Credential(
            self.settings.client_id, self.settings.client_secret
        )
        self.client = dingtalk_stream.DingTalkStreamClient(credential)
        self.client.register_callback_handler(
            dingtalk_stream.chatbot.ChatbotMessage.TOPIC, _BotHandler(self)
        )
        self.connected = True
        logger.info("钉钉 Stream 客户端[%s]已启动", self.slug)
        try:
            await self.client.start()
        finally:
            self.connected = False

    def run_forever(self) -> None:
        """阻塞运行（含重连退避），必须在独立线程调用（main.start_bot_task 负责拉线程）。

        SDK 的建连/取 token 走【同步 requests】，凭证错误或网络故障时会长时间
        阻塞——不能跑在主 event loop，否则一个机器人异常会拖垮所有机器人与
        REST API。这里给 SDK 单独的 loop，与主 loop 完全隔离；消息处理经
        self.loop（主 loop）投递回去。
        """
        self._sdk_loop = sdk = asyncio.new_event_loop()
        asyncio.set_event_loop(sdk)
        retry = 0
        try:
            while not self._stopped:
                try:
                    sdk.run_until_complete(self._start_client())
                except asyncio.CancelledError:
                    break
                except Exception:  # noqa: BLE001
                    retry += 1
                    wait = min(30, 2 ** min(retry, 5))
                    logger.exception(
                        "钉钉 Stream[%s] 断开，%ss 后重连（第 %d 次）", self.slug, wait, retry
                    )
                    time.sleep(wait)
                else:
                    retry = 0
                    time.sleep(1)
        finally:
            self.connected = False
            sdk.close()

    def shutdown(self) -> None:
        """请求停止（幂等）：置停止标记并停掉 SDK loop，线程随后退出。"""
        self._stopped = True
        self.connected = False
        loop = self._sdk_loop
        if loop and loop.is_running():
            loop.call_soon_threadsafe(loop.stop)


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
            "收到消息: type=%s conv_type=%s conv_id=%s senderId=%s staff=%s nick=%s atUsers=%s text=%r",
            incoming_message.message_type,
            incoming_message.conversation_type,
            incoming_message.conversation_id,          # 全量（群卡片投放要用 openConversationId）
            incoming_message.sender_id,                # unionId 形态，@人实验用
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
        session_key = self.bot.session_key(uid)
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
                "**可用命令**\n"
                "- 直接发消息与我对话（支持图片/图文混合，图片会作为附件转给 agent）\n"
                "- `/new` 开始新会话（清空上下文）\n"
                "- `/issue` 查看当前会话绑定的 issue\n"
                "- `/ws` 查看/切换工作区（`/ws <名称|UUID>`）\n"
                "- `/agent` 查看/切换当前工作区的 agent（`/agent <名称|UUID>`）",
            )
            return
        if low == "/issue":
            s = manager.get_session(session_key)
            issue_key = (s or {}).get("issue_key") or "无"
            await self._reply(incoming_message, title, f"当前会话 issue：`{issue_key}`")
            return
        if low == "/ws" or low.startswith("/ws "):
            await self._cmd_workspace(incoming_message, title, session_key, text)
            return
        if low == "/agent" or low.startswith("/agent "):
            await self._cmd_agent(incoming_message, title, session_key, text)
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

    # ------------------------------------------------------------------
    # /ws 与 /agent：多工作区 / 多 agent 路由
    def _ms(self) -> MulticaSettings:
        return self.bot.manager.settings

    def _current_target(self, session_key: str) -> Tuple[str, str]:
        """(当前生效 workspace UUID, 会话已选 agent UUID)；agent 空串 = 待选。"""
        s = self.bot.manager.get_session(session_key) or {}
        m = self._ms()
        return (
            s.get("workspace_id") or m.workspace_id,
            s.get("agent_id") or "",
        )

    @staticmethod
    def _short(uuid: str) -> str:
        return f"`{uuid[:8]}…`" if uuid and uuid != "" else "未配置"

    @staticmethod
    def _match_id(rows: list, arg: str, aliases: dict) -> str:
        """按 UUID / 名称 / 别名 在 live 列表里匹配，返回对应 UUID；无匹配返回空串。

        列表展示的是名称，选择参数理应同样接受名称——别名表只是额外的快捷方式。
        """
        q = arg.strip()
        ql = q.lower()
        for k, v in aliases.items():
            if k == q or k.lower() == ql:
                return v
        for row in rows:
            rid = row.get("id") or ""
            if rid == q or (row.get("name") or "").lower() == ql:
                return rid
        return ""

    def _ws_label(self, ws_id: str) -> str:
        alias = self._ms().workspace_alias(ws_id)
        return f"`{alias}`" if alias != ws_id else self._short(ws_id)

    async def _cmd_workspace(
        self,
        incoming_message: dingtalk_stream.ChatbotMessage,
        title: str,
        session_key: str,
        text: str,
    ) -> None:
        manager = self.bot.manager
        m = self._ms()
        parts = text.split(None, 1)
        arg = parts[1].strip() if len(parts) > 1 else ""

        cur_ws, cur_agent = self._current_target(session_key)

        try:
            rows = await manager.client.list_workspaces()
        except Exception as e:  # noqa: BLE001
            rows, err = None, str(e)
        else:
            err = ""

        if not arg:
            lines = ["**工作区**", f"当前：{self._ws_label(cur_ws) if cur_ws else '未配置'}"]
            if rows is None:
                lines.append(f"\n⚠️ 无法获取工作区列表：{err}")
            alias_by_id = {wid: alias for alias, wid in m.workspaces.items()}
            if rows:
                lines.append("\n发送 `/ws <名称|UUID>` 切换：")
                for w in rows:
                    wid = w.get("id") or ""
                    alias = alias_by_id.get(wid)
                    label = alias or (w.get("name") or wid)
                    mark = " ←当前" if wid == cur_ws else ""
                    extra = "" if alias else f" `{wid[:8]}…`"
                    lines.append(f"- `{label}`{extra}{mark}")
            elif m.workspaces:
                lines.append("\n发送 `/ws <名称|UUID>` 切换：")
                for alias, wid in m.workspaces.items():
                    mark = " ←当前" if wid == cur_ws else ""
                    lines.append(f"- `{alias}`{mark}")
            await self._reply(incoming_message, title, "\n".join(lines))
            return

        if rows is None:
            await self._reply(
                incoming_message, title,
                f"⚠️ 无法获取工作区列表（multica 调用失败：{err}），请稍后再试",
            )
            return

        ws_id = self._match_id(rows, arg, m.workspaces)
        if not ws_id:
            await self._reply(
                incoming_message, title,
                f"⚠️ 未找到工作区 `{arg}`，发送 `/ws` 查看可选列表",
            )
            return

        if ws_id == cur_ws:
            # 已在该工作区：顺便给出 agent 列表（覆盖 /ws 切换后待选 agent 的场景）
            hint = (
                "\n\n⚠️ 当前尚未选择 agent，请先 `/agent <名称>` 选择。"
                if not cur_agent
                else ""
            )
            await self._reply(
                incoming_message, title,
                f"当前已在工作区 {self._ws_label(ws_id)}。\n\n"
                + await self._agent_list_lines(ws_id)
                + hint,
            )
            return

        # 切工作区后原 agent 不一定在新工作区，置为待选，强制用户重新 /agent
        manager.set_target(session_key, ws_id, "")
        await self._reply(
            incoming_message, title,
            f"✅ 已切换工作区：{self._ws_label(ws_id)}\n\n"
            + await self._agent_list_lines(ws_id)
            + "\n\n⚠️ 请先 `/agent <名称|UUID>` 选择该工作区下的 agent，再开始对话。",
        )

    async def _cmd_agent(
        self,
        incoming_message: dingtalk_stream.ChatbotMessage,
        title: str,
        session_key: str,
        text: str,
    ) -> None:
        manager = self.bot.manager
        m = self._ms()
        parts = text.split(None, 1)
        arg = parts[1].strip() if len(parts) > 1 else ""

        cur_ws, cur_agent = self._current_target(session_key)
        if not cur_ws:
            await self._reply(
                incoming_message, title,
                "⚠️ 未配置工作区：请先配置 multica.workspace_id，或发 `/ws <名称|UUID>` 切换",
            )
            return

        if not arg:
            agent_label = (
                f"`{m.agent_alias(cur_agent)}`"
                if cur_agent and m.agent_alias(cur_agent) != cur_agent
                else (self._short(cur_agent) if cur_agent else "未选择")
            )
            lines = [
                "**当前目标**",
                f"工作区：{self._ws_label(cur_ws)} · agent：{agent_label}",
                "",
                await self._agent_list_lines(cur_ws),
            ]
            await self._reply(incoming_message, title, "\n".join(lines))
            return

        try:
            rows = await manager.client.list_agents(workspace_id=cur_ws)
        except Exception as e:  # noqa: BLE001
            await self._reply(
                incoming_message, title,
                f"⚠️ 无法校验 agent（multica 调用失败：{e}），请稍后再试",
            )
            return
        agent_uuid = self._match_id(
            [a for a in rows if not a.get("archived_at")], arg, m.agents
        )
        if not agent_uuid:
            await self._reply(
                incoming_message, title,
                f"⚠️ agent `{arg}` 不在当前工作区，发送 `/agent` 查看可选列表",
            )
            return

        alias = m.agent_alias(agent_uuid)
        label = f"`{alias}`" if alias != agent_uuid else self._short(agent_uuid)
        stored = manager.get_session(session_key) or {}
        if agent_uuid == (stored.get("agent_id") or "") and stored.get("issue_id"):
            await self._reply(
                incoming_message, title, f"当前已是 agent {label}，会话保持不变。"
            )
            return

        # agent 绑定在 issue 上：切换 = 断开当前会话，下一条消息新建 issue
        manager.set_target(session_key, cur_ws, agent_uuid)
        await self._reply(
            incoming_message, title,
            f"✅ 已切换 agent：{label}（工作区 {self._ws_label(cur_ws)}）\n"
            "已开启新会话，发送消息开始对话。",
        )

    async def _agent_list_lines(self, ws_id: str) -> str:
        """当前工作区的 agent 列表（live 查询，过滤 archived）。"""
        m = self._ms()
        try:
            rows = await self.bot.manager.client.list_agents(workspace_id=ws_id)
        except Exception as e:  # noqa: BLE001
            return f"⚠️ 无法获取 agent 列表：{e}"
        rows = [a for a in rows if not a.get("archived_at")]
        if not rows:
            return "（该工作区没有可用 agent）"
        alias_by_id = {aid: alias for alias, aid in m.agents.items()}
        lines = ["**该工作区的 agent**（`/agent <名称|UUID>` 选择）："]
        for a in rows:
            aid = a.get("id") or ""
            alias = alias_by_id.get(aid)
            name = a.get("name") or aid
            if alias:
                lines.append(f"- `{alias}` — {name}")
            else:
                lines.append(f"- `{name}` `{aid[:8]}…`")
        return "\n".join(lines)

    async def _reply(
        self, incoming_message: dingtalk_stream.ChatbotMessage, title: str, text: str
    ) -> None:
        """sessionWebhook 普通消息回复（群聊带真 @；失败仅记日志）。

        群聊 @ 提问人（会渲染成 @昵称）；单聊无需 @（回复本身即私聊语义）。
        """
        webhook = incoming_message.session_webhook
        if not webhook:
            logger.error("sessionWebhook 缺失，无法回复")
            return
        is_group = incoming_message.conversation_type == "2"
        at = (incoming_message.sender_staff_id or "") if is_group else ""
        try:
            await self.bot.dtk.reply_markdown(webhook, title, text, at_staff_id=at)
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
