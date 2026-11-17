"""会话编排：把一条用户消息变成 multica 侧的一次 agent 运行，并流式产出事件。

事件类型（dict，键 type）：
  meta    {session_key, issue_key, run_id?}   本轮元信息
  wait    {elapsed, text}                     排队进度（会话串行/全局并发/等待 run）
  status  {text}                              过程状态（思考中/执行工具…）
  delta   {text}                              回答正文增量（累积拼接）
  done    {answer, run_id, issue_key}         结束（answer 为完整回复）
  error   {message}                           失败

并发模型：
  - 同一 session 串行（保证上下文顺序），排队期间持续产出 wait 事件；
  - 全局最多 settings.max_concurrent_rounds 轮同时执行，超出排队并产出 wait 事件；
  - 轮询带随机抖动，避免多轮同时打点造成 CLI 子进程惊群。
"""

from __future__ import annotations

import asyncio
import logging
import random
from typing import Any, AsyncIterator, Dict, Optional

from .multica_client import MulticaClient, MulticaError, RUN_ACTIVE_STATUSES
from .settings import MulticaSettings
from .store import SessionStore

logger = logging.getLogger("multica_dingding.conversation")

# 轮询 comment list 时一次最多拉的条数
_COMMENT_PAGE = 100
# 轮询类调用允许的连续失败次数（网络抖动容错）
_MAX_POLL_FAILURES = 3


class ConversationManager:
    def __init__(
        self,
        client: MulticaClient,
        store: SessionStore,
        settings: MulticaSettings,
    ):
        self.client = client
        self.store = store
        self.settings = settings
        # 同一 session 的发送互斥：避免并发评论触发 run 匹配混乱
        self._locks: Dict[str, asyncio.Lock] = {}
        self._locks_guard = asyncio.Lock()
        # 全局并发轮数上限
        self._sem = asyncio.Semaphore(max(1, settings.max_concurrent_rounds))
        self._active_rounds = 0
        # 每会话排队计数（排队上限 max_session_queue）
        self._pending: Dict[str, int] = {}

    async def _session_lock(self, key: str) -> asyncio.Lock:
        async with self._locks_guard:
            return self._locks.setdefault(key, asyncio.Lock())

    # ------------------------------------------------------------------
    async def reset_session(self, session_key: str) -> bool:
        s = self.store.get_session(session_key)
        if not s:
            return False
        self.store.reset_session(session_key)
        return True

    def get_session(self, session_key: str) -> Optional[dict]:
        return self.store.get_session(session_key)

    # ------------------------------------------------------------------
    async def converse(
        self,
        session_key: str,
        channel: str,
        user_id: str,
        user_name: str,
        text: str,
        force_new: bool = False,
        attachments: Optional[List[str]] = None,
    ) -> AsyncIterator[Dict[str, Any]]:
        """发送一条消息并流式跟随 agent 的运行。

        attachments: 本地文件路径列表（如钉钉图片），随消息作为 multica 附件上传。
        """
        lock = await self._session_lock(session_key)
        enqueue_t = asyncio.get_event_loop().time()

        # ① 会话队列：每条消息收到即入队（占队列位，直到本轮处理完成释放），
        #    同时在队列中的消息（含处理中的）最多 max_session_queue 条，超出拒收。
        pending = self._pending.get(session_key, 0)
        if pending >= self.settings.max_session_queue:
            yield {
                "type": "error",
                "message": f"本会话队列已满（{pending}/{self.settings.max_session_queue}），本条消息未受理，请稍后再发",
            }
            return
        self._pending[session_key] = pending + 1
        acquired = False
        queued_secs = 0
        try:
            # ② 等待本会话上一轮结束（串行保证上下文顺序），每秒产出排队进度
            waited = 0
            while True:
                try:
                    await asyncio.wait_for(lock.acquire(), timeout=1.0)
                    acquired = True
                    queued_secs = int(asyncio.get_event_loop().time() - enqueue_t)
                    break
                except asyncio.TimeoutError:
                    waited += 1
                    yield {
                        "type": "wait",
                        "elapsed": waited,
                        "text": (
                            f"上一条消息处理中，本条排队"
                            f"（{self._pending[session_key]}/{self.settings.max_session_queue}）… 已等 {waited}s"
                        ),
                    }

            # ③ 等待全局并发槽位
            gwaited = 0
            while True:
                try:
                    await asyncio.wait_for(self._sem.acquire(), timeout=1.0)
                    break
                except asyncio.TimeoutError:
                    gwaited += 1
                    yield {
                        "type": "wait",
                        "elapsed": gwaited,
                        "text": f"当前 {self._active_rounds} 轮对话并发执行中，排队等待… 已等 {gwaited}s",
                    }

            self._active_rounds += 1
            try:
                session = self.store.upsert_session(
                    session_key, channel, user_id, user_name, self.settings.agent_id
                )
                if force_new:
                    self.store.reset_session(session_key)
                    session = self.store.get_session(session_key)

                try:
                    trigger_comment_id: Optional[str] = None
                    if session.get("issue_id"):
                        comment = await self.client.add_comment(
                            session["issue_id"], text, attachments=attachments
                        )
                        trigger_comment_id = comment.get("id")
                    else:
                        issue = await self.client.create_issue(
                            title=f"DingTalk 会话 · {user_name or user_id}",
                            description=text,
                            assignee_id=self.settings.agent_id,
                            attachments=attachments,
                        )
                        self.store.bind_issue(session_key, issue["id"], issue["identifier"])
                        session = self.store.get_session(session_key)

                    self.store.add_message(session_key, "user", text)
                    yield {
                        "type": "meta",
                        "session_key": session_key,
                        "issue_key": session.get("issue_key"),
                    }

                    run = None
                    already = None
                    async for ev in self._wait_for_run(session["issue_id"], trigger_comment_id):
                        if ev.get("type") == "run":
                            run = ev["run"]
                        elif ev.get("type") == "already_replied":
                            already = ev.get("answer")
                        else:
                            yield ev
                    if already is not None:
                        # 评论被合并/steer，无独立 run，但 agent 已回复 → 直接送达
                        self.store.add_message(session_key, "agent", already)
                        yield {"type": "done", "answer": already, "run_id": "", "queued_secs": queued_secs}
                        return
                    if run is None:
                        raise MulticaError("等待 agent 运行超时（run 未出现，可能 agent 排队中）")
                    run_id = run["id"]
                    yield {"type": "meta", "run_id": run_id, "issue_key": session.get("issue_key")}

                    async for event in self._follow_run(session_key, session["issue_id"], run_id, trigger_comment_id):
                        if event.get("type") == "done":
                            event["queued_secs"] = queued_secs
                        yield event
                except MulticaError as e:
                    logger.error("converse failed: %s", e)
                    yield {"type": "error", "message": str(e)}
                except Exception as e:  # noqa: BLE001
                    logger.exception("converse unexpected error")
                    yield {"type": "error", "message": f"内部错误: {e}"}
            finally:
                self._active_rounds -= 1
                self._sem.release()
        finally:
            if acquired:
                lock.release()
            self._pending[session_key] = max(0, self._pending[session_key] - 1)

    # ------------------------------------------------------------------
    async def _wait_for_run(
        self, issue_id: str, trigger_comment_id: Optional[str]
    ) -> AsyncIterator[Dict[str, Any]]:
        """轮询【完整 run 历史】（非 active），直到找到本轮触发的 run。

        关键：run 一创建就出现在历史列表里（含 queued），且 agent 快速跑完的
        run 不会漏掉 —— 之前用 --active 轮询存在"run 在两次轮询之间已完成"
        的竞态，导致该条消息永远等不到（表现为回复丢失）。

        产出事件：
          {"type":"wait", "elapsed": 秒}          排队进度
          {"type":"run", "run": {...}}            找到本轮 run（任意状态）
          {"type":"already_replied", "answer"}    无独立 run 但 agent 已在触发
                                                  评论线程下代答（steer/合并场景）
        """
        deadline = asyncio.get_event_loop().time() + self.settings.run_wait_timeout
        poll = max(0.5, self.settings.poll_interval)
        failures = 0
        start = asyncio.get_event_loop().time()
        last_wait_emit = 0.0
        polls = 0
        while asyncio.get_event_loop().time() < deadline:
            try:
                runs = await self.client.list_runs(issue_id)
                failures = 0
            except MulticaError as e:
                failures += 1
                if failures >= _MAX_POLL_FAILURES:
                    raise
                logger.warning("list_runs 轮询失败(%d/%d): %s", failures, _MAX_POLL_FAILURES, e)
                await asyncio.sleep(poll)
                continue
            polls += 1
            found: Optional[Dict[str, Any]] = None
            if trigger_comment_id:
                for r in runs:
                    if r.get("trigger_comment_id") == trigger_comment_id:
                        found = r
                        break
            elif runs:
                # 首条消息触发：issue 刚创建，历史里最新的 run 就是本轮的
                found = runs[0]
            if found:
                yield {"type": "run", "run": found}
                return

            # 兜底：评论被合并/steer 进别的 run 时不会产生以它为 trigger 的 run，
            # 但 agent 可能已直接在触发评论线程下回复 —— 每隔几轮检查一次
            if trigger_comment_id and polls % 4 == 0:
                try:
                    reply = await self._final_answer(issue_id, trigger_comment_id, run_id=None)
                except Exception:  # noqa: BLE001
                    reply = None
                if reply:
                    yield {"type": "already_replied", "answer": reply}
                    return

            # 每 ~3s 通知一次排队进度
            now = asyncio.get_event_loop().time()
            if now - last_wait_emit >= 3.0:
                last_wait_emit = now
                yield {"type": "wait", "elapsed": int(now - start)}
            await asyncio.sleep(1.0 + random.uniform(0, 0.5))

    async def _follow_run(
        self,
        session_key: str,
        issue_id: str,
        run_id: str,
        trigger_comment_id: Optional[str],
    ) -> AsyncIterator[Dict[str, Any]]:
        poll = max(0.5, self.settings.poll_interval)
        deadline = asyncio.get_event_loop().time() + self.settings.run_timeout
        since = 0
        answer_text = ""

        failures = 0
        while True:
            try:
                msgs = await self.client.run_messages(run_id, since=since)
                runs = await self.client.list_runs(issue_id, active_only=True)
                failures = 0
            except MulticaError as e:
                failures += 1
                if failures >= _MAX_POLL_FAILURES:
                    yield {"type": "error", "message": f"轮询 multica 连续失败: {e}"}
                    return
                logger.warning("轮询失败(%d/%d): %s", failures, _MAX_POLL_FAILURES, e)
                await asyncio.sleep(poll)
                continue

            for m in msgs:
                since = max(since, m.get("seq", 0))
                mtype = m.get("type")
                if mtype == "text":
                    answer_text += m.get("content", "")
                    yield {"type": "delta", "text": m.get("content", "")}
                elif mtype == "thinking":
                    thought = (m.get("content") or "").strip()
                    if thought:
                        yield {"type": "status", "text": f"💭 {self._clip(thought)}"}
                elif mtype == "tool_use":
                    label = self._tool_label(m)
                    yield {"type": "status", "text": f"🔧 {label}"}

            still_active = any(r["id"] == run_id for r in runs)
            if not still_active:
                break
            if asyncio.get_event_loop().time() > deadline:
                yield {"type": "error", "message": f"运行超时（>{self.settings.run_timeout:.0f}s）"}
                return
            await asyncio.sleep(poll + random.uniform(0, 0.4))

        # 终态：完整回复优先取 agent 在触发评论线程下的评论
        final = await self._final_answer(issue_id, trigger_comment_id, run_id)
        if final is None:
            final = answer_text.strip() or "（agent 没有产生回复）"
        self.store.add_message(session_key, "agent", final, run_id)
        yield {"type": "done", "answer": final, "run_id": run_id}

    async def _final_answer(
        self, issue_id: str, trigger_comment_id: Optional[str], run_id: str
    ) -> Optional[str]:
        """从评论线程找 agent 的最终回复。

        - 评论触发：parent_id == trigger_comment_id 且 author_type == agent
        - 首条消息触发：取最新的 agent 顶层评论（run 结束后写入）
        """
        try:
            comments = await self.client.list_comments(issue_id)
        except MulticaError:
            return None
        if not isinstance(comments, list):
            return None
        candidates = [
            c
            for c in comments
            if c.get("author_type") == "agent"
            and (c.get("type") == "comment" or c.get("type") is None)
        ]
        if trigger_comment_id:
            threaded = [
                c for c in candidates if c.get("parent_id") == trigger_comment_id
            ]
            if threaded:
                return threaded[-1].get("content")
        elif candidates:
            return candidates[-1].get("content")
        return None

    # ------------------------------------------------------------------
    @staticmethod
    def _clip(text: str, limit: int = 60) -> str:
        text = " ".join(text.split())
        return text if len(text) <= limit else text[: limit - 1] + "…"

    @staticmethod
    def _tool_label(m: Dict[str, Any]) -> str:
        tool = m.get("tool", "tool")
        inp = m.get("input") or {}
        desc = inp.get("description") or inp.get("command") or inp.get("file_path") or ""
        label = ConversationManager._clip(str(desc)) if desc else ""
        return f"{tool} {label}".strip()
