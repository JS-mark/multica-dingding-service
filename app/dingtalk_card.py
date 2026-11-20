"""钉钉 AI 卡片 + webhook 回复的全异步实现（httpx，全部调用带超时）。

替代 dingtalk-stream SDK 的卡片/回复方法：SDK 用同步 requests 且无超时，
钉钉 API 挂起时会永久占用 to_thread 线程池，导致回复管线整体冻结（表现为
"消息被吞/回复丢失"）。端点与请求体与 SDK 保持一致。
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from typing import Optional

import httpx

logger = logging.getLogger("multica_dingding.dingtalk_card")

API = "https://api.dingtalk.com"
# SDK 内置通用 AI Markdown 卡片模板
DEFAULT_AI_MARKDOWN_TEMPLATE = "382e4302-551d-4880-bf29-a30acfab2e71.schema"

# AI Markdown 模板的组件顺序
_ORDER = ["msgTitle", "msgContent", "staticMsgContent", "msgTextList", "msgImages", "msgSlider", "msgButtons"]

# AICardStatus（与 SDK 一致）
STATUS_PROCESSING = "1"
STATUS_INPUTING = "2"
STATUS_FINISHED = "3"
STATUS_FAILED = "5"


class DingTalkAsyncClient:
    """OpenAPI 异步客户端：token 缓存 + 卡片三件套 + webhook 回复 + 图片下载链接。"""

    def __init__(self, client_id: str, client_secret: str, timeout: float = 10.0):
        self.client_id = client_id
        self.client_secret = client_secret
        self._http = httpx.AsyncClient(timeout=timeout)
        self._token: Optional[str] = None
        self._token_expire = 0.0

    async def aclose(self) -> None:
        await self._http.aclose()

    # ------------------------------------------------------------------
    async def _access_token(self) -> str:
        now = time.time()
        if self._token and now < self._token_expire:
            return self._token
        resp = await self._http.post(
            f"{API}/v1.0/oauth2/accessToken",
            json={"appKey": self.client_id, "appSecret": self.client_secret},
        )
        resp.raise_for_status()
        data = resp.json()
        self._token = data["accessToken"]
        self._token_expire = now + int(data.get("expireIn", 7200)) - 300
        return self._token

    async def _headers(self) -> dict:
        return {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "x-acs-dingtalk-access-token": await self._access_token(),
        }

    async def _post(self, path: str, body: dict) -> dict:
        resp = await self._http.post(f"{API}{path}", headers=await self._headers(), json=body)
        resp.raise_for_status()
        return resp.json() if resp.text else {}

    async def _put(self, path: str, body: dict) -> dict:
        resp = await self._http.put(f"{API}{path}", headers=await self._headers(), json=body)
        resp.raise_for_status()
        return resp.json() if resp.text else {}

    # ------------------------------------------------------------------
    async def create_and_deliver_card(
        self,
        template_id: str,
        card_data: dict,
        incoming_message,           # dingtalk_stream.ChatbotMessage
        at_sender: bool = False,
        support_forward: bool = True,
    ) -> str:
        """创建 + 投放 AI 卡片，返回 outTrackId；失败抛异常（调用方决定降级）。"""
        instance_id = uuid.uuid1().hex
        create_body = {
            "cardTemplateId": template_id,
            "outTrackId": instance_id,
            "cardData": {"cardParamMap": card_data},
            "callbackType": "STREAM",
            "imGroupOpenSpaceModel": {"supportForward": support_forward},
            "imRobotOpenSpaceModel": {"supportForward": support_forward},
        }
        await self._post("/v1.0/card/instances", create_body)

        deliver_body: dict = {"outTrackId": instance_id, "userIdType": 1}
        if incoming_message.conversation_type == "2":  # 群聊
            deliver_body["openSpaceId"] = f"dtv1.card//IM_GROUP.{incoming_message.conversation_id}"
            group_model: dict = {"robotCode": self.client_id}
            if at_sender and incoming_message.sender_staff_id:
                group_model["atUserIds"] = {
                    incoming_message.sender_staff_id: incoming_message.sender_nick or ""
                }
            deliver_body["imGroupOpenDeliverModel"] = group_model
            await self._post("/v1.0/card/instances/deliver", deliver_body)
        elif incoming_message.conversation_type == "1":  # 单聊
            deliver_body["openSpaceId"] = f"dtv1.card//IM_ROBOT.{incoming_message.sender_staff_id}"
            deliver_body["imRobotOpenDeliverModel"] = {"spaceType": "IM_ROBOT"}
            await self._post("/v1.0/card/instances/deliver", deliver_body)
        else:
            raise ValueError(f"未知会话类型: {incoming_message.conversation_type}")
        return instance_id

    async def update_card(self, instance_id: str, card_data: dict) -> None:
        """整体更新卡片内容（PUT /v1.0/card/instances）。"""
        await self._put("/v1.0/card/instances", {
            "outTrackId": instance_id,
            "cardData": {"cardParamMap": card_data},
        })

    async def streaming_update(self, instance_id: str, content: str, finished: bool = False) -> None:
        """流式更新正文（PUT /v1.0/card/streaming，打字机效果）。"""
        await self._put("/v1.0/card/streaming", {
            "outTrackId": instance_id,
            "guid": uuid.uuid1().hex,
            "key": "msgContent",
            "content": content,
            "isFull": True,
            "isFinalize": finished,
            "isError": False,
        })

    # ------------------------------------------------------------------
    async def reply_markdown(
        self,
        session_webhook: str,
        title: str,
        text: str,
        at_staff_id: str = "",
    ) -> None:
        """sessionWebhook 回复 markdown（带 at.atUserIds，真 @）。

        与 SDK ChatbotClient.reply_markdown 同通道（sessionWebhook，群/单聊均可）。
        官方《群聊@人接入指南》：atUserIds 要在 text 里有对应的 @userId 占位符
        才会渲染成 @昵称并触发提醒，缺占位符时 @ 不生效 —— 这里自动补。
        """
        body: dict = {
            "msgtype": "markdown",
            "markdown": {"title": title, "text": text},
        }
        if at_staff_id:
            body["at"] = {"atUserIds": [at_staff_id]}
            body["markdown"]["text"] = f"@{at_staff_id} {text}"
        resp = await self._http.post(session_webhook, json=body)
        resp.raise_for_status()

    async def get_image_download_url(self, download_code: str) -> str:
        """机器人消息文件下载链接（POST /v1.0/robot/messageFiles/download）。"""
        data = await self._post("/v1.0/robot/messageFiles/download", {
            "robotCode": self.client_id,
            "downloadCode": download_code,
        })
        return data.get("downloadUrl", "")


class AiCardSession:
    """一张 AI 卡片的会话：start → push(多次流式) → finish/fail。

    push 内部处理 SDK 原有的两段式逻辑：首推 = 整体更新（INPUTING + 全量数据），
    后续 = streaming 打字机更新；finish/fail = 整体更新为终态。
    """

    def __init__(
        self,
        client: DingTalkAsyncClient,
        incoming_message,
        template_id: str = DEFAULT_AI_MARKDOWN_TEMPLATE,
        title: str = "",
        logo: str = "",
        at_sender: bool = False,
    ):
        self.client = client
        self.incoming_message = incoming_message
        self.template_id = template_id
        self.title = title
        self.logo = logo
        self.at_sender = at_sender
        self.instance_id = ""
        self._first_push = True

    def _card_data(self, content: str, status: str) -> dict:
        data: dict = {
            "msgContent": content,
            "staticMsgContent": "",
            "flowStatus": status,
            "sys_full_json_obj": json.dumps({"order": _ORDER}),
        }
        if self.title:
            data["msgTitle"] = self.title
        if self.logo:
            data["logo"] = self.logo
        return data

    async def start(self, initial_content: str = "") -> bool:
        """创建并投放卡片；成功返回 True（instance_id 就绪）。"""
        self.instance_id = await self.client.create_and_deliver_card(
            self.template_id,
            self._card_data(initial_content, STATUS_PROCESSING),
            self.incoming_message,
            at_sender=self.at_sender,
        )
        return bool(self.instance_id)

    async def push(self, content: str, finished: bool = False, failed: bool = False) -> None:
        if not self.instance_id:
            raise RuntimeError("卡片未创建")
        if failed:
            await self.client.update_card(self.instance_id, self._card_data(content, STATUS_FAILED))
            return
        if finished:
            await self.client.update_card(self.instance_id, self._card_data(content, STATUS_FINISHED))
            return
        if self._first_push:
            await self.client.update_card(self.instance_id, self._card_data(content, STATUS_INPUTING))
            self._first_push = False
        await self.client.streaming_update(self.instance_id, content)

    async def finish(self, content: str) -> None:
        await self.push(content, finished=True)

    async def fail(self, content: str = "") -> None:
        await self.push(content or "处理失败", failed=True)
