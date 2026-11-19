"""配置加载：config/config.yaml + 环境变量覆盖。"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import List

import yaml


def _csv(v: str) -> List[str]:
    """逗号分隔 -> 列表（环境变量里配白名单用）。"""
    return [x.strip() for x in v.split(",") if x.strip()]

# 环境变量 -> 配置路径 覆盖表
_ENV_OVERRIDES = {
    "MDS_LOG_LEVEL": ("log_level", str),
    "MDS_API_HOST": ("api.host", str),
    "MDS_API_PORT": ("api.port", int),
    "MDS_API_KEY": ("api.api_key", str),
    "MDS_MULTICA_BINARY": ("multica.binary", str),
    "MDS_MULTICA_PROFILE": ("multica.profile", str),
    "MDS_MULTICA_SERVER_URL": ("multica.server_url", str),
    "MDS_MULTICA_TOKEN": ("multica.token", str),
    "MDS_MULTICA_WORKSPACE_ID": ("multica.workspace_id", str),
    "MDS_MULTICA_AGENT_ID": ("multica.agent_id", str),
    "MDS_MULTICA_POLL_INTERVAL": ("multica.poll_interval", float),
    "MDS_MULTICA_RUN_TIMEOUT": ("multica.run_timeout", float),
    "MDS_MAX_CONCURRENT_ROUNDS": ("multica.max_concurrent_rounds", int),
    "MDS_MAX_SESSION_QUEUE": ("multica.max_session_queue", int),
    "MDS_DINGTALK_ENABLED": ("dingtalk.enabled", lambda v: v.lower() in ("1", "true", "yes")),
    "MDS_DINGTALK_CLIENT_ID": ("dingtalk.client_id", str),
    "MDS_DINGTALK_CLIENT_SECRET": ("dingtalk.client_secret", str),
    "MDS_DINGTALK_CARD_TEMPLATE_ID": ("dingtalk.card_template_id", str),
    "MDS_DINGTALK_CARD_TITLE": ("dingtalk.card_title", str),
    "MDS_DINGTALK_SHOW_PROCESS": ("dingtalk.show_process", lambda v: v.lower() in ("1", "true", "yes")),
    "MDS_DINGTALK_ALLOWED_STAFF_IDS": ("dingtalk.allowed_staff_ids", _csv),
    "MDS_DINGTALK_ALLOWED_USER_IDS": ("dingtalk.allowed_user_ids", _csv),
    "MDS_DINGTALK_ALLOWED_NICKS": ("dingtalk.allowed_nicks", _csv),
}


@dataclass
class ApiSettings:
    host: str = "0.0.0.0"
    port: int = 8300
    api_key: str = ""


@dataclass
class MulticaSettings:
    binary: str = "multica"
    profile: str = ""
    server_url: str = ""   # multica 服务端，如 https://api.multica.ai 或自部署 http://multica.example.com；留空用 profile 默认
    token: str = ""        # PAT token；设置后启动时自动写入 ~/.multica/profiles/<profile>/config.json（已存在则不覆盖）。
                           # 供无法挂载 multica-home 目录的 Docker 运行时（named volume 模式）注入认证
    workspace_id: str = ""
    agent_id: str = ""
    poll_interval: float = 1.2
    run_wait_timeout: float = 300.0
    run_timeout: float = 600.0
    max_concurrent_rounds: int = 12   # 全局同时进行的对话轮数上限，超出排队
    max_session_queue: int = 5        # 单会话排队上限，超出直接拒绝该条消息


@dataclass
class DingTalkSettings:
    enabled: bool = True
    client_id: str = ""
    client_secret: str = ""
    card_template_id: str = ""
    card_title: str = ""               # AI 卡片标题；留空 = 不显示标题栏
    show_process: bool = False         # 卡片是否显示 agent 执行过程（💭思考/🔧工具调用）
    stream_update_interval: float = 1.2
    allowed_staff_ids: List[str] = field(default_factory=list)
    allowed_user_ids: List[str] = field(default_factory=list)
    allowed_nicks: List[str] = field(default_factory=list)


@dataclass
class SessionSettings:
    db_path: str = "data/sessions.db"


@dataclass
class Settings:
    log_level: str = "info"
    api: ApiSettings = field(default_factory=ApiSettings)
    multica: MulticaSettings = field(default_factory=MulticaSettings)
    dingtalk: DingTalkSettings = field(default_factory=DingTalkSettings)
    session: SessionSettings = field(default_factory=SessionSettings)
    config_path: str = "config/config.yaml"

    @classmethod
    def load(cls, config_path: str = "config/config.yaml") -> "Settings":
        raw: dict = {}
        path = Path(config_path)
        if path.exists():
            raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}

        def set_nested(d: dict, dotted: str, value):
            keys = dotted.split(".")
            node = d
            for k in keys[:-1]:
                node = node.setdefault(k, {})
            node[keys[-1]] = value

        for env, (dotted, cast) in _ENV_OVERRIDES.items():
            if env in os.environ and os.environ[env] != "":
                set_nested(raw, dotted, cast(os.environ[env]))

        s = cls(
            log_level=raw.get("log_level", "info"),
            api=ApiSettings(**{**raw.get("api", {})}),
            multica=MulticaSettings(**{**raw.get("multica", {})}),
            dingtalk=DingTalkSettings(**{**raw.get("dingtalk", {})}),
            session=SessionSettings(**{**raw.get("session", {})}),
            config_path=str(config_path),
        )
        return s
