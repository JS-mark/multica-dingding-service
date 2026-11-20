"""配置加载：config/config.yaml + 环境变量覆盖。"""

from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass, field, fields, replace
from pathlib import Path
from typing import Dict, List, Tuple

import yaml

logger = logging.getLogger("multica_dingding.settings")


def _csv(v: str) -> List[str]:
    """逗号分隔 -> 列表（环境变量里配白名单用）。"""
    return [x.strip() for x in v.split(",") if x.strip()]


def _json_dict(v: str) -> Dict[str, str]:
    """JSON 对象字符串 -> dict（环境变量里配别名表用）。"""
    try:
        d = json.loads(v)
    except json.JSONDecodeError:
        return {}
    return {str(k): str(val) for k, val in d.items()} if isinstance(d, dict) else {}

# 环境变量 -> 配置路径 覆盖表
_ENV_OVERRIDES = {
    "MDS_LOG_LEVEL": ("log_level", str),
    "MDS_API_HOST": ("api.host", str),
    "MDS_API_PORT": ("api.port", int),
    "MDS_API_KEY": ("api.api_key", str),
    "MDS_ADMIN_TOKEN": ("api.admin_token", str),
    "MDS_MULTICA_BINARY": ("multica.binary", str),
    "MDS_MULTICA_PROFILE": ("multica.profile", str),
    "MDS_MULTICA_SERVER_URL": ("multica.server_url", str),
    "MDS_MULTICA_TOKEN": ("multica.token", str),
    "MDS_MULTICA_WORKSPACE_ID": ("multica.workspace_id", str),
    "MDS_MULTICA_WORKSPACES": ("multica.workspaces", _json_dict),
    "MDS_MULTICA_AGENT_ID": ("multica.agent_id", str),
    "MDS_MULTICA_AGENTS": ("multica.agents", _json_dict),
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
    # 管理接口（/api/admin/*）鉴权 token；留空 = 启动时自动生成并持久化到 data/admin_token
    admin_token: str = ""


@dataclass
class MulticaSettings:
    binary: str = "multica"
    profile: str = ""
    server_url: str = ""   # multica 服务端，如 https://api.multica.ai 或自部署 http://multica.example.com；留空用 profile 默认
    token: str = ""        # PAT token；设置后启动时自动写入 ~/.multica/profiles/<profile>/config.json（已存在则不覆盖）。
                           # 供无法挂载 multica-home 目录的 Docker 运行时（named volume 模式）注入认证
    workspace_id: str = ""
    # 别名表（可选）：别名 -> UUID。/ws、/agent 命令与 REST 参数都接受别名或 UUID
    workspaces: Dict[str, str] = field(default_factory=dict)
    agent_id: str = ""
    agents: Dict[str, str] = field(default_factory=dict)
    poll_interval: float = 1.2
    run_wait_timeout: float = 300.0   # run 未出现（如 agent 排队/daemon 离线）的等待上限
    run_timeout: float = 7200.0       # 单轮 run 跟随上限（agent 长任务可达小时级）
    max_concurrent_rounds: int = 12   # 全局同时进行的对话轮数上限，超出排队
    max_session_queue: int = 5        # 单会话排队上限，超出直接拒绝该条消息

    # ------------------------------------------------------------------
    def resolve_workspace(self, s: str) -> str:
        """别名 -> workspace UUID；不是已知别名则原样返回（视为 UUID）。"""
        return self.workspaces.get(s, s)

    def resolve_agent(self, s: str) -> str:
        """别名 -> agent UUID；不是已知别名则原样返回（视为 UUID）。"""
        return self.agents.get(s, s)

    def workspace_alias(self, ws_id: str) -> str:
        """UUID -> 别名；未配别名时返回原 UUID（首8位展示由调用方裁剪）。"""
        for alias, wid in self.workspaces.items():
            if wid == ws_id:
                return alias
        return ws_id

    def agent_alias(self, agent_id: str) -> str:
        for alias, aid in self.agents.items():
            if aid == agent_id:
                return alias
        return agent_id


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
    # 额外机器人（仅 config.yaml）：{slug: {dingtalk 配置 + 可选 multica 覆盖}}，见 iter_bots
    dingtalk_bots: Dict[str, dict] = field(default_factory=dict)
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
            dingtalk_bots=raw.get("dingtalk_bots", {}) or {},
            config_path=str(config_path),
        )
        return s


# 多机器人 slug 约束：小写字母/数字/连字符（进会话键，不能含 ':'）
_BOT_SLUG_RE = re.compile(r"^[a-z0-9-]{1,32}$")


def _pick_known(dc_cls, cfg: dict, where: str) -> dict:
    """过滤出 dataclass 已知字段；未知键告警（防止配置手误静默失效）。"""
    known = {f.name for f in fields(dc_cls)}
    picked = {k: v for k, v in cfg.items() if k in known}
    for k in cfg:
        if k not in known:
            logger.warning("%s: 忽略未知配置键 %r（不在 %s 中）", where, k, dc_cls.__name__)
    return picked


def bots_file_path(db_path: str) -> str:
    """运行时机器人配置文件路径（管理接口维护），与数据库同目录。"""
    return str(Path(db_path).parent / "bots.yaml")


def load_bots_file(path: str) -> Dict[str, dict]:
    """读取运行时机器人配置；不存在/损坏返回空（config.yaml 的 dingtalk_bots 仍生效）。"""
    p = Path(path)
    if not p.exists():
        return {}
    try:
        d = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError:
        logger.warning("bots 文件解析失败（%s），忽略", path)
        return {}
    return {str(k): v for k, v in d.items()} if isinstance(d, dict) else {}


def save_bots_file(path: str, bots: Dict[str, dict]) -> None:
    """原子写入运行时机器人配置。"""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_name(p.name + ".tmp")
    tmp.write_text(yaml.safe_dump(bots, allow_unicode=True, sort_keys=True), encoding="utf-8")
    os.replace(tmp, p)


def build_bot_cfg(
    slug: str, cfg: dict, base_multica: MulticaSettings
) -> Tuple[DingTalkSettings, MulticaSettings]:
    """把单条 dingtalk_bots 配置组装成 (DingTalkSettings, MulticaSettings)。

    无效配置抛 ValueError（slug 非法 / 非映射 / 缺凭证 / multica 非映射），
    供配置加载（警告跳过）与管理接口（400 拒绝）共用。
    白名单等钉钉配置完全独立（不继承默认机器人），multica 只覆盖显式给出的键。
    """
    slug = str(slug).strip()
    if not _BOT_SLUG_RE.match(slug):
        raise ValueError(f"机器人 {slug!r}：slug 只允许小写字母/数字/连字符（1-32位）")
    if not isinstance(cfg, dict):
        raise ValueError(f"机器人 {slug!r}：配置需为键值映射")
    dt_cfg = _pick_known(
        DingTalkSettings,
        {k: v for k, v in cfg.items() if k != "multica"},
        f"dingtalk_bots.{slug}",
    )
    ms_cfg = cfg.get("multica") or {}
    if not isinstance(ms_cfg, dict):
        raise ValueError(f"机器人 {slug!r}：multica 配置需为键值映射")
    dt = DingTalkSettings(**{"enabled": True, **dt_cfg})
    if not (dt.client_id and dt.client_secret):
        raise ValueError(f"机器人 {slug!r}：缺少 client_id/client_secret")
    ms = replace(
        base_multica, **_pick_known(MulticaSettings, ms_cfg, f"dingtalk_bots.{slug}.multica")
    )
    return dt, ms


def iter_bots(s: Settings) -> List[Tuple[str, DingTalkSettings, MulticaSettings]]:
    """把配置展开成机器人清单：[(slug, 钉钉配置, multica 生效配置)]。

    - 默认机器人 slug="default"（会话键保持 dingtalk:{uid}，老会话兼容）
    - 额外机器人来自 dingtalk_bots（config.yaml + data/bots.yaml 合并结果），
      会话键 dingtalk:{slug}:{uid}
    - 无效条目（slug 非法/缺凭证/client_id 重复）跳过并告警，不影响其他机器人
    """
    bots: List[Tuple[str, DingTalkSettings, MulticaSettings]] = []
    seen_cids: Dict[str, str] = {}

    if s.dingtalk.enabled:
        if s.dingtalk.client_id and s.dingtalk.client_secret:
            bots.append(("default", s.dingtalk, s.multica))
            seen_cids[s.dingtalk.client_id] = "default"
        else:
            logger.warning("默认机器人缺少 client_id/client_secret，跳过启动")

    for raw_slug, cfg in (s.dingtalk_bots or {}).items():
        slug = str(raw_slug).strip()
        try:
            dt, ms = build_bot_cfg(slug, cfg, s.multica)
        except ValueError as e:
            logger.warning("%s，跳过", e)
            continue
        if dt.client_id in seen_cids:
            logger.warning(
                "机器人 %r：client_id 与 %r 重复，两个实例会互相抢消息，跳过",
                slug, seen_cids[dt.client_id],
            )
            continue
        seen_cids[dt.client_id] = slug
        bots.append((slug, dt, ms))
    return bots
