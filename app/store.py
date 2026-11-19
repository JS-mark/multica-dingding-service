"""会话存储（SQLite，回滚日志模式）。

每个「渠道 + 用户」对应一个 session；session 绑定一个 multica issue，
第一条消息是 issue 描述，后续消息是 issue 顶层评论，agent 在评论线程里回复。
"""

from __future__ import annotations

import sqlite3
import threading
import time
from pathlib import Path
from typing import List, Optional

_SCHEMA = """
CREATE TABLE IF NOT EXISTS sessions (
    key         TEXT PRIMARY KEY,
    channel     TEXT NOT NULL,           -- dingtalk | api
    user_id     TEXT NOT NULL,           -- 渠道内用户标识
    user_name   TEXT DEFAULT '',
    issue_id    TEXT,
    issue_key   TEXT,
    agent_id    TEXT,
    created_at  REAL NOT NULL,
    updated_at  REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS messages (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    session_key TEXT NOT NULL,
    role        TEXT NOT NULL,           -- user | agent | system
    content     TEXT NOT NULL,
    run_id      TEXT DEFAULT '',
    created_at  REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_messages_session ON messages(session_key, id);
"""


class SessionStore:
    def __init__(self, db_path: str):
        try:
            Path(db_path).parent.mkdir(parents=True, exist_ok=True)
            self._conn = sqlite3.connect(db_path, check_same_thread=False)
        except (OSError, sqlite3.Error) as e:
            raise RuntimeError(
                f"会话数据库不可用: {db_path}（{e}）。"
                "请检查 data 挂载是否可写（属主需为运行用户 uid 10001），"
                "或通过 session.db_path / 环境变量调整路径"
            ) from e
        self._lock = threading.Lock()
        self._conn.row_factory = sqlite3.Row
        # 不用 WAL：WAL 的共享内存索引依赖 mmap，在 Docker 卷挂载
        # （colima/virtiofs 等网络文件系统语义）上会出现提交丢失。
        # 回滚日志模式在任何文件系统上都安全，且本服务写入量很小。
        self._conn.execute("PRAGMA journal_mode=DELETE")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    def _execute(self, sql: str, params: tuple = ()) -> sqlite3.Cursor:
        with self._lock:
            cur = self._conn.execute(sql, params)
            self._conn.commit()
            return cur

    # ------------------------------------------------------------------
    def get_session(self, key: str) -> Optional[dict]:
        row = self._conn.execute(
            "SELECT * FROM sessions WHERE key = ?", (key,)
        ).fetchone()
        return dict(row) if row else None

    def upsert_session(
        self, key: str, channel: str, user_id: str, user_name: str, agent_id: str
    ) -> dict:
        now = time.time()
        self._execute(
            """INSERT INTO sessions (key, channel, user_id, user_name, agent_id, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(key) DO UPDATE SET user_name = excluded.user_name, updated_at = excluded.updated_at""",
            (key, channel, user_id, user_name, agent_id, now, now),
        )
        return self.get_session(key)

    def bind_issue(self, key: str, issue_id: str, issue_key: str) -> None:
        self._execute(
            "UPDATE sessions SET issue_id = ?, issue_key = ?, updated_at = ? WHERE key = ?",
            (issue_id, issue_key, time.time(), key),
        )

    def reset_session(self, key: str) -> None:
        """断开与 issue 的绑定（保留历史消息记录），下一条消息将新建 issue。"""
        self._execute(
            "UPDATE sessions SET issue_id = NULL, issue_key = NULL, updated_at = ? WHERE key = ?",
            (time.time(), key),
        )

    def list_sessions(self) -> List[dict]:
        rows = self._conn.execute(
            "SELECT * FROM sessions ORDER BY updated_at DESC LIMIT 200"
        ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    def add_message(
        self, session_key: str, role: str, content: str, run_id: str = ""
    ) -> None:
        self._execute(
            "INSERT INTO messages (session_key, role, content, run_id, created_at) VALUES (?, ?, ?, ?, ?)",
            (session_key, role, content, run_id, time.time()),
        )

    def recent_messages(self, session_key: str, limit: int = 50) -> List[dict]:
        rows = self._conn.execute(
            """SELECT * FROM (
                   SELECT * FROM messages WHERE session_key = ? ORDER BY id DESC LIMIT ?
               ) ORDER BY id ASC""",
            (session_key, limit),
        ).fetchall()
        return [dict(r) for r in rows]
