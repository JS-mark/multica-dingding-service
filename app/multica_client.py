"""multica CLI 的异步封装。

所有调用走 `multica --output json` 子进程；stdout 前可能带人类可读的提示行，
解析时跳到第一个 JSON 值（`{` 或 `[`）开始的位置。
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger("multica_dingding.multica")

# run 的非终态（来自 `multica issue runs --help`）
RUN_ACTIVE_STATUSES = {"queued", "dispatched", "running", "waiting_local_directory"}


class MulticaError(RuntimeError):
    pass


class MulticaClient:
    def __init__(
        self,
        binary: str = "multica",
        profile: str = "",
        workspace_id: str = "",
        server_url: str = "",
        timeout: float = 60.0,
    ):
        self.binary = binary
        self.profile = profile
        self.workspace_id = workspace_id
        self.server_url = server_url
        self.timeout = timeout

    async def _run(
        self, *args: str, timeout: Optional[float] = None, workspace_id: Optional[str] = None
    ) -> Any:
        cmd = [self.binary]
        if self.profile:
            cmd += ["--profile", self.profile]
        if self.server_url:
            cmd += ["--server-url", self.server_url]
        # 会话可路由到非默认工作区：按调用覆盖，None = 用构造时的默认
        ws = workspace_id if workspace_id is not None else self.workspace_id
        if ws:
            cmd += ["--workspace-id", ws]
        cmd += list(args)

        logger.debug("exec: %s", " ".join(cmd))
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(), timeout=timeout or self.timeout
            )
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            raise MulticaError(f"multica 命令超时（{timeout or self.timeout}s）: {' '.join(args[:3])}")

        out = stdout.decode("utf-8", errors="replace").strip()
        err = stderr.decode("utf-8", errors="replace").strip()
        if proc.returncode != 0:
            raise MulticaError(
                f"multica 命令失败(rc={proc.returncode}): {' '.join(args[:4])}"
                f" | stderr: {err[:500]}"
            )
        return self._parse_json(out)

    @staticmethod
    def _parse_json(out: str) -> Any:
        # stdout 可能带前缀提示行（如 "Comment added to issue ..."），定位第一个 JSON 值
        for i, ch in enumerate(out):
            if ch in "{[":
                try:
                    return json.loads(out[i:])
                except json.JSONDecodeError:
                    break
        if not out:
            return None
        raise MulticaError(f"无法解析 multica 输出: {out[:300]}")

    # ------------------------------------------------------------------
    # workspace
    async def list_workspaces(self) -> List[Dict[str, Any]]:
        return await self._run("workspace", "list", "--output", "json")

    # ------------------------------------------------------------------
    # agent
    async def list_agents(self, workspace_id: Optional[str] = None) -> List[Dict[str, Any]]:
        return await self._run("agent", "list", "--output", "json", workspace_id=workspace_id)

    # ------------------------------------------------------------------
    # issue
    async def create_issue(
        self,
        title: str,
        description: str,
        assignee_id: str,
        attachments: Optional[List[str]] = None,
        workspace_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        args = [
            "issue", "create",
            "--title", title,
            "--description", description,
            "--assignee-id", assignee_id,
            "--allow-duplicate",
        ]
        timeout = 60
        if attachments:
            args.append("--allow-external-file")  # 附件在服务临时目录，不在 CLI cwd
            for p in attachments:
                args += ["--attachment", p]
            timeout = 120
        args += ["--output", "json"]
        return await self._run(*args, timeout=timeout, workspace_id=workspace_id)

    async def get_issue(
        self, issue_id: str, workspace_id: Optional[str] = None
    ) -> Dict[str, Any]:
        return await self._run("issue", "get", issue_id, "--output", "json", workspace_id=workspace_id)

    async def delete_issue(self, issue_id: str, workspace_id: Optional[str] = None) -> None:
        await self._run("issue", "delete", issue_id, "--yes", workspace_id=workspace_id)

    async def list_runs(
        self, issue_id: str, active_only: bool = False, workspace_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        args = ["issue", "runs", issue_id, "--output", "json"]
        if active_only:
            args.append("--active")
        return await self._run(*args, workspace_id=workspace_id)

    async def run_messages(
        self, run_id: str, since: int = 0, workspace_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        return await self._run(
            "issue", "run-messages", run_id,
            "--since", str(since), "--output", "json",
            timeout=60, workspace_id=workspace_id,
        )

    # ------------------------------------------------------------------
    # comment
    async def add_comment(
        self,
        issue_id: str,
        content: str,
        attachments: Optional[List[str]] = None,
        workspace_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """添加顶层评论；返回的 dict 含 id（评论 ID，用于匹配触发的新 run）。"""
        args = ["issue", "comment", "add", issue_id, "--content", content]
        timeout = 30
        if attachments:
            args.append("--allow-external-file")
            for p in attachments:
                args += ["--attachment", p]
            timeout = 120
        args += ["--output", "json"]
        return await self._run(*args, timeout=timeout, workspace_id=workspace_id)

    async def list_comments(
        self, issue_id: str, workspace_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        return await self._run(
            "issue", "comment", "list", issue_id, "--output", "json", workspace_id=workspace_id
        )
