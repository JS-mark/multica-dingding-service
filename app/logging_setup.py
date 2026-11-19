"""日志：控制台 + 文件（logs/app.log，按天轮转）。"""

from __future__ import annotations

import logging
import sys
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

_FORMAT = "%(asctime)s %(levelname)-7s [%(name)s] %(message)s"


def setup_logging(level: str = "info", log_dir: str = "logs") -> logging.Logger:
    root = logging.getLogger()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))
    root.handlers.clear()

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(logging.Formatter(_FORMAT))
    root.addHandler(console)

    try:
        Path(log_dir).mkdir(parents=True, exist_ok=True)
        file_handler = TimedRotatingFileHandler(
            Path(log_dir) / "app.log", when="midnight", backupCount=14, encoding="utf-8"
        )
        file_handler.setFormatter(logging.Formatter(_FORMAT))
        root.addHandler(file_handler)
    except OSError as e:
        # 日志目录不可写（受限运行时挂载卷属主不对、只读文件系统等）：
        # 退化为仅控制台输出（docker logs 仍可见），不阻断服务启动
        root.warning("日志目录 %s 不可写（%s），本次运行仅输出到控制台", log_dir, e)

    # 第三方库降噪（根日志为 debug 时同步放开 SDK 调试日志）
    sdk_level = getattr(logging, level.upper(), logging.INFO)
    logging.getLogger("dingtalk_stream").setLevel(sdk_level)
    return logging.getLogger("multica_dingding")
