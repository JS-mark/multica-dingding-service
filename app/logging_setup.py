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

    Path(log_dir).mkdir(parents=True, exist_ok=True)
    file_handler = TimedRotatingFileHandler(
        Path(log_dir) / "app.log", when="midnight", backupCount=14, encoding="utf-8"
    )
    file_handler.setFormatter(logging.Formatter(_FORMAT))
    root.addHandler(file_handler)

    # 第三方库降噪（根日志为 debug 时同步放开 SDK 调试日志）
    sdk_level = getattr(logging, level.upper(), logging.INFO)
    logging.getLogger("dingtalk_stream").setLevel(sdk_level)
    return logging.getLogger("multica_dingding")
