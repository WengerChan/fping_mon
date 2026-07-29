"""日志辅助。

日志统一输出到 stdout，方便 Docker / Podman 收集。本模块只负责
初始化根 logger，业务代码使用 logging.getLogger(__name__) 即可。
"""

from __future__ import annotations

import logging
import os
import sys


_CONFIGURED = False


def configure_logging(level: str | None = None) -> None:
    """初始化根 logger。可以重复调用，不会重复挂 handler。"""
    global _CONFIGURED
    if _CONFIGURED:
        return
    if level is None:
        level = os.environ.get("FPING_MONITOR_LOG_LEVEL", "INFO")
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s %(levelname)s %(name)s %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S%z",
        )
    )
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)
    _CONFIGURED = True
