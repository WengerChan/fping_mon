"""布谷鸟告警接口。

布谷鸟提供“接入”和“转发”两个接口，用途不同。
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal

import requests

from .config import AppConfig


_LOG = logging.getLogger(__name__)

_RESPONSIBLE_PERSON_SEP = ","


def _now_ms() -> int:
    """当前时间的毫秒级时间戳，对应 Java 的 System.currentTimeMillis()。"""
    return int(datetime.now().timestamp() * 1000)


def _validate_priority(value: int, field_name: str) -> None:
    if value not in (1, 2, 3, 4, 5):
        raise ValueError(f"{field_name} 必须是 1..5 之间的整数，当前是 {value!r}")


def _validate_mode(value: int) -> None:
    if value not in (0, 1):
        raise ValueError(f"mode 必须是 0 或 1，当前是 {value!r}")


def _require_nonempty(value: str, field_name: str) -> None:
    if not value:
        raise ValueError(f"{field_name} 不能为空")


def _truncate_id(value: str, field_name: str) -> str:
    if len(value) > 32:
        return value[:32]
    return value


@dataclass
class CuckooReceiveMapBody:
    """布谷鸟接入接口的请求体。

    `alrmaName` 告警标题
    `alarmContent` 告警内容
    `appKey` 应用标识，又布谷鸟管理员分配
    `application` 系统名称，需保持和cmdb系统中文名一致
    `entityId` 消息序列ID，消息唯一ID，不超过32位
    `firstOccurrence` 消息首次产生时间，格式必须为"YYYY-mm-dd HH:MM:ss"
    `host` 消息所属对象，可以是IP或主机名
    `lastOccurrence` 消息最近产生时间
    `originalEventId` 外部消息原始ID，消息数据唯一ID，不超过32位
    `priority` 消息等级，1-信息、2-警告、3-次要、4-主要、5-严重
    `sourceName` 监控源名称
    `responsiblePerson` 默认责任人，消息分发时可以使用此值（使用时需要转str）
    `wechatRedirectURL` 企微跳转
    `files` 附件
    """

    alrmaName: str
    alarmContent: str
    application: str
    entityId: str
    lastOccurrence: str
    originalEventId: str
    sourceName: str
    appKey: str = ""
    firstOccurrence: str = field(
        default_factory=lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    )
    host: str = "127.0.0.1"
    priority: Literal[1, 2, 3, 4, 5] = 3
    responsiblePerson: list[str] = field(default_factory=list)
    wechatRedirectURL: str = "https://cuckoo.sdicsc.com.cn/cuckoo/event/event-activity"
    files: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for name in ("alrmaName", "alarmContent", "application", "entityId",
                     "lastOccurrence", "originalEventId", "sourceName"):
            _require_nonempty(getattr(self, name), name)
        _validate_priority(self.priority, "priority")
        self.entityId = _truncate_id(self.entityId, "entityId")
        self.originalEventId = _truncate_id(self.originalEventId, "originalEventId")
        # 责任人列表统一转字符串，避免上游误传 int 等类型导致拼接异常
        self.responsiblePerson = [str(p) for p in self.responsiblePerson]


@dataclass
class CuckooForwardBody:
    """布谷鸟转发接口的请求体。

    `ver` 版本，固定为1.0
    `appKey` 应用表示，又布谷鸟管理员分配
    `sn` 消息序列ID，消息唯一ID，不超过32位
    `rdt` 消息首次产生时间戳，System.currentTimeMillis()
    `sys` 系统名称，需保持和cmdb系统中文名一致
    `level` 消息等级，1-信息、2-警告、3-次要、4-主要、5-严重
    `alrmaName` 告警标题
    `alarmContent` 告警内容
    `mode` 处理模式，0-assign分派，1-forward转发
    `chn` 分发渠道列表（接入接口文档未公开字段名，调用方按布谷鸟约定填写）
    `wechatRedirectURL` 企微跳转
    `files` 附件
    """

    sn: str
    sys: str
    alrmaName: str
    alarmContent: str
    appKey: str = ""
    ver: float = field(default=1.0, init=False)
    rdt: int = field(default_factory=_now_ms)
    level: Literal[1, 2, 3, 4, 5] = 3
    mode: Literal[0, 1] = 1
    chn: list[dict[str, Any]] = field(default_factory=list)
    wechatRedirectURL: str = "https://cuckoo.sdicsc.com.cn/cuckoo/event/event-activity"
    files: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for name in ("sn", "sys", "alrmaName", "alarmContent"):
            _require_nonempty(getattr(self, name), name)
        _validate_priority(self.level, "level")
        _validate_mode(self.mode)
        self.sn = _truncate_id(self.sn, "sn")


class Cuckoo:
    """布谷鸟告警发送器。"""

    def __init__(self, config: AppConfig) -> None:
        self.config = config

    def payload(self, body: CuckooReceiveMapBody | CuckooForwardBody) -> dict:
        """生成布谷鸟告警的请求体。

        不会修改传入的 `body`：appKey 缺失时只在返回的 dict 里补齐。
        """
        pl = dict(body.__dict__)
        if not pl.get("appKey"):
            pl["appKey"] = self.config.cuckoo.app_key
        if isinstance(body, CuckooReceiveMapBody):
            pl["responsiblePerson"] = _RESPONSIBLE_PERSON_SEP.join(body.responsiblePerson)
        return pl

    def send_msg(self, body: CuckooReceiveMapBody | CuckooForwardBody) -> None:
        """发送布谷鸟告警。成功仅记录日志；非 2xx 与网络异常向上抛。"""
        if isinstance(body, CuckooReceiveMapBody):
            url = self.config.cuckoo.url.get("receivemap", "")
        else:
            url = self.config.cuckoo.url.get("forward", "")

        if not url:
            raise ValueError("布谷鸟 URL 未配置")

        headers = {"Content-Type": "application/json"}
        payload = self.payload(body)
        _LOG.debug("发送布谷鸟告警: url=%s payload=%s", url, payload)
        try:
            response = requests.request(
                method="POST",
                url=url,
                headers=headers,
                json=payload,
                timeout=5,
            )
            response.raise_for_status()
        except requests.RequestException as e:
            _LOG.error("发送布谷鸟告警失败: %s", e)
            raise

        _LOG.info("布谷鸟告警发送成功: %s", response.json())