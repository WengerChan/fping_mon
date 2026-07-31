"""布谷鸟告警接口。

布谷鸟提供“接入”和“转发”两个接口，用途不同。
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Literal
from dataclasses import dataclass, field
import requests

from .config import AppConfig


_LOG = logging.getLogger(__name__)


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
    appKey: str = ""
    application: str
    entityId: str
    firstOccurrence: str = field(
        default_factory=lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    )
    host: str = '127.0.0.1'
    lastOccurrence: str
    originalEventId: str
    priority: Literal[1, 2, 3, 4, 5] = 3
    sourceName: str
    responsiblePerson: list[str] = field(default_factory=list)
    wechatRedirectURL: str = "https://cuckoo.sdicsc.com.cn/cuckoo/event/event-activity"
    files: dict[str, str] = field(default_factory=dict)

    def __post_init__(self):
        if len(self.entityId) > 32:
            self.entityId = self.entityId[:32]
        if len(self.originalEventId) > 32:
            self.originalEventId = self.originalEventId[:32]


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
    `wechatRedirectURL` 企微跳转
    `files` 附件
    """

    ver: float = field(default=1.0, init=False)
    appKey: str = ""
    sn: str
    rdt: int = field(
        default_factory=lambda: int(datetime.timestamp(datetime.now() * 1000))
    )
    sys: str
    level: Literal[1, 2, 3, 4, 5] = 3
    alrmaName: str
    alarmContent: str
    mode: Literal[0, 1] = 1
    chn: list[dict[str, any]] = field(default_factory=list)
    wechatRedirectURL: str = "https://cuckoo.sdicsc.com.cn/cuckoo/event/event-activity"
    files: dict[str, str] = field(default_factory=dict)


class Cuckoo:
    config: AppConfig

    def payload(self, body: CuckooReceiveMapBody | CuckooForwardBody) -> dict:
        """生成布谷鸟告警的请求体。"""
        if body.appKey == "":
            body.appKey = self.config.cuckoo.app_key
        if isinstance(body, CuckooReceiveMapBody):
            pl = body.__dict__
            pl["responsiblePerson"] = ",".join(body.responsiblePerson)
            return pl
        return body.__dict__

    def send_msg(self, body: CuckooReceiveMapBody | CuckooForwardBody) -> None:
        """发送布谷鸟告警。"""
        if isinstance(body, CuckooReceiveMapBody):
            url = self.config.cuckoo.url.get("receivemap", "")
        else:
            url = self.config.cuckoo.url.get("forward", "")
        
        if not url:
            raise ValueError("布谷鸟 URL 未配置")

        headers = {"Content-Type": "application/json"}
        payload = self.payload(body)
        try:
            _LOG.debug("发送布谷鸟告警: url=%s payload=%s", url, payload)
            response = requests.request(
                method="POST",
                url=url,
                headers=headers,
                json=payload,
                timeout=5,
            )
        except Exception as e:
            _LOG.error("发送布谷鸟告警失败: %s", e)
            raise
        
        return response.json()


