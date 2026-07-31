"""配置加载模块的测试。"""

from pathlib import Path

import pytest

from fping_monitor.config import ConfigError, load_config


VALID_YAML = """
server:
  listen: 127.0.0.1
  port: 9101

probe:
  interval_seconds: 5
  timeout_ms: 500
  packets: 2
  batch_size: 50

state:
  down_after_failures: 3
  up_after_successes: 3
  mass_failure_ratio: 0.4

webhook:
  enabled: true
  url: https://alert.example.invalid/api
  timeout_seconds: 3
  max_attempts: 5
  max_backoff_seconds: 30
  token_env: TOKEN
  monitor_instance: m1

storage:
  path: /tmp/state.db

targets:
  - id: host-a
    address: 10.0.0.1
    labels:
      site: shanghai
      group: web
  - id: host-b
    address: host-b.internal
    labels:
      env: prod
"""


def _write(tmp_path: Path, body: str) -> Path:
    p = tmp_path / "config.yaml"
    p.write_text(body, encoding="utf-8")
    return p


def test_load_valid_config(tmp_path: Path):
    path = _write(tmp_path, VALID_YAML)
    cfg = load_config(path)
    assert cfg.server.port == 9101
    assert cfg.probe.interval_seconds == 5
    assert cfg.state.down_after_failures == 3
    assert cfg.state.up_after_successes == 3
    assert cfg.webhook.url == "https://alert.example.invalid/api"
    assert cfg.webhook.monitor_instance == "m1"
    assert len(cfg.targets) == 2
    assert cfg.targets[0].id == "host-a"
    assert cfg.targets[0].labels == {"site": "shanghai", "group": "web"}
    assert cfg.targets[1].address == "host-b.internal"


def test_missing_file(tmp_path: Path):
    with pytest.raises(ConfigError):
        load_config(tmp_path / "nope.yaml")


def test_duplicate_target_id(tmp_path: Path):
    body = """
targets:
  - { id: dup, address: 10.0.0.1 }
  - { id: dup, address: 10.0.0.2 }
"""
    with pytest.raises(ConfigError, match="重复"):
        load_config(_write(tmp_path, body))


def test_invalid_target_id(tmp_path: Path):
    body = """
targets:
  - { id: "-bad", address: 10.0.0.1 }
"""
    with pytest.raises(ConfigError, match="不匹配"):
        load_config(_write(tmp_path, body))


def test_disallowed_label(tmp_path: Path):
    body = """
targets:
  - id: host-x
    address: 10.0.0.1
    labels:
      region: cn-east
"""
    with pytest.raises(ConfigError, match="白名单"):
        load_config(_write(tmp_path, body))


def test_invalid_address(tmp_path: Path):
    body = """
targets:
  - id: host-x
    address: "-not-a-host"
"""
    with pytest.raises(ConfigError, match="合法的 IP"):
        load_config(_write(tmp_path, body))


def test_invalid_thresholds(tmp_path: Path):
    body = """
state:
  down_after_failures: 0
"""
    with pytest.raises(ConfigError, match="down_after_failures"):
        load_config(_write(tmp_path, body))


def test_empty_config(tmp_path: Path):
    with pytest.raises(ConfigError, match="为空"):
        load_config(_write(tmp_path, ""))


def test_webhook_url_must_be_http(tmp_path: Path):
    body = """
webhook:
  url: "ftp://example.invalid/hook"
"""
    with pytest.raises(ConfigError, match="http"):
        load_config(_write(tmp_path, body))


def test_webhook_url_missing_host(tmp_path: Path):
    body = """
webhook:
  url: "https://"
"""
    with pytest.raises(ConfigError, match="主机名"):
        load_config(_write(tmp_path, body))


# ---- address 展开（CIDR / 区间）----

def test_target_address_cidr_24_expands(tmp_path: Path):
    """10.1.1.0/24 应展开为 254 个 host（丢 .0 与 .255）。"""
    body = """
targets:
  - id: subnet
    address: 10.1.1.0/24
"""
    cfg = load_config(_write(tmp_path, body))
    assert len(cfg.targets) == 254
    # id 形如 subnet-1.1 ... subnet-1.254（末两段，避免跨 /16 撞名）
    assert cfg.targets[0].id == "subnet-1.1"
    assert cfg.targets[0].address == "10.1.1.1"
    assert cfg.targets[-1].id == "subnet-1.254"
    assert cfg.targets[-1].address == "10.1.1.254"
    # .0 和 .255 不应出现
    assert "10.1.1.0" not in {t.address for t in cfg.targets}
    assert "10.1.1.255" not in {t.address for t in cfg.targets}


def test_target_address_cidr_30_keeps_all(tmp_path: Path):
    """10.1.1.0/30 全保留 4 个地址（点对点/小型子网场景）。"""
    body = """
targets:
  - id: p2p
    address: 10.1.1.0/30
"""
    cfg = load_config(_write(tmp_path, body))
    assert {t.address for t in cfg.targets} == {"10.1.1.0", "10.1.1.1", "10.1.1.2", "10.1.1.3"}


def test_target_address_range_closed(tmp_path: Path):
    """区间语法：闭区间，包含两端。"""
    body = """
targets:
  - id: rng
    address: 10.1.1.100-10.1.1.105
"""
    cfg = load_config(_write(tmp_path, body))
    assert len(cfg.targets) == 6
    assert cfg.targets[0].address == "10.1.1.100"
    assert cfg.targets[0].id == "rng-1.100"
    assert cfg.targets[-1].address == "10.1.1.105"
    assert cfg.targets[-1].id == "rng-1.105"


def test_target_address_range_reversed_rejected(tmp_path: Path):
    body = """
targets:
  - id: bad
    address: 10.1.1.200-10.1.1.100
"""
    with pytest.raises(ConfigError, match="起点"):
        load_config(_write(tmp_path, body))


def test_target_address_expansion_limit(tmp_path: Path):
    """展开数超过 max_expansion 必须 ConfigError。"""
    body = """
max_expansion: 4
targets:
  - id: big
    address: 10.1.1.0/24
"""
    with pytest.raises(ConfigError, match="max_expansion"):
        load_config(_write(tmp_path, body))


def test_target_address_max_expansion_configurable(tmp_path: Path):
    """显式调高 max_expansion 后可接受更大段。"""
    body = """
max_expansion: 1024
targets:
  - id: big
    address: 10.1.0.0/22
"""
    cfg = load_config(_write(tmp_path, body))
    assert len(cfg.targets) > 512  # /22 展开为 ~1022
    assert cfg.max_expansion == 1024


def test_target_address_collision_on_expansion(tmp_path: Path):
    """两个 CIDR 展开后地址冲突时（id 拼接去重），仍要报错。

    a /24 展开含 10.1.1.1（id=a-1.1），单独写 a-1.1 作为基础 id 应被拒绝。
    """
    body = """
targets:
  - id: a
    address: 10.1.1.0/24
  - id: a-1.1
    address: 10.1.2.1
"""
    with pytest.raises(ConfigError, match="重复"):
        load_config(_write(tmp_path, body))
