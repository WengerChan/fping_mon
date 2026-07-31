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
