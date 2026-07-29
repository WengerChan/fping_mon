#!/usr/bin/env python3
"""从 CHANGELOG.md 抽取指定版本的段落，写入 /tmp/release_body.md。

CI 用法：
    python3 scripts/ci/extract_changelog.py <version>
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CHANGELOG = ROOT / "CHANGELOG.md"
OUT = Path("/tmp/release_body.md")


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: extract_changelog.py <version>", file=sys.stderr)
        return 2
    version = sys.argv[1]
    text = CHANGELOG.read_text(encoding="utf-8")

    # 匹配 "## [<version>] ..." 段落，到下一个 "## [" 或文件末尾为止
    esc = re.escape(version)
    pattern = (
        r"^## \[(?:"
        + esc
        + r"|"
        + re.escape(version.split(".")[0])
        + r")\] .*?(?=^## \[|\Z)"
    )
    m = re.search(pattern, text, re.MULTILINE | re.DOTALL)
    if not m:
        print("not found")
        return 1

    body = m.group(0).rstrip() + (
        "\n\n镜像：`ghcr.io/wengerchan/fping-monitor:"
        + version
        + "`（push tag `v"
        + version
        + "` 时去掉 v 前缀）"
    )
    OUT.write_text(body, encoding="utf-8")
    print("found")
    return 0


if __name__ == "__main__":
    sys.exit(main())