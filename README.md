# fping-monitor

一个使用 Python + `fping` 实现的主机在线监控程序，适用于约 1000 台主机的规模。程序通过 HTTP 暴露延时与状态指标（Prometheus 拉取），并在主机上下线时调用外部 Webhook 接口发送告警。

完整设计稿见 [`arch.md`](./arch.md)；项目以小步提交的方式实现，每个 commit 都只做一件事，方便回溯。

## 环境要求

- Python 3.12+
- `fping` 5.x（运行探针的宿主机或容器内；容器镜像已自带）

## 本地开发

使用本地虚拟环境（`.venv`）做开发与测试。

```bash
# 如果你的网络需要代理
export http_proxy=http://127.0.0.1:10809
export https_proxy=http://127.0.0.1:10809

# 用 uv 创建虚拟环境
uv venv .venv --python 3.12

# 以可编辑模式安装项目（含 dev 测试依赖）
.venv/bin/python -m pip install -e ".[dev]"
# 或者（网络通畅时）
uv pip install -e ".[dev]"
```

## 运行

配置从 YAML 文件加载，完整字段见 `config/config.example.yaml`。

```bash
.venv/bin/python -m fping_monitor --config config/config.example.yaml
```

容器方式：

```bash
docker compose up --build
```

`docker-compose.yml` 会为容器授予 `NET_RAW` 能力以发送 ICMP，同时把根文件系统设为只读；持久卷 `monitor-data` 挂载到 `/var/lib/fping-monitor`，用于存放 SQLite 状态文件。

## 镜像发布

每次 `git push --tags vX.Y.Z`，GitHub Actions 会自动：

- 构建 linux/amd64 与 linux/arm64 多架构镜像；
- 推送到公开镜像仓库 `ghcr.io/wengerchan/fping-monitor`，并打上 `vX.Y.Z`、`vX.Y`、`vX`、`latest` 标签；
- 创建对应版本的 GitHub Release，正文取自 `CHANGELOG.md`；
- 镜像**公开**，任何人可直接 `docker pull`，无需登录。

### 必需的 GitHub Secrets

仓库 Settings → Secrets and variables → Actions 需要新增：

| Secret 名称 | 用途 | 说明 |
| ---------- | ---- | ---- |
| `GHCR_TOKEN` | 推送镜像到 `ghcr.io` | 一个具备 `write:packages` 与 `read:packages` 权限的 PAT（建议使用 GitHub App token 或 fine-grained PAT） |

`GITHUB_TOKEN` 自带的 `packages: write` 权限被 `permissions:` 显式移除，避免它意外同时被使用；只保留 `contents: write` 给创建 Release 使用。

> ⚠️ **安全提示**：泄露在外的 PAT 应当立即撤销（GitHub → Settings → Developer settings → Personal access tokens），并在仓库 secrets 中轮换。workflow 文件本身不应包含任何明文 token。

直接拉取镜像（无需登录，公开镜像）：

```bash
# 默认 amd64（公开镜像，无需 docker login）
docker run --rm --cap-add NET_RAW --read-only \
    -p 9100:9100 \
    -v "$PWD/config:/etc/fping-monitor:ro" \
    -v fping-data:/var/lib/fping-monitor \
    ghcr.io/wengerchan/fping-monitor:v1.0.0

# 多架构镜像会自动选择 host 架构（macOS arm64 / Linux amd64）
docker run --rm --platform linux/arm64 ...
```

### 验证镜像可拉取

```bash
# 1. 看 manifest list 是否包含 amd64 + arm64
docker buildx imagetools inspect ghcr.io/wengerchan/fping-monitor:v1.0.0
# 期望输出两个 manifest：linux/amd64 与 linux/arm64。

# 2. 匿名拉取（无需 docker login）
docker pull ghcr.io/wengerchan/fping-monitor:v1.0.0

# 3. 运行 /healthz 自检
docker run --rm -p 9100:9100 \
    ghcr.io/wengerchan/fping-monitor:v1.0.0 \
    --config /etc/fping-monitor/config.yaml
curl http://127.0.0.1:9100/healthz
# {"status":"ok"}
```

### 关于 `unknown/unknown` 平台条目

GHCR Web UI 在每个 tag 下可能显示 `linux/amd64`、`linux/arm64`、外加一个
`unknown/unknown`。这是 **GHCR 自身的渲染兜底**，不是镜像真的有问题：

- 实际 `docker pull` 会按你的 host 架构自动选 amd64/arm64，不会拉到 `unknown/unknown`；
- 想确认真实性，跑 `docker buildx imagetools inspect <image>`，manifest list 应当**只**
  列 amd64 + arm64 两个条目（最多 + 一个 0 byte 的 OCI 空 manifest）。

如果你在 `imagetools inspect` 输出里**确实**看到第三个真实 digest（size > 0，mediaType 为 OCI manifest），
那是一次早期失败的 workflow run 留下的孤儿 manifest。清理方法：

```bash
# 用 imagetools 重建一个干净的 manifest list，只引用 amd64 与 arm64
docker buildx imagetools create \
    -t ghcr.io/wengerchan/fping-monitor:v1.0.0 \
    ghcr.io/wengerchan/fping-monitor:v1.0.0-amd64 \
    ghcr.io/wengerchan/fping-monitor:v1.0.0-arm64
```

## 测试

```bash
.venv/bin/python -m pytest
```

## 详细文档

- 架构与代码调用链：[`docs/architecture.md`](docs/architecture.md) · [浏览器版本](docs/architecture.html)
- 部署、运维与排错指南：[`docs/usage.md`](docs/usage.md)（中文）
- 完整设计稿：[`arch.md`](arch.md)
- Prometheus 自监控告警规则示例：[`deploy/prometheus_alerts.yml`](deploy/prometheus_alerts.yml)
