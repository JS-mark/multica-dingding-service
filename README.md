# multica-dingding-service

把 [multica](https://github.com/multica-ai/multica) CLI 封装成 REST 服务，对接**钉钉机器人**：服务是纯粹的**多对一中转** —— 多个白名单用户（钉钉/REST 各自独立会话）都路由到**同一个 multica agent**，回复以 **AI 卡片流式打字机**效果呈现。multica 服务端地址完全由配置决定（官方云或自部署实例均可）。

```
钉钉用户A ─┐                                    ┌─▶ multica 服务端（可配置）
钉钉用户B ─┼─WebSocket(Stream)─▶ 本服务 ──CLI───┤      workspace / issue / agent
REST调用 ──┘         │                          └─▶ agent 绑定的 runtime 执行 run
                     └── AI 卡片流式更新 / SSE ◀── run-messages 增量轮询
```

**技术栈**：Python 3.12 · FastAPI · dingtalk-stream（官方 SDK）· SQLite · Docker

---

## 架构说明

- **会话模型**：每个用户一个长期会话，对应 multica 里的一个 issue。首条消息 = issue 描述，后续消息 = issue 评论，agent 在评论线程回复。`/new`（钉钉）或 `POST /api/sessions/{key}/reset`（REST）可重置。
- **流式实现**：轮询 `multica issue run-messages --since <seq>` 拿增量消息（思考/工具/正文），节流推送到钉钉 AI 卡片（打字机效果）；REST 侧直接以 **SSE** 流式返回。
- **权限**：钉钉白名单（staffId / userId / 昵称），**白名单为空 = 拒绝所有人**；REST 可选 `X-API-Key` 鉴权。
- **agent 执行位置**：agent 由你机器上的 `multica daemon` 在本地 runtime（Claude/Codex 等）执行。容器内的服务只调 api.multica.ai 的 CLI 命令，**daemon 必须在你的机器上保持运行**（`multica daemon status` 查看）。

## 目录结构与持久化

```
├── app/                 # 服务代码
├── config/
│   ├── config.yaml      # 服务配置（从 config.example.yaml 复制）
│   └── multica-home/    # multica 认证（挂载进容器 HOME/.multica）
├── data/                # SQLite 会话库（重启不丢）
└── logs/                # 日志（按天轮转，保留 14 天）
```

`config/`、`data/`、`logs/` 均为 Docker 卷映射，容器重启数据不丢。

> **Docker 运行时不支持宿主机目录映射（bind mount）时**——如 rootless Docker、远程 daemon、部分托管容器环境——用 [named volume 模式](#三启动)，配置全部走环境变量，数据保存在 Docker 管理的卷里。

---

## 一、准备

### 1. multica 认证与服务器地址

服务通过 multica CLI 访问你的 workspace，**服务器地址完全在配置里**：

- 官方云：`multica.server_url` 留空（profile 默认 `https://api.multica.ai`）
- 自部署实例：`multica.server_url: http://xxxx.xxx.xxx.xxx`（你的实例）

认证方式 —— 把 token 放进 `config/multica-home/profiles/<profile>/config.json`：

```bash
# 方式一：已有本机认证，直接复制（token 必须是目标服务端签发的）
mkdir -p config/multica-home/profiles
cp -R ~/.multica/profiles/<profile> config/multica-home/profiles/
rm -f config/multica-home/profiles/<profile>/daemon*   # 去掉本机 daemon 运行态文件

# 方式二：对新服务端登录（PAT token），会写入 ~/.multica/profiles/<server-host>/
multica --server-url http://xxxx.xxx.xxx.xxx login --token mul_xxxxxxxx
cp -R ~/.multica/profiles/xxxx.xxx.xxx.xxx config/multica-home/profiles/
```

`multica.profile` 与目录名保持一致。config.json 内容形如：

```json
{"server_url": "http://xxxx.xxxx.xxxx.xxxx", "token": "mul_..."}
```

> 无法挂载 `multica-home` 目录的运行时（named volume 模式），改用环境变量注入：`.env` 里设 `MDS_MULTICA_TOKEN`（自部署再加 `MDS_MULTICA_SERVER_URL`），首次启动会自动生成上面的 config.json 到容器内 `~/.multica`，之后可移除该 token。

查 workspace 和 agent ID：

```bash
multica workspace list --output json
multica --workspace-id <ws-id> agent list --output json
```

### 2. 钉钉机器人（Stream 模式）

1. 登录[钉钉开放平台](https://open-dev.dingtalk.com) → 创建企业内部应用，记下 **AppKey / AppSecret**（→ 配置 `dingtalk.client_id / client_secret`）。
2. 应用「添加应用能力」→ 勾选 **机器人**，消息接收模式选 **Stream 模式**。
3. 发布应用版本并审核通过。
4. 白名单：让白名单里的人给机器人发一条消息，从服务日志 `拒绝未授权用户: staff=xxx` 中拿到 **senderStaffId**，填入 `dingtalk.allowed_staff_ids`。（或先用 REST `/api/chat` 验证服务，再配钉钉。）

### 3. AI 卡片模板（可选）

默认使用 dingtalk-stream SDK **内置的通用 AI Markdown 卡片模板**，零配置即可获得打字机效果。如需自定义卡片样式，在开放平台「卡片平台」创建模板，把模板 ID 填入 `dingtalk.card_template_id`。

---

## 二、配置

```bash
cp config/config.example.yaml config/config.yaml
cp .env.example .env
```

编辑 `config/config.yaml`（或 `.env`，环境变量优先）：

| 配置项 | 说明 |
|---|---|
| `multica.server_url` | multica 服务端（留空=官方云 `api.multica.ai`；自部署填实例地址） |
| `multica.workspace_id` | workspace UUID |
| `multica.agent_id` | 对话用的 agent UUID（多对一：所有白名单用户都路由到它） |
| `multica.profile` | 认证 profile 名（与 multica-home 目录一致） |
| `multica.token` | PAT token，启动时自动写入认证文件（无目录映射模式用，见上） |
| `dingtalk.client_id/secret` | 钉钉 AppKey/AppSecret |
| `dingtalk.allowed_staff_ids` | 白名单（senderStaffId，推荐） |
| `api.api_key` | REST 鉴权（留空 = 不鉴权） |

所有配置都有对应 `MDS_*` 环境变量（优先级更高）。白名单的环境变量为逗号分隔：`MDS_DINGTALK_ALLOWED_STAFF_IDS` / `MDS_DINGTALK_ALLOWED_USER_IDS` / `MDS_DINGTALK_ALLOWED_NICKS`。

## 三、启动

### Docker（推荐）

```bash
docker compose up -d --build
docker compose logs -f
```

健康检查：`curl http://127.0.0.1:8300/health`

#### 运行时不支持宿主机目录映射（named volume 模式）

rootless Docker、远程 daemon、部分托管容器环境等不支持 bind mount（`./config`、`./data`、`./logs`）时，叠加 `docker-compose.named-volumes.yml`，把目录映射换成 Docker 管理的 named volume：

```bash
docker compose -f docker-compose.yml -f docker-compose.named-volumes.yml up -d --build
```

差异：

- **配置**：不能挂 `config/config.yaml`，全部走 `.env` 环境变量（含白名单 `MDS_DINGTALK_ALLOWED_STAFF_IDS` 等）
- **multica 认证**：`.env` 里设 `MDS_MULTICA_TOKEN`（自部署加 `MDS_MULTICA_SERVER_URL`），首次启动自动写入 `multica_home` 卷；写入后可移除 token
- **数据位置**：会话库/日志在 named volume（`docker volume ls` 可见，`multica-dingding_multica_data` 等），容器重建不丢；如需查看/备份：

```bash
# 会话库/日志
docker run --rm -v multica-dingding_multica_data:/data -v "$PWD":/backup alpine \
  cp /data/sessions.db /backup/
# 进容器看日志
docker compose -f docker-compose.yml -f docker-compose.named-volumes.yml exec multica-dingding tail -f logs/app.log
```

### 本地开发

```bash
uv venv && uv pip install -r requirements.txt   # 或 pip
python -m app.main
```

> 本地开发时把 `multica.binary` 指向本机 CLI 路径即可（如 Multica.app 内置二进制）。

---

## 四、REST API

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/health` | 健康检查（免鉴权） |
| GET | `/api/info` | 配置摘要 |
| POST | `/api/chat` | 发消息，**SSE 流式**返回 |
| GET | `/api/sessions` | 会话列表 |
| GET | `/api/sessions/{key}/messages` | 会话最近消息 |
| POST | `/api/sessions/{key}/reset` | 重置会话 |
| GET | `/api/agents` | workspace agent 列表 |

除 `/health` 外，若配置了 `api_key`，请求需带 `X-API-Key` 头。

SSE 聊天示例：

```bash
curl -N -X POST http://127.0.0.1:8300/api/chat \
  -H 'Content-Type: application/json' \
  -d '{"message":"你好","session_id":"mark"}'
```

事件类型（`data: {"type": ...}`）：

- `meta` — 会话/issue/run 元信息
- `status` — 过程状态（💭 思考 / 🔧 工具调用）
- `delta` — 回答正文增量
- `done` — 结束（`answer` 为完整回复）
- `error` — 失败

## 五、并发模型

- **同一用户串行**：同一会话的消息依次处理（保证上下文顺序），排队时卡片/SSE 显示 `上一条消息处理中，本条排队… 已等 Ns`
- **多用户并发**：不同用户/不同 session 同时执行，互不阻塞
- **全局上限**：最多 `multica.max_concurrent_rounds`（默认 12）轮同时执行，超出排队并显示进度；到达上限说明 agent runtime 或服务达到容量瓶颈，可调大（注意 agent 侧 `max_concurrent_tasks` 也要配套）
- **单会话排队上限**：`multica.max_session_queue`（默认 5）—— 同一用户最多 5 条消息同时排队，超出直接拒绝并提示，防止消息无限堆积
- **排队等待上限**：`run_wait_timeout`（默认 300s）内 run 未出现则报错提示
- **轮询抖动**：多轮并发时轮询节奏带随机抖动，避免 CLI 子进程惊群
- 单实例设计。多实例部署需共享 SQLite 且各自持有会话锁，不推荐

## 六、钉钉内使用

直接给机器人发消息即可。会话内命令：

- `/new` — 新会话（清空上下文）
- `/issue` — 查看当前会话绑定的 issue
- `/help` — 帮助

## 七、运维要点

- **daemon 必须在线**：agent 实际在你本机（或任一注册了 runtime 的机器）的 `multica daemon` 上执行。`multica daemon start` / `multica daemon autostart enable`。
- **日志**：`logs/app.log`（容器内映射到宿主机）；目录不可写时自动退化为仅控制台输出（`docker compose logs -f` 仍可见）。
- **PermissionError 排查**：若启动报 `Permission denied: 'logs'` / `sessions.db`，多为旧镜像或旧卷属主是 root（Dockerfile 已预建目录并归属 uid 10001，重新 `--build` 即可）。若 named volume 在旧镜像时期已被初始化为 root 属主，重建镜像不会自动修复，需一次性处理：

```bash
docker compose -f docker-compose.yml -f docker-compose.named-volumes.yml down
# 日志卷直接删（无价值数据）
docker volume rm multica-dingding-service_multica_logs
# 数据/认证卷有数据，改为修正属主
docker run --rm -v multica-dingding-service_multica_data:/d alpine chown -R 10001:10001 /d
docker run --rm -v multica-dingding-service_multica_home:/d alpine chown -R 10001:10001 /d
```
- **CLI 版本**：Dockerfile 的 `MULTICA_VERSION` 构建参数控制，默认与 `api.multica.ai` 匹配的 0.10.0；国内构建慢可用 `--build-arg MULTICA_DOWNLOAD_URL=<镜像前缀>`。
- **升级 agent**：直接改 `multica.agent_id` 后 `docker compose restart`。
