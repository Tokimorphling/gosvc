# gosvc — 多协议 Go 服务模板

一个开箱即用的 Go 服务模板：同一套领域逻辑，同时通过 **REST**、**JSON-RPC 2.0**、**gRPC** 和
**自定义 TCP（netpoll）** 暴露，自带认证、链路追踪、日志、指标、限流、时间序列存储、
优雅退出、容器化和压测工具。

传输层基于 CloudWeGo 生态（Hertz / netpoll / sonic），RPC 层用 grpc-go，并附赠一个
Kitex 服务间 RPC 示例。

## 特性

| 能力 | 说明 |
|---|---|
| 四协议同栈 | REST + JSON-RPC 2.0（HTTP 端口）、gRPC、行分隔 JSON-RPC over TCP（netpoll），共享同一 service 层 |
| 统一错误模型 | `internal/apierror` 定义与传输无关的错误类型，各协议一处映射（见下表） |
| 认证 | API Key（constant-time 比较）与 HS256 JWT，HTTP 中间件 + gRPC 拦截器共用；health/reflection 免认证 |
| 链路追踪 | OpenTelemetry OTLP/HTTP，Hertz 自研中间件 + `otelgrpc` StatsHandler，W3C TraceContext 传播 |
| 日志 | `internal/logx`：移植自 not-only-mining-pool 的 geth 风格 slog handler（彩色/对齐/调用点）；支持 terminal / json / logfmt，stdout / 轮转文件 / 双写，运行期改级别 |
| 可观测性 | Prometheus 指标（HTTP / JSON-RPC / gRPC / Go runtime）、pprof、healthz / readyz / version、时间序列查询，独立 admin 端口 |
| 时间序列存储 | Redis 分钟桶计数器 + 内存聚合批量写入（请求路径不碰 Redis），`/debug/ts` 查询 |
| 配置 | 默认值 < JSON 文件 < 环境变量，带完整校验；`Duration` 支持 `"5s"` 与秒数 |
| 生命周期 | `errgroup` + 信号处理 + 各组件优雅退出 + 就绪门 |
| 中间件 | request id、tracing、access log、panic recovery、CORS、限流、认证（HTTP 与 gRPC 共用限流器） |
| 压测器 | `cmd/bench` 支持 rest / jsonrpc / grpc，输出 QPS 与 p50/p90/p99 |
| Kitex 示例 | `examples/kitex`：Thrift IDL + 代码生成 + server/client + 静态 resolver（服务发现扩展点） |
| 工程化 | Makefile、distroless Dockerfile（二进制健康检查）、compose、CI（gofmt / vet / race）、golangci 配置、模块改名脚本 |

本地冒烟数据（Apple Silicon，loopback，10 并发 2 秒，仅供参考）：

```
rest    qps=77393.9  p50=102µs  p99=408µs
jsonrpc qps=69251.7  p50=109µs  p99=460µs
grpc    qps=52694.3  p50=153µs  p99=720µs
```

## 快速开始

```bash
make build
./bin/gosvc -c configs/config.example.json     # 或 make run
```

```bash
# REST
curl 'http://127.0.0.1:8080/api/v1/hello?name=world'
curl http://127.0.0.1:8080/api/v1/greetings/1

# JSON-RPC 2.0（支持批量与通知）
curl -s -X POST http://127.0.0.1:8080/rpc \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"greeter.sayHello","params":{"name":"rpc"}}'

# gRPC（server 已开启 reflection）
grpcurl -plaintext 127.0.0.1:9090 list
grpcurl -plaintext -d '{"name":"grpc"}' 127.0.0.1:9090 greeter.v1.Greeter/SayHello

# 自定义 TCP（启用后）：行分隔 JSON-RPC 2.0
printf '{"jsonrpc":"2.0","id":1,"method":"greeter.sayHello","params":{"name":"tcp"}}\n' | nc 127.0.0.1 7070

# 运维端点
curl http://127.0.0.1:6060/healthz
curl http://127.0.0.1:6060/metrics
curl http://127.0.0.1:6060/debug/loglevel          # GET 当前级别
curl -X PUT -d '{"level":"debug"}' http://127.0.0.1:6060/debug/loglevel
curl 'http://127.0.0.1:6060/debug/ts?metric=http.requests:/api/v1/hello&minutes=60'
```

压测：

```bash
make bench
./bin/bench -mode jsonrpc -http-addr 127.0.0.1:8080 -c 50 -d 10s
./bin/bench -mode grpc    -grpc-addr 127.0.0.1:9090 -c 50 -d 10s
```

## 目录结构

```
.
├── api/
│   ├── greeter/v1/            # protobuf IDL 与生成代码（gRPC）
│   └── kitex/                 # Thrift IDL 与 Kitex 生成代码（示例）
├── cmd/
│   ├── gosvc/                 # 服务入口（含 -healthcheck 模式）
│   └── bench/                 # 多协议压测器
├── configs/                   # 配置示例
├── deploy/                    # Dockerfile / docker-compose
├── examples/kitex/            # Kitex 可选层示例 + README
├── internal/
│   ├── app/                   # 组件装配 + 生命周期
│   ├── config/                # 配置加载、合并、校验
│   ├── logging/               # slog 装配（多 sink、格式、级别）
│   ├── logx/                  # geth 风格 slog handlers（移植自 pool 项目）
│   ├── auth/                  # API Key + JWT
│   ├── telemetry/             # OpenTelemetry 初始化
│   ├── reqid/                 # request id
│   ├── apierror/              # 传输无关错误
│   ├── health/                # readiness
│   ├── observability/         # Prometheus 指标
│   ├── ratelimit/             # 按 key 的 token bucket
│   ├── workerpool/            # 有界 goroutine 池（背压）
│   ├── store/                 # 存储接口
│   │   └── redisx/            # Redis 分钟桶 + 聚合写入
│   ├── service/greeter/       # 领域服务（与传输无关）
│   ├── transport/
│   │   ├── http/              # Hertz：REST + JSON-RPC + 中间件
│   │   ├── grpc/              # grpc-go：拦截器 + 错误映射
│   │   └── tcp/               # netpoll：行分隔 JSON-RPC + worker pool
│   ├── jsonrpc/               # JSON-RPC 2.0 协议实现（与框架无关）
│   └── admin/                 # metrics / pprof / health / loglevel / ts
├── test/e2e/                  # 端到端测试
└── Makefile
```

## 架构

```
   REST/JSON-RPC        gRPC            TCP (netpoll)
        │                │                    │
        ▼                ▼                    ▼
 ┌────────────┐   ┌────────────┐   ┌──────────────────┐
 │transport/  │   │transport/  │   │ transport/tcp    │
 │  http      │   │  grpc      │   │ EventLoop + pool │
 └─────┬──────┘   └─────┬──────┘   └────────┬─────────┘
       │                │                    │
       └────────────────┼────────────────────┘
                        ▼
              ┌──────────────────┐     ┌───────────────┐
              │ service/greeter  │────▶│ store.Recorder│──▶ Redis 分钟桶
              └────────┬─────────┘     └───────────────┘
                       ▼
              ┌──────────────────┐
              │ apierror.Kind    │
              └──────────────────┘
```

### 统一错误映射

| Kind | HTTP | JSON-RPC | gRPC |
|---|---|---|---|
| `invalid_argument` | 400 | -32602 | `InvalidArgument` |
| `unauthenticated` | 401 | -32006 | `Unauthenticated` |
| `not_found` | 404 | -32001 | `NotFound` |
| `conflict` | 409 | -32002 | `AlreadyExists` |
| `permission_denied` | 403 | -32003 | `PermissionDenied` |
| `rate_limited` | 429 | -32005 | `ResourceExhausted` |
| `unavailable` | 503 | -32004 | `Unavailable` |
| `internal` / 未知 | 500 | -32603 | `Internal` |

内部错误消息不会泄漏给客户端。

### HTTP 中间件顺序

```
RequestID → Tracing → AccessLog → Recovery → CORS → RateLimit → [Auth] → handler
```

`Auth` 通过路由组挂在 `/api/v1/*` 与 `/rpc` 上，`/healthz`、`/readyz` 保持公开。

## 日志

`internal/logx` 是 not-only-mining-pool 日志库的移植（其本身是 go-ethereum `log` 包的 slog 版），
输出形如：

```
INFO  2026-09-27T01:31:46.481Z middleware.go:47  - http request    service=gosvc env=dev version=... request_id=... method=GET route=/api/v1/hello status=200
```

**结构化日志怎么落盘？** 推荐默认 **stdout 输出 JSON，由平台收集**（Docker/k8s/systemd 负责轮转），
进程自己写文件只适合裸机部署。本模板两种都支持：

```json
"log": {
  "level": "info",          // trace|debug|info|warn|error
  "format": "terminal",     // auto|terminal|json|logfmt（auto：dev→terminal，其它→json）
  "output": "stdout",       // stdout|file|both
  "color": "auto",          // auto|always|never
  "addSource": false,       // JSON sink 是否带源码位置
  "file": {
    "path": "logs/gosvc.log",
    "maxSizeMB": 100,
    "maxBackups": 5,
    "maxAgeDays": 7,
    "compress": true
  }
}
```

- `output: "both"` 时，**终端用彩色 terminal 格式，文件固定 JSON**（便于采集与检索），由 lumberjack 轮转；
- 运行期改级别：`PUT /debug/loglevel {"level":"debug"}`（终端与文件 sink 同步生效）；
- 所有请求日志带 `request_id`（HTTP 响应头回写 `X-Request-ID`，gRPC 通过 metadata 传播）；
- Hertz 内部日志通过 `hlog.FullLogger` 适配器汇入同一 logger。

> 说明：`internal/logx` 只移植了 handler/format 部分（未包含原项目的 glog verbosity 与
> `Crit=os.Exit` 行为），级别控制改由 `slog.LevelVar` + admin 接口提供。

## 认证

```json
"auth": {
  "enabled": true,
  "apiKeys": ["key-1", "key-2"],
  "jwt": { "secret": "at-least-16-chars", "issuer": "gosvc", "audience": "" }
}
```

```bash
# API Key
curl -H 'X-API-Key: key-1' http://127.0.0.1:8080/api/v1/info
# JWT（HS256）
curl -H "Authorization: Bearer $TOKEN" http://127.0.0.1:8080/api/v1/info
# gRPC
grpcurl -plaintext -H 'x-api-key: key-1' -d '{"name":"x"}' 127.0.0.1:9090 greeter.v1.Greeter/SayHello
```

密钥可用环境变量注入，避免写进配置文件：`GOSVC_AUTH_API_KEYS=key-1,key-2`（自动开启认证）。

## 链路追踪

```json
"telemetry": { "enabled": true, "otlpEndpoint": "127.0.0.1:4318", "insecure": true, "sampleRatio": 1.0 }
```

或 `GOSVC_OTLP_ENDPOINT=127.0.0.1:4318`。启用后：HTTP 每个请求一个 server span（W3C TraceContext
提取/注入），gRPC 使用 `otelgrpc` StatsHandler，服务名/版本/环境写入 resource。

## TCP 传输（netpoll）

```json
"tcp": { "enabled": true, "host": "0.0.0.0", "port": 7070, "workers": 4, "queueSize": 1024, "maxFrameBytes": 1048576, "readTimeout": "60s", "shutdownTimeout": "5s" }
```

- 协议：**行分隔 JSON-RPC 2.0**，与 HTTP 的 `/rpc` 完全同一套方法；
- 读取在 netpoll EventLoop（每连接串行 OnRequest），业务投递到**有界 worker pool**：
  队列满立即返回 `-32004 server busy`，不会无限堆积；
- 写路径按连接加锁（netpoll 要求同一连接写串行化），帧超限直接断开；
- **不做 TLS**：请在网关（nginx/HAProxy）终止 TLS 与 PROXY protocol，内网明文；
- 该传输也写入 JSON-RPC 指标与 `tcp.requests` 时间序列。

## 时间序列存储（Redis）

```json
"storage": { "redis": { "enabled": true, "addr": "127.0.0.1:6379", "prefix": "gosvc", "bucketTtl": "25h", "queueSize": 4096 } }
```

或 `GOSVC_REDIS_ADDR=127.0.0.1:6379`。

- 请求路径只做 `recorder.Incr`（非阻塞入队），后台单写者聚合后按**分钟桶**批量写 Redis：
  `{prefix}:ts:{metric}:{yyyyMMddHHmm}`，带 TTL；
- 查询：`GET /debug/ts?metric=http.requests:/api/v1/hello&minutes=60`；
- 为什么不用 ZSET 全量扫描？见 `internal/store/redisx/store.go` 的注释——这是从原挖矿池项目
  吸取的教训（大时间窗全量拉取会拖垮 Redis）。

## 测试与压测

```bash
make test      # 单元测试
make race      # -race 全量（CI 同款）
make lint      # gofmt + go vet
make proto     # 重新生成 protobuf
make kitex     # 重新生成 Kitex 示例代码
```

- `internal/jsonrpc`：协议层单测（批量、通知、错误码、参数校验）；
- `internal/auth`：API Key / JWT（含错误密钥、错误算法、过期）；
- `internal/logx`：terminal 格式、级别过滤、格式/级别解析；
- `internal/workerpool`：执行、背压、关闭、panic 恢复；
- `internal/store/redisx`：基于 miniredis 的桶读写与聚合刷新；
- `test/e2e`：真实启动全部端口，覆盖 REST、JSON-RPC、TCP、gRPC、认证（HTTP+gRPC）、
  metrics、运行期日志级别、优雅关闭；
- `examples/kitex`：真实起 Kitex server，client 分别用 host-ports 与静态 resolver 调用。

## 部署

```bash
make docker
docker compose -f deploy/docker-compose.yml up --build
```

- distroless + 非 root；健康检查复用二进制：`gosvc -healthcheck http://127.0.0.1:6060/healthz`；
- **admin 端口只应暴露在内网**（默认绑定 `127.0.0.1`）；
- 生产建议：网关终止 TLS；Redis 使用独立实例或 Sentinel/Cluster（当前为单机客户端，替换
  `redis.NewClient` 即可接入集群客户端）。

## 设计取舍

- **Hertz** 作为 HTTP 层：性能好、中间件生态完整，业务通过 `internal/transport/http` 隔离；
- **sonic** 作为 JSON 编解码：热路径零拷贝/JIT，非 amd64/arm64 自动退化；
- **gRPC 用 grpc-go 而不是 Kitex**：对外契约只需服务端 + 拦截器，grpc-go 生态最稳；
  服务间 RPC 场景见 `examples/kitex`（可选层，核心不依赖）；
- **标准库 `slog`** 作为日志门面：Hertz `hlog`、Kitex `klog` 都能通过适配器接入；
- **JSON-RPC 只支持具名参数**：定位参数明确拒绝（-32602）；
- **netpoll 只在 TCP 传输使用**：HTTP/gRPC 不需要为连接数优化，保持生态兼容。

## Roadmap

- [x] 认证与鉴权中间件（API Key / JWT）
- [x] OpenTelemetry tracing
- [x] Kitex 可选层示例（服务间 RPC + 服务发现扩展点）
- [x] 自定义 TCP 协议示例（netpoll EventLoop + 有界 worker pool）
- [x] Redis 存储与时间序列指标
- [x] geth 风格日志（terminal/JSON/logfmt + 轮转文件 + 运行期级别）
- [ ] 配置热更新（fsnotify + atomic 快照）
- [ ] Redis Cluster / Sentinel 客户端
- [ ] 日志采样（高 QPS 下 debug 降噪）
- [ ] 前端静态资源 embed 示例

## 重命名模块

```bash
scripts/rename-module.sh github.com/you/your-service
# 然后更新 api/greeter/v1/greeter.proto 与 api/kitex/echo.thrift 的包路径并重新生成：
make proto kitex
```
