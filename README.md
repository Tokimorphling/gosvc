# gosvc — 多协议 Go 服务模板

一个开箱即用的 Go 服务模板：同一套领域逻辑，同时通过 **REST**、**JSON-RPC 2.0** 和 **gRPC** 暴露，
自带日志、指标、链路级 request id、限流、优雅退出、容器化和压测工具。

基于 CloudWeGo 生态构建（Hertz / netpoll / sonic），适合作为新服务的起点。

## 特性

| 能力 | 说明 |
|---|---|
| 三协议同栈 | REST + JSON-RPC 2.0（同一 HTTP 端口）、gRPC（独立端口），共享同一个 service 层 |
| 统一错误模型 | `internal/apierror` 定义与传输无关的错误类型，三种协议各有一处映射（见下表） |
| 结构化日志 | 标准库 `slog`（JSON/Text），request id 注入，Hertz 内部日志通过适配器汇入同一 logger |
| 可观测性 | Prometheus 指标（HTTP / JSON-RPC / gRPC / Go runtime）、pprof、healthz / readyz / version，独立 admin 端口 |
| 配置 | 默认值 < JSON 文件 < 环境变量，带校验；`Duration` 支持 `"5s"` 与秒数两种写法 |
| 生命周期 | `errgroup` + 信号处理 + 各组件优雅退出 + 就绪门（readiness gate） |
| 中间件 | request id、access log、panic recovery、CORS、按客户端限流（HTTP 与 gRPC 共用同一个 limiter） |
| 压测器 | `cmd/bench` 支持 rest / jsonrpc / grpc 三种模式，输出 QPS 与 p50/p90/p99 |
| 工程化 | Makefile、多阶段 Dockerfile（distroless + 二进制健康检查）、compose、GitHub Actions（gofmt / vet / race）、golangci 配置、模块改名脚本 |

本地冒烟数据（Apple Silicon，loopback，10 并发 2 秒，仅供参考）：

```
rest    qps=77393.9  p50=102µs  p99=408µs
jsonrpc qps=69251.7  p50=109µs  p99=460µs
grpc    qps=52694.3  p50=153µs  p99=720µs
```

## 快速开始

```bash
make build
./bin/gosvc -c configs/config.example.json
```

或者 `make run`。启动后：

```bash
# REST
curl 'http://127.0.0.1:8080/api/v1/hello?name=world'
curl http://127.0.0.1:8080/api/v1/greetings/1
curl http://127.0.0.1:8080/api/v1/info

# JSON-RPC 2.0
curl -s -X POST http://127.0.0.1:8080/rpc \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"greeter.sayHello","params":{"name":"rpc"}}'

# JSON-RPC 批量 + 通知（通知不产生响应）
curl -s -X POST http://127.0.0.1:8080/rpc \
  -H 'Content-Type: application/json' \
  -d '[{"jsonrpc":"2.0","id":2,"method":"greeter.getGreeting","params":{"id":2}},
       {"jsonrpc":"2.0","method":"greeter.info"}]'

# gRPC（需要 grpcurl；server 已开启 reflection）
grpcurl -plaintext 127.0.0.1:9090 list
grpcurl -plaintext -d '{"name":"grpc"}' 127.0.0.1:9090 greeter.v1.Greeter/SayHello

# 运维端点（独立 admin 端口）
curl http://127.0.0.1:6060/healthz
curl http://127.0.0.1:6060/readyz
curl http://127.0.0.1:6060/metrics
curl http://127.0.0.1:6060/debug/pprof/
```

压测：

```bash
make bench                                    # rest 模式
./bin/bench -mode jsonrpc -http-addr 127.0.0.1:8080 -c 50 -d 10s
./bin/bench -mode grpc    -grpc-addr 127.0.0.1:9090 -c 50 -d 10s
```

## 目录结构

```
.
├── api/greeter/v1/            # protobuf IDL 与生成代码（gRPC 契约）
├── cmd/
│   ├── gosvc/                 # 服务入口（含 -healthcheck 模式）
│   └── bench/                 # 多协议压测器
├── configs/                   # 配置示例
├── deploy/                    # Dockerfile / docker-compose
├── internal/
│   ├── app/                   # 组件装配 + 生命周期（Run）
│   ├── config/                # 配置加载、合并、校验
│   ├── logging/               # slog 初始化 + context 传递
│   ├── reqid/                 # request id 生成与传递
│   ├── apierror/              # 传输无关的错误类型
│   ├── health/                # readiness 标志
│   ├── observability/         # Prometheus 指标
│   ├── ratelimit/             # 按 key 的 token bucket
│   ├── service/greeter/       # 领域服务（示例，与传输无关）
│   ├── transport/
│   │   ├── http/              # Hertz：REST + JSON-RPC + 中间件 + hlog 适配
│   │   └── grpc/              # gRPC server + 拦截器 + 错误映射
│   ├── jsonrpc/               # JSON-RPC 2.0 协议实现（与框架无关）
│   └── admin/                 # 运维端口：metrics / pprof / health
├── test/e2e/                  # 端到端测试（三协议 + 关闭流程）
├── scripts/rename-module.sh   # 模块改名脚本
└── Makefile
```

## 架构

```
        REST            JSON-RPC 2.0           gRPC
         │                   │                  │
         ▼                   ▼                  ▼
 ┌───────────────┐   ┌───────────────┐   ┌───────────────┐
 │ transport/http│   │ internal/     │   │ transport/grpc│
 │  (Hertz)      │──▶│   jsonrpc     │   │  (grpc-go)    │
 └───────┬───────┘   └───────┬───────┘   └───────┬───────┘
         │                   │                   │
         └───────────────────┼───────────────────┘
                             ▼
                  ┌────────────────────┐
                  │ service/greeter    │  ← 领域逻辑只写一遍
                  │ (transport-agnostic)│
                  └─────────┬──────────┘
                            ▼
                  ┌────────────────────┐
                  │ apierror.Kind      │  ← 统一错误分类
                  └────────────────────┘
```

### 统一错误映射

领域层只返回 `*apierror.Error`，各协议在唯一的位置完成映射：

| Kind | HTTP | JSON-RPC | gRPC |
|---|---|---|---|
| `invalid_argument` | 400 | -32602 | `InvalidArgument` |
| `not_found` | 404 | -32001 | `NotFound` |
| `conflict` | 409 | -32002 | `AlreadyExists` |
| `permission_denied` | 403 | -32003 | `PermissionDenied` |
| `rate_limited` | 429 | -32005 | `ResourceExhausted` |
| `unavailable` | 503 | -32004 | `Unavailable` |
| `internal` / 未知 | 500 | -32603 | `Internal` |

内部错误的消息不会泄漏给客户端（`apierror.ClientMessage` 统一替换为 `internal server error`）。

### HTTP 中间件顺序

```
RequestID → AccessLog → Recovery → CORS → RateLimit → 业务 handler
```

- `AccessLog` 在 `Recovery` 外层：panic 被捕获后仍会记录一条 500 访问日志；
- 每个请求的 logger 带 `request_id`，响应头回写 `X-Request-ID`。

### 新增一个 API 的三步

以增加 `greeter.ping` 为例：

1. **领域层** `internal/service/greeter/service.go`：新增 `Ping(ctx) (*Pong, error)`；
2. **REST** `internal/transport/http/router.go`：`h.GET("/api/v1/ping", ...)`；
3. **JSON-RPC** 同文件 `registerJSONRPCMethods`：`s.dispatcher.Register("greeter.ping", ...)`；
4. **gRPC**：在 `api/greeter/v1/greeter.proto` 增加 rpc → `make proto` → 在 `internal/transport/grpc/greeter.go` 实现。

## 配置参考

```json
{
  "service": { "name": "gosvc", "env": "dev" },
  "http": {
    "host": "0.0.0.0", "port": 8080,
    "readTimeout": "10s", "writeTimeout": "10s", "idleTimeout": "60s",
    "shutdownTimeout": "10s", "maxBodyBytes": 1048576
  },
  "grpc": { "host": "0.0.0.0", "port": 9090, "shutdownTimeout": "10s" },
  "admin": { "host": "127.0.0.1", "port": 6060 },
  "log": { "level": "info", "format": "json", "addSource": false },
  "limiter": { "rps": 0, "burst": 0 }
}
```

环境变量覆盖（优先级最高）：

| 变量 | 作用 |
|---|---|
| `GOSVC_SERVICE_NAME` / `GOSVC_ENV` | 服务名 / 环境（dev\|staging\|prod） |
| `GOSVC_HTTP_ADDR` | REST/JSON-RPC 地址，如 `0.0.0.0:8080` |
| `GOSVC_GRPC_ADDR` | gRPC 地址 |
| `GOSVC_ADMIN_ADDR` | admin 地址 |
| `GOSVC_LOG_LEVEL` / `GOSVC_LOG_FORMAT` | `debug\|info\|warn\|error` / `json\|text` |

`limiter.rps > 0` 时启用按客户端 IP 的限流，`burst` 必填；HTTP 与 gRPC 共用同一限流器。

## 可观测性

- `GET /metrics`（admin 端口）：`gosvc_http_requests_total`、`gosvc_http_request_duration_seconds`、
  `gosvc_jsonrpc_requests_total`、`gosvc_jsonrpc_request_duration_seconds`、
  `gosvc_grpc_requests_total`、`gosvc_grpc_request_duration_seconds`，外加 Go runtime / process 指标。
- `GET /debug/pprof/*`：CPU、heap、goroutine 等 profile。
- `GET /healthz` / `GET /readyz`：存活与就绪；k8s 探针可直接用 `gosvc -healthcheck <url>`。
- gRPC 自带 health service（`grpc.health.v1.Health`）与 reflection。

## 测试与压测

```bash
make test          # 单元测试
make race          # -race 全量测试（CI 同款）
make lint          # gofmt 检查 + go vet
make proto         # 重新生成 protobuf 代码
```

- `internal/jsonrpc`：协议层单测（批量、通知、错误码、参数校验）；
- `internal/config`、`internal/service/greeter`：配置合并/校验、领域逻辑；
- `test/e2e`：真实启动全部端口，覆盖 REST、JSON-RPC（含批量+通知）、gRPC（含 NotFound 映射）、
  metrics、优雅关闭。

## 部署

```bash
make docker                     # 构建镜像
docker compose -f deploy/docker-compose.yml up --build
```

- 运行镜像是 distroless + 非 root；健康检查复用二进制：`gosvc -healthcheck http://127.0.0.1:6060/healthz`；
- **admin 端口只应暴露在内网**（默认绑定 `127.0.0.1`）；
- 需要 TLS 时：HTTP 可用 Hertz `server.WithTLS`（会回退到标准库网络层），生产更推荐由网关/负载均衡终止 TLS。

## 设计取舍

- **Hertz** 作为 HTTP 层：性能好、中间件生态完整（CORS/JWT/pprof/prometheus 等），且基于 netpoll；
  本项目只用其核心能力，业务代码通过 `internal/transport/http` 隔离。
- **sonic** 作为 JSON 编解码：热路径零拷贝/JIT 优化，非 amd64/arm64 平台会自动退化为兼容实现。
- **gRPC 用 grpc-go 而不是 Kitex**：模板的 gRPC 只需服务端契约与拦截器，grpc-go 生态最稳、无需额外 codegen 工具链。
  如果后续要做服务间 RPC（服务发现、重试、熔断），可以把 `internal/transport/grpc` 换成一薄层 Kitex，
  领域层与错误映射无需改动。
- **标准库 `slog`** 作为日志门面：Kitex/Hertz 的 `klog`/`hlog` 都能通过适配器接入（本模板已实现 Hertz 的
  `hlog.FullLogger` 适配，见 `internal/transport/http/hlog_adapter.go`）。
- **JSON-RPC 只支持具名参数（object）**：定位参数会被明确拒绝（-32602），避免隐式兼容问题。

## Roadmap

- [ ] 认证与鉴权中间件（JWT / API Key）
- [ ] OpenTelemetry tracing（Hertz、gRPC 拦截器各一处接入）
- [ ] Kitex 可选层示例（服务间 RPC + 服务发现）
- [ ] 自定义 TCP 协议示例（netpoll EventLoop + 有界 worker pool）
- [ ] Redis 存储与时间序列指标组件
- [ ] 配置热更新（fsnotify + atomic 快照）

## 重命名模块

```bash
scripts/rename-module.sh github.com/you/your-service
# 然后更新 api/greeter/v1/greeter.proto 的 go_package 并重新生成：
make proto
```
