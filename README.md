# gosvc — 多协议 Go 服务运行时（库）

`gosvc` 是一个可 import 的服务运行时：把日志、配置、认证、追踪、指标、限流、存储、
HTTP / JSON-RPC / gRPC / TCP 四种传输和优雅退出都装好，业务代码只写自己的 handler 与领域逻辑。

```bash
go get github.com/you/gosvc        # 或先把本仓库改名为你的模块（见文末）
```

```go
package main

import (
	"context"
	"log/slog"
	"os/signal"
	"syscall"

	"github.com/cloudwego/hertz/pkg/app/server"
	ggrpc "google.golang.org/grpc"

	"github.com/you/gosvc"
	"github.com/you/gosvc/jsonrpc"
	"github.com/you/gosvc/logging"
)

// 1. 配置：嵌入运行时配置，加自己的段
type Config struct {
	gosvc.Config
	Database DatabaseConfig `json:"database"`
}

type DatabaseConfig struct{ DSN string `json:"dsn"` }

func (c *Config) SetDefaults() {
	c.Config.SetDefaults()
	if c.Database.DSN == "" {
		c.Database.DSN = "postgres://localhost/app"
	}
}

func (c *Config) Validate() error {
	if err := c.Config.Validate(); err != nil {
		return err
	}
	if c.Database.DSN == "" {
		return errors.New("database.dsn is required")
	}
	return nil
}

func main() {
	// 2. 加载：默认值 < 文件 < 环境变量（泛型方法，编译期检查）
	cfg, err := (gosvc.Source{Path: "config.toml", EnvPrefix: "MYAPP", Strict: true}).Load[Config]()
	if err != nil {
		slog.Error("config", "error", err)
		os.Exit(1)
	}

	logHandle, err := logging.New(cfg.Log, cfg.Service.Name, cfg.Service.Env, "v1.0.0")
	if err != nil {
		slog.Error("logger", "error", err)
		os.Exit(1)
	}
	slog.SetDefault(logHandle.Logger())

	// 3. 组装运行时
	app, err := gosvc.New(&cfg.Config,
		gosvc.WithLogger(logHandle),
		gosvc.WithVersion("v1.0.0"),
		gosvc.WithHotReload("config.toml", "MYAPP"),
	)
	if err != nil {
		slog.Error("runtime", "error", err)
		os.Exit(1)
	}

	// 4. 注册业务 handler（三种协议共享同一个领域服务）
	svc := NewOrderService(cfg.Database.DSN)
	app.RegisterHTTP(func(h *server.Hertz) {
		h.GET("/api/v1/orders/:id", handleGetOrder(svc))
	})
	app.RegisterJSONRPC(func(d *jsonrpc.Dispatcher) {
		d.RegisterTyped("orders.get", func(ctx context.Context, req GetOrderRequest) (*Order, error) {
			return svc.Get(ctx, req.ID)
		})
	})
	app.RegisterGRPC(func(s *ggrpc.Server) {
		orderv1.RegisterOrderServiceServer(s, newOrderServer(svc))
	})

	// 5. 运行：信号、优雅退出、热更新、指标全部由运行时处理
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := app.Run(ctx); err != nil {
		slog.Error("run", "error", err)
		os.Exit(1)
	}
}
```

## 特性

| 能力 | 说明 |
|---|---|
| 四协议同栈 | REST + JSON-RPC 2.0（HTTP 端口）、gRPC、行分隔 JSON-RPC over TCP（netpoll），JSON-RPC 方法注册一次两种传输都可用 |
| 类型安全 | `Dispatcher.RegisterTyped[Req, Resp]` 与 `Client.Call[Req, Resp]` 让方法两端都带类型；`config.Source.Load[T]` 编译期校验配置类型 |
| 统一错误模型 | `apierror.Kind` 一处定义，各协议自动映射（HTTP 状态码 / JSON-RPC code / gRPC code），内部错误不泄漏 |
| 认证 | API Key（constant-time）+ HS256 JWT，HTTP 中间件与 gRPC 拦截器共用；health/reflection 默认放行 |
| 链路追踪 | OpenTelemetry OTLP/HTTP，W3C TraceContext 传播，Hertz 中间件 + `otelgrpc` StatsHandler |
| 日志-链路关联 | 请求日志自动带 `trace_id` / `span_id`（HTTP、gRPC、TCP 一致），与导出的 span 对应 |
| 日志 | 两层：`slogx`（geth 风格 handler，零依赖）+ `logging`（多 sink、轮转、采样、运行期级别） |
| 可观测性 | Prometheus 指标、pprof、healthz/readyz（含依赖探针）、日志统计、配置查看、时间序列查询，独立 admin 端口 |
| 存储连接器 | Redis（单机 / Cluster / Sentinel，分钟桶 + 内存聚合批量写）与 PostgreSQL（pgx 连接池、池指标、就绪探针） |
| 访问日志独立 sink | `log.access.*`：请求日志走自己的级别/格式/输出/文件，自动带 `request_id`/`trace_id`/`log_type=access` |
| 无锁状态 | `state.Snapshot[T]` 提供读无锁、写替换的共享状态 |
| 热更新 | fsnotify 监听配置文件：`log.*` / `auth.*` / `limiter.*` 热生效，`storage.*` **重建连接**（新连接就绪后切换，失败保留旧连接），其余字段提示 `restartRequired` |
| 有界并发 | `workerpool`（显式 `ErrFull`/`ErrClosed`）+ netpoll 事件循环 + 每连接写串行化，慢业务不阻塞 IO |
| 压测器 | `examples/app/cmd/bench` 支持 rest / jsonrpc / grpc |

## 目录结构

```
.
├── app.go                  # package gosvc：运行时门面（New / Register* / Run / Reload）
├── types.go                # 配置类型别名，方便单 import 使用
│
│  # 公开包（应用可以 import）
├── config/                 # 配置 + 泛型加载器 Source.Load[T]（TOML）
├── logging/                # 日志装配 + request id + trace_id 注入
├── slogx/                  # geth 风格 slog handler（terminal/json/logfmt、采样、SwapHandler）
├── auth/                   # API Key + JWT（支持运行期 Reload）
├── apierror/               # 传输无关错误
├── health/                 # 就绪标志 + 命名依赖探针
├── observability/          # Prometheus 指标（含数据库连接池采集器）
├── ratelimit/              # 按 key 的 token bucket（运行期 SetRate）
├── state/                  # 泛型无锁快照 Snapshot[T]
├── jsonrpc/                # JSON-RPC 2.0：Dispatcher（RegisterTyped）+ Client（Call）
├── store/                  # 存储接口（Recorder / TimeSeries）
│   ├── redis/              #   Redis：单机 / Cluster / Sentinel + 分钟桶
│   └── postgres/           #   PostgreSQL：pgx 连接池 + 健康探针
├── transport/http/         # Hertz：/rpc、中间件链（认证/限流/追踪/日志）
├── transport/grpc/         # grpc-go：拦截器、health、reflection、ToStatus
├── transport/tcp/          # netpoll：行分隔 JSON-RPC + 有界 worker pool
│
│  # 实现细节（不对外，应用无需 import）
└── internal/
    ├── admin/              # 运维端口
    ├── reload/             # 配置文件监听
    ├── telemetry/          # OpenTelemetry 初始化
    ├── version/            # 库自身构建信息（应用用自己的版本变量）
    └── workerpool/         # 有界 goroutine 池（tcp 使用）

examples/
├── app/                    # 示例应用（greeter）：config / bindings / cmd / e2e 测试
└── kitex/                  # Kitex 服务间 RPC 示例（不进入库依赖）
```

## API 速查

### 运行时

```go
app, err := gosvc.New(&cfg.Config, opts...)   // 绑定端口、建好各组件（未开始服务）

// 注册（必须在 Run 之前；Run 之后再注册返回 gosvc.ErrStarted）
app.RegisterHTTP(func(h *server.Hertz))
app.RegisterGRPC(func(s *grpc.Server))
app.RegisterJSONRPC(func(d *jsonrpc.Dispatcher))
app.RegisterAdmin(func(mux *http.ServeMux))

app.Run(ctx)            // 阻塞直到 ctx 取消或某个 server 失败，随后优雅退出

// 运行时访问器（handler 里常用）
app.Logger()            // *slog.Logger
app.Metrics()           // *observability.Metrics
app.Auth()              // *auth.Authenticator
app.Recorder()          // store.Recorder（未启用 Redis 时为 nil）
app.Store()             // *redis.Store（未启用时为 nil）
app.Postgres()          // *postgres.DB（未启用时为 nil）
app.Health()            // *health.Ready，可注册自己的依赖探针
app.Config()            // 当前生效配置（未脱敏）
app.CurrentConfig()     // 脱敏快照（admin /debug/config 用）
app.HTTPAddr() / GRPCAddr() / TCPAddr() / AdminAddr()
app.Reload()            // 手动重载配置
```

选项：`WithLogger`、`WithVersion`、`WithHotReload(path, envPrefix)`、`WithPublicPaths(...)`、`WithOnReload(fn)`。

### 配置

```go
// 泛型方法：T 是应用配置类型，*T 必须实现 Configurable（SetDefaults/Validate）
cfg, err := (gosvc.Source{Path: "config.toml", EnvPrefix: "MYAPP", Strict: true}).Load[Config]()

// 嵌入 gosvc.Config 即自动获得 SetDefaults/Validate/ApplyEnv，按需覆盖
func (c *Config) SetDefaults() { c.Config.SetDefaults(); /* 应用默认值 */ }
func (c *Config) Validate() error { /* 应用校验 */ }
```

环境变量前缀可配（默认 `GOSVC`）：`MYAPP_HTTP_ADDR`、`MYAPP_AUTH_API_KEYS`、`MYAPP_REDIS_ADDR`、`MYAPP_OTLP_ENDPOINT` 等。

### JSON-RPC：注册与调用

```go
// 服务端：类型从 handler 推断
d.RegisterTyped("orders.get", svc.Get)   // func(context.Context, GetOrderRequest) (*Order, error)

// 客户端：HTTP 或 TCP，同一套类型
client := jsonrpc.NewHTTPClient("http://127.0.0.1:8080", jsonrpc.WithHeader("X-API-Key", key))
defer client.Close()
order, err := client.Call[GetOrderRequest, *Order](ctx, "orders.get", GetOrderRequest{ID: 1})
```

### 错误映射

```go
return nil, apierror.New(apierror.KindNotFound, "order 1 not found")
```

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

gRPC handler 里用 `grpctransport.ToStatus(err)`；HTTP 用 `httptransport.WriteError(ctx, c, err)`。

### 无锁状态

```go
var config state.Snapshot[*RouteTable]
config.Store(table)                    // 写：整体替换
table := config.Load()                 // 读：无锁
config.Update(func(cur *RouteTable) *RouteTable { ... })  // CAS 循环
```

## Go 1.27 与泛型用法

本库要求 **Go 1.27+**，刻意用上了这些新能力：

| 特性 | 用在哪 |
|---|---|
| **泛型方法**（1.27 新增） | `config.Source.Load[T, PT]`、`jsonrpc.Dispatcher.RegisterTyped[Req, Resp]`、`jsonrpc.Client.Call[Req, Resp]` |
| 泛型类型 | `state.Snapshot[T]`、`jsonrpc.Client.Call` 的返回值 |
| `errors.AsType[E]` | `apierror.KindOf` / `Wrap` / `ClientMessage`，去掉 `var target *T; errors.As(...)` 样板 |
| `sync.WaitGroup.Go` | `workerpool` 的 worker 启动与等待 |
| `testing/synctest` | 日志采样窗口测试用虚拟时钟，去掉真实 sleep |
| 函数类型推断改进 | `RegisterTyped` 的 handler 实参直接推断类型参数，无需显式写 |

`encoding/json/v2`、`uuid`、实验性 `simd` 在 1.27 已可用，本库暂未使用（配置解析仍在冷路径，
热路径 JSON 用 sonic）。需要时可以按包逐步切换。

## 配置参考

TOML 文件；优先级：内置默认值 < 文件 < 环境变量。时长写字符串（`"10s"`、`"1h30m"`）或数字（秒）。
`Source.Strict = true` 会拒绝未知键，能在启动时抓出拼写错误。

```toml
[service]
name = "myapp"
env = "dev"

[http]
host = "0.0.0.0"
port = 8080
readTimeout = "10s"
writeTimeout = "10s"
idleTimeout = "60s"
shutdownTimeout = "10s"
maxBodyBytes = 1048576

[grpc]
host = "0.0.0.0"
port = 9090
shutdownTimeout = "10s"

[tcp]
enabled = false
host = "0.0.0.0"
port = 7070
workers = 4
queueSize = 1024
maxFrameBytes = 1048576
readTimeout = "60s"
shutdownTimeout = "5s"

[admin]
host = "127.0.0.1"
port = 6060

[log]
level = "info"
format = "terminal"       # auto | terminal | json | logfmt
output = "stdout"         # stdout | file | both
color = "auto"
addSource = false

[log.file]
path = "logs/app.log"
maxSizeMB = 100
maxBackups = 5
maxAgeDays = 7
compress = true

[log.sampling]
enabled = false
initial = 100
thereafter = 100
tick = "1s"

# 请求日志独立 sink（关闭时与主日志共用）
[log.access]
enabled = false
level = "info"
format = "json"
output = "stdout"
color = "auto"

[log.access.file]
path = "logs/access.log"
maxSizeMB = 100
maxBackups = 5
maxAgeDays = 7
compress = true

[limiter]
rps = 0
burst = 0

[auth]
enabled = false
apiKeys = []

[auth.jwt]
secret = ""
issuer = "myapp"
audience = ""

[telemetry]
enabled = false
otlpEndpoint = "127.0.0.1:4318"
insecure = true
sampleRatio = 1.0
batchTimeout = "5s"

[storage.redis]
enabled = false
mode = "single"            # single | cluster | sentinel
addr = "127.0.0.1:6379"    # single
addrs = []                 # cluster 种子节点 / sentinel 节点
masterName = ""            # sentinel
password = ""
db = 0
prefix = "myapp"
bucketTtl = "25h"
queueSize = 4096
poolSize = 0

[storage.postgres]
enabled = false
dsn = "postgres://user:pass@localhost/app?sslmode=disable"
maxOpenConns = 16
maxIdleConns = 4
connMaxLifetime = "1h"
connMaxIdleTime = "10m"
pingTimeout = "3s"

# 应用自己的段
[database]
dsn = "postgres://localhost/app"
```

## 日志

两层结构：`slogx`（一行长什么样，零依赖）与 `logging`（写到哪、什么级别、带什么字段）。

```
INFO  2026-09-27T01:31:46.481Z middleware.go:47   - http request   service=myapp env=dev request_id=... trace_id=638ec9c8... method=GET route=/api/v1/orders status=200
```

- `format`: `auto`（dev→terminal，其它→json）| `terminal` | `json` | `logfmt`；
- `output`: `stdout`（推荐，交给平台收集）| `file` | `both`（终端彩色、文件固定 JSON + lumberjack 轮转）；
- **采样**：按 `(级别, 消息)` 计数，每窗口前 `initial` 条必出、之后每 `thereafter` 条抽 1 条，`warn`/`error` 豁免；
- **访问日志独立 sink**：`log.access` 可把请求日志（HTTP/gRPC）路由到自己的级别/格式/输出/文件，
  并自动带 `request_id` / `trace_id` / `log_type=access`；关闭时与主日志共用同一条链；
- **日志与链路关联**：开启 tracing 后，HTTP/gRPC/TCP 的请求日志自动带 `trace_id` / `span_id`
  （`logging.WithTrace(ctx)` 由各传输在 span 创建后注入），在 Jaeger/Tempo 里可以按 trace 反查日志；
- 运行期改级别 `PUT /debug/loglevel`，采样计数 `GET /debug/logstats`。

## 存储与就绪

### Redis（时间序列）

- `mode`: `single`（`addr`）| `cluster`（`addrs` 种子节点）| `sentinel`（`addrs` + `masterName`），
  由 go-redis 的 universal client 统一处理；
- 请求路径只做非阻塞入队，后台单写者聚合后按**分钟桶**批量写：`{prefix}:ts:{metric}:{yyyyMMddHHmm}`，带 TTL；
- 查询：`GET /debug/ts?metric=http.requests:/api/v1/hello&minutes=60`；
- 环境变量：`GOSVC_REDIS_ADDR`（单机模式快捷方式）。

### PostgreSQL

```toml
[storage.postgres]
enabled = true
dsn = "postgres://user:pass@localhost/app?sslmode=disable"
maxOpenConns = 16
maxIdleConns = 4
connMaxLifetime = "1h"
```

- 基于 `database/sql` + pgx，启动时 Ping（带 `pingTimeout`），失败即拒绝启动；
- 连接池参数在启动时应用；池指标自动注册：`gosvc_db_pool_open_connections`、
  `in_use_connections`、`idle_connections`、`wait_total`、`max_open_connections`；
- 环境变量：`GOSVC_POSTGRES_DSN`；
- 使用：`app.Postgres()` 拿到 `*sql.DB` 包装，直接查询即可。

### 就绪探针

`readyz` 会聚合命名依赖探针（PostgreSQL 启用时自动注册），返回逐项状态：

```json
{"status":"not_ready","checks":{"postgres":"dial tcp 127.0.0.1:5432: connect: connection refused"}}
```

应用也可以注册自己的探针：

```go
app.Health().AddCheck("redis", func(ctx context.Context) error { return redisClient.Ping(ctx).Err() })
```

### 热更新时重建连接

`storage.*` 变化时运行时会**新建连接 → 切换 → 优雅关闭旧连接**（recorder 先 flush 再关闭）：

- 新连接建立失败（例如 DSN 写错、数据库不可达）时保留旧连接，日志记录 error，服务不中断；
- 通过 `app.Store()` / `app.Postgres()` / `app.Recorder()` 读到的始终是当前生效的连接；
- 池指标通过 provider 采集，切换后无需重新注册。

## 运维端点（admin 端口）

| 端点 | 说明 |
|---|---|
| `GET /healthz` / `GET /readyz` | 存活 / 就绪（含依赖探针明细；k8s 探针可用 `app -healthcheck <url>`） |
| `GET /metrics` | Prometheus（HTTP / JSON-RPC / gRPC / Go runtime） |
| `GET /debug/pprof/*` | CPU、heap、goroutine |
| `GET/PUT /debug/loglevel` | 查看/修改日志级别 |
| `GET /debug/logstats` | 采样计数 |
| `GET /debug/config` | 当前生效配置（密钥脱敏） |
| `POST /debug/reload` | 手动触发配置重载（ConfigMap 场景） |
| `GET /debug/ts?metric=...&minutes=60` | Redis 分钟桶查询 |

## 示例应用

```bash
make run          # 启动 examples/app（四协议 + 可观测性）
make bench        # rest 压测
go test ./...     # 单元 + 端到端（含热更新、采样、认证）
go run ./examples/kitex/cmd -mode server   # Kitex 示例
```

`examples/app` 演示了推荐的分层：`config.go`（嵌入配置）、`bindings.go`（三协议绑定）、
`greeter/`（领域服务）、`cmd/`（入口与压测器）、`e2e_test.go`（端到端测试）。

## 部署

```bash
make docker
docker compose -f deploy/docker-compose.yml up --build
```

distroless + 非 root；admin 端口默认只绑 `127.0.0.1`；TCP 传输不做 TLS，请在网关终止。

## 设计取舍

- **框架类型出现在 API 里**（`*server.Hertz`、`*grpc.Server`）：换取零额外抽象和完整的框架能力；
  想换框架时只需替换 transport 包；
- **一个共享 Dispatcher**：HTTP `/rpc` 与 TCP 方法只注册一次，指标也只记一份；
- **认证默认全局开启**（healthz/readyz 白名单）：避免应用忘记给业务路由加鉴权；
- **热更新只覆盖库拥有的字段**：应用自己的段通过 `WithOnReload` 处理；
- **配置加载用泛型方法**：编译期保证 `*T` 可配置，避免运行时类型断言。

## Roadmap

- [ ] 前端静态资源 embed 辅助（Hertz StaticFS + 构建产物）
- [ ] 把 trace_id 注入到 OTLP 日志导出（logs signal）
- [ ] 配置热更新支持 TLS 证书轮换

## 写一个新服务

- 逐步指南：**[docs/new-service.md](docs/new-service.md)**（配置、入口、协议绑定、存储、测试、部署清单）
- 可运行示例：`examples/app`；OpenCode 用户可加载技能 `gosvc-new-service`

## 重命名模块

```bash
scripts/rename-module.sh github.com/you/gosvc
make proto kitex     # 重新生成示例的 protobuf / Kitex 代码
```
