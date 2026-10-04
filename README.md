# gosvc — 多协议 Go 服务运行时（库）

`gosvc` 是一个可 import 的服务运行时：把日志、配置、认证、追踪、指标、限流、存储、
HTTP / JSON-RPC / gRPC / TCP 四种传输和优雅退出都装好，业务代码只写自己的 handler 与领域逻辑。

想先运行一个简单业务，可以从 [待办事项 sample](examples/tasks/README.md) 开始：
`go run ./examples/tasks/cmd/tasks -c examples/tasks/config.toml`。
它用内存保存任务，演示 REST / JSON-RPC 共用业务逻辑、错误映射、认证和框架启动退出；重启后数据清空。

组件边界、并发约束和泛型使用见 [架构说明](docs/architecture.md)，局部基准数据与复现命令见 [性能记录](docs/performance.md)。

```bash
go get github.com/Tokimorphling/gosvc        # 或先把本仓库改名为你的模块（见文末）
```

```go
package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/cloudwego/hertz/pkg/app/server"
	ggrpc "google.golang.org/grpc"

	"github.com/Tokimorphling/gosvc"
	"github.com/Tokimorphling/gosvc/jsonrpc"
	"github.com/Tokimorphling/gosvc/logging"
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
	must := func(err error) {
		if err != nil {
			slog.Error("register", "error", err)
			os.Exit(1)
		}
	}
	must(app.RegisterHTTP(func(h *server.Hertz) {
		h.GET("/api/v1/orders/:id", handleGetOrder(svc))
	}))
	must(app.RegisterJSONRPC(func(d *jsonrpc.Dispatcher) {
		d.RegisterTyped("orders.get", func(ctx context.Context, req GetOrderRequest) (*Order, error) {
			return svc.Get(ctx, req.ID)
		})
	}))
	must(app.RegisterGRPC(func(s *ggrpc.Server) {
		orderv1.RegisterOrderServiceServer(s, newOrderServer(svc))
	}))

	// 5. 运行：信号、优雅退出、热更新、指标全部由运行时处理。
	// Run 返回时所有传输的优雅退出（drain 在途请求）已经真正完成。
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
| 统一错误模型 | `apierror.Kind` 一处定义，各协议自动映射（HTTP 状态码 / JSON-RPC code / gRPC code），内部错误不泄漏；客户端把传输层错误映射回 `apierror` |
| 认证 | API Key（constant-time）+ HS256 JWT，HTTP 中间件与 gRPC 拦截器（unary + streaming）共用，`auth.JSONRPCMiddleware` 把同一鉴权带到 Dispatcher 层（TCP 无传输层鉴权的自然补齐）；health/reflection 默认放行；TCP 传输默认不鉴权（见设计取舍） |
| 链路追踪 | OpenTelemetry OTLP/HTTP，W3C TraceContext 传播，Hertz 中间件 + `otelgrpc` StatsHandler |
| 日志-链路关联 | 请求日志自动带 `trace_id` / `span_id`（HTTP、gRPC、TCP 一致），与导出的 span 对应 |
| 日志 | 两层：`slogx`（geth 风格 handler，零依赖）+ `logging`（多 sink、轮转、采样、运行期级别；重载关闭旧文件 sink） |
| 可观测性 | Prometheus 指标（HTTP / JSON-RPC / gRPC / TCP 拒绝与连接数 / Go runtime）、pprof、healthz/readyz（含依赖探针）、日志统计、配置查看、时间序列查询，独立 admin 端口（可选 bearer token） |
| 存储连接器 | Redis（单机 / Cluster / Sentinel，分钟桶 + 内存聚合批量写）与 PostgreSQL（pgx 连接池、池指标、就绪探针） |
| 访问日志独立 sink | `log.access.*`：请求日志走自己的级别/格式/输出/文件，自动带 `request_id`/`trace_id`/`log_type=access` |
| 无锁状态 | `state.Snapshot[T]` 提供读无锁、写替换的共享状态 |
| 热更新 | fsnotify 监听配置文件：`log.*` / `auth.*` / `limiter.*` 热生效，`storage.*` **重建连接**（新连接就绪后切换，失败保留旧连接且有效配置回滚），其余字段提示 `restartRequired` |
| 有界并发 | `workerpool`（显式 `ErrFull`/`ErrClosed`）+ netpoll 事件循环 + 每连接写串行化，慢业务不阻塞 IO；TCP 逐帧限制解析长度 |
| 推送（push） | `push.Broker[T]` 统一 fan-out：订阅登记、背压、死连接剪枝及显式 Shutdown 排空；`Sink` 适配 TCP session / SSE / gRPC stream 三种形态，`SinkFromContext` 让 "subscribe" 方法零胶水 |
| 扩展点 | TCP 帧方言 `tcp.Codec`（stratum 等非 JSON-RPC 协议可直接落 gosvc TCP）、连接生命周期 `tcp.Callbacks`（OnConnect/OnDisconnect 恰好一次）、逐方法 `jsonrpc.Middleware`（覆盖 HTTP /rpc + TCP 全部路径）、串行派发 `Serial` 能力、按传输方法表 `WithTCPDispatcher`；默认行为零侵入 |
| 优雅退出 | 四个传输的 `Serve` 都会 join 自己的 drain；业务 handler 需响应 `ctx` 取消，否则仍可能阻止 `Run` 返回 |
| 压测器 | `examples/app/cmd/bench` 支持 rest / jsonrpc / grpc |

## 目录结构

```
.
├── app.go                  # package gosvc：组件组装、运行与访问器（New / Run）
├── options.go              # 运行时装配选项
├── app_registration.go     # 类型化协议注册与统一生命周期校验
├── app_shutdown.go         # 退出 hook、共享预算与资源释放
├── app_storage.go          # 存储连接与热重建（WithStore / WithPostgres lease）
├── app_reload.go           # 热重载事务（reloadable 字段 / restartRequired）
├── types.go                # 少量类型别名（Config / Source / Configurable）
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
    └── workerpool/         # 泛型 key、有界槽位与 O(1) 调度（tcp 使用）

examples/
├── tasks/                  # 入门业务 sample：待办事项 CRUD，内存存储，REST + JSON-RPC
├── app/                    # 完整能力示例（greeter）：config / bindings / cmd / e2e 测试
└── kitex/                  # Kitex 服务间 RPC 示例（独立 go module，不进入库依赖）
```

## API 速查

### 运行时

```go
app, err := gosvc.New(&cfg.Config, opts...)   // 绑定端口、建好各组件（未开始服务）

// 注册（必须在 Run 之前；Run 之后再注册返回 gosvc.ErrStarted）
err = app.RegisterHTTP(func(h *server.Hertz))    // 每个 Register* 都返回 error
err = app.RegisterGRPC(func(s *grpc.Server))
err = app.RegisterJSONRPC(func(d *jsonrpc.Dispatcher))
err = app.RegisterAdmin(func(mux *http.ServeMux))

app.Run(ctx)            // 阻塞直到 ctx 取消或某个 server 失败；返回前等待所有传输的 drain 完成
app.Close()             // New 成功但不调用 Run 时释放已绑定资源；可重复调用

// 运行时访问器（handler 里常用）
app.Logger()            // *slog.Logger
app.Metrics()           // *observability.Metrics
app.Auth()              // *auth.Authenticator
app.Recorder()          // 当前 recorder 快照（未启用 Redis 时为 nil）
app.StableRecorder()    // 可长期持有，随 Redis 重载自动切换
app.Store()             // 当前 *redis.Store 快照（未启用时为 nil）
app.Postgres()          // 当前 *postgres.DB 快照（未启用时为 nil）
app.WithStore(fn)       // 在 fn 执行期间保持当前 Redis 连接可用
app.WithPostgres(fn)    // 在 fn 执行期间保持当前 PostgreSQL 连接池可用
app.Health()            // *health.Ready，可注册自己的依赖探针
app.Config()            // 当前生效配置（未脱敏）
app.CurrentConfig()     // 脱敏快照（admin /debug/config 用）
app.HTTPAddr() / GRPCAddr() / TCPAddr() / AdminAddr()
app.Reload()            // 手动重载配置
```

选项：`WithLogger`、`WithVersion`、`WithHotReload(path, envPrefix)`、`WithPublicPaths(...)`、`WithOnReload(fn)`、`WithOnShutdown(fn...)`、`WithOnShutdownContext(fn...)`、`WithShutdownTimeout(d)`、`WithTCPCodec(codec)`、`WithTCPCallbacks(cb)`、`WithJSONRPCMiddleware(mw...)`、`WithTCPDispatcher(d)`。

收到退出信号时，运行时先将 readiness 和 gRPC health 置为不可用，在连接仍可用时执行清理 hook，
然后停止传输。hook 与传输 drain 共享一个截止时间，默认取已启用传输中最大的 shutdown timeout，
可用 `WithShutdownTimeout` 覆盖；各传输还会遵守自身较短的 timeout。

耗时清理使用 `WithOnShutdownContext(func(ctx context.Context) error { ... })` 并遵守 ctx。
hook 按注册顺序执行，错误与 panic 由 `Run` 返回；hook 超时后不再等待它，并继续关闭传输。
旧的 `WithOnShutdown(func())` 仍可使用，但忽略取消的 hook 可能在 `Run` 返回后继续执行。
Go 无法强制终止任意 goroutine；业务 handler、存储 lease 和资源清理仍需遵守取消并及时返回。

### 配置

```go
// 泛型方法：T 是应用配置类型，*T 必须实现 Configurable（SetDefaults/Validate）
cfg, err := (gosvc.Source{Path: "config.toml", EnvPrefix: "MYAPP", Strict: true}).Load[Config]()

// 嵌入 gosvc.Config 即自动获得 SetDefaults/Validate/ApplyEnv，按需覆盖
func (c *Config) SetDefaults() { c.Config.SetDefaults(); /* 应用默认值 */ }
func (c *Config) Validate() error { /* 应用校验 */ }
```

环境变量前缀可配（默认 `GOSVC`）：`MYAPP_HTTP_ADDR`、`MYAPP_GRPC_ADDR`、`MYAPP_TCP_ADDR`、`MYAPP_AUTH_API_KEYS`、`MYAPP_REDIS_ADDR`、`MYAPP_OTLP_ENDPOINT` 等。

### JSON-RPC：注册与调用

```go
// 服务端：类型从 handler 推断
d.RegisterTyped("orders.get", svc.Get)   // func(context.Context, GetOrderRequest) (*Order, error)
// 运行期动态注册用 TryRegister：重复返回 ErrDuplicateMethod 而不是 panic
d.TryRegister("orders.get", handler)

// 客户端：HTTP 或 TCP，同一套类型
client := jsonrpc.NewHTTPClient("http://127.0.0.1:8080", jsonrpc.WithHeader("X-API-Key", key))
defer client.Close()
order, err := client.Call[GetOrderRequest, *Order](ctx, "orders.get", GetOrderRequest{ID: 1})
```

- 批量请求默认最多 **128** 个/次（`Dispatcher.SetMaxBatch(n)` 调整，超出整批拒绝 `-32600`）；
- Dispatcher 通过不可变快照发布注册变更，调用路径不取注册锁；中间件链惰性构建且不持有注册锁，单方法变更不重建其他方法的链；在途请求使用调用开始时的方法表与 observer；
- `NewTCPClient` 单连接支持 **pipeline**：并发 `Call` 在同一连接上多路复用，按 JSON-RPC id 匹配响应，慢请求不会阻塞后续请求；
- HTTP 客户端把运行时错误映射（401/429 + `{"error":{"code":"..."}}`）还原成 `*apierror.Error`，调用方按 `apierror.KindOf` 分支即可；
- `Notify` 同样检查 HTTP 状态码并返回拒绝错误；默认 HTTP 客户端超时为 5 秒，可用 `WithTimeout` 调整，传入 `WithHTTPClient` 时保留该客户端的超时设置；
- 服务端 handler panic 不会丢连接：worker 存活，客户端收到 `internal error` 帧；
- batch 内的请求**顺序执行**（一个慢方法会拖慢同批的后续请求）：这是刻意为之，一个 batch 不能无限占用 worker；需要并发请拆成多个单请求；
- 内置方法：`system.methods` 列出已注册方法；`system.health` 返回 `{"ready":bool,"checks":{...}}`，
  聚合与 GET /readyz 相同的依赖探针，纯 JSON-RPC 客户端（如 TCP 上的设备）不用起 HTTP 即可探测就绪。

### Dispatcher 层鉴权：auth.JSONRPCMiddleware

`auth.JSONRPCMiddleware(authenticator, credentials)` 返回一个 `jsonrpc.Middleware`：传输层已鉴权的调用
（HTTP /rpc 后面的 HTTP 鉴权中间件）直接透传；否则用 `credentials` 从本次调用的 params 里取凭据
（`auth.CredentialsFromParams("token", "apiKey")`，为无 header 的 TCP 方言准备）交给同一个
`Authenticator` 校验，并把 identity 注入 ctx（`auth.FromIdentity`）与请求日志（`subject`/`auth_method`）。
auth 未启用时透传，与「认证需显式开启」一致。TCP 传输不做传输层鉴权，逐方法的 `UseFor` +
这个 helper 正是补齐点：

```go
mw := auth.JSONRPCMiddleware(app.Auth(), auth.CredentialsFromParams("", "apiKey"))
d.UseFor("mining.submit", mw)
// 或直接给整个方法表：
d.Use(mw)
```

## 推送（push）

推送不是 Dispatcher 的通用能力，而是**按传输、显式声明**的一等能力：`RegisterTyped` 的请求/响应模型保持不动。三种形态：

### Broker：fan-out 一处收口

订阅登记、逐订阅者背压、死连接剪枝、shutdown 排空——这些每应用都要抄一遍的胶水由
`push.Broker[T]` 统一提供。`Publish` 永不阻塞：每个订阅者一条有界队列 + 一个 pump
goroutine，慢客户端只拖慢自己；队列满按策略 `drop`（默认，计数）或 `disconnect`
（终结慢消费者，重连重订阅）。指标：`gosvc_broker_subscribers/delivered_total/dropped_total`。
TCP 会再经过每连接的发送队列；该队列按 `drop` 策略丢弃一条通知时，broker 将其计为
`dropped` 并保留订阅。把 `Broker.Shutdown` 注册进生命周期后，排空发生在传输仍在
服务的窗口内：`gosvc.New(cfg, gosvc.WithOnShutdown(events.Shutdown))`——shutdown 触发
后、各传输关闭连接前运行，队列里的事件还能送达在线客户端（之后 `Publish` 不再投递，
`Len()` 在排空期间可能暂时非零）。

```go
events := push.NewBroker[Event]("events", push.WithQueueSize(256), push.WithPolicy(push.Drop))

// subscribe 型 handler：从 ctx 拿当前传输的 sink（TCP 有，HTTP /rpc 没有）
d.RegisterTyped("events.subscribe", func(ctx context.Context, _ struct{}) (map[string]any, error) {
    sink, ok := push.SinkFromContext(ctx)
    if !ok {
        return nil, apierror.New(apierror.KindInvalidArgument, "requires the TCP transport")
    }
    events.Subscribe(sink)
    return map[string]any{"subscribers": events.Len()}, nil
})

// 业务触发：一次 Publish，全部订阅者收到
events.Publish("events.pong", Event{At: time.Now()})

// 传输适配：session.AsSink()（TCP）/ stream.AsSink()（SSE）/ push.StreamSink{Stream: serverStream}
```

### TCP：出站 JSON-RPC notification

handler 通过 `tcp.SessionFromContext(ctx)` 拿到**本连接的推送会话**并可在请求结束后持有它：

```go
// "events.subscribe" handler 内
session := tcptransport.SessionFromContext(ctx)
if session == nil {
    return nil, apierror.New(apierror.KindInvalidArgument, "requires the TCP transport")
}
broker.Add(session) // 应用自己的订阅表

// 之后任何时候（连接存活期间）
err := session.Notify("events.pong", map[string]any{"at": time.Now()})
// 或类型化形式 session.NotifyTyped("events.pong", Pong{At: ...})
```

背压与生命周期：

- 每连接一个**有界发送队列**（`tcp.notifyQueueSize`，默认 256），独立 pump goroutine 写出；
- 队列满时按 `tcp.notifyPolicy` 处理：`drop`（默认，返回 `ErrNotifyDropped` + `gosvc_notify_dropped_total{reason="queue_full"}`）或 `disconnect`（断开慢消费者，客户端重连重订阅）；
- 连接关闭后 `Notify` 返回 `ErrSessionClosed`（pump 的 reaper 会回收死会话）；
- shutdown 时先广播 going-away（`gosvc.shutdown` notification）再 drain；
- 会话**惰性创建**：不调用 `SessionFromContext` 的连接零开销（无队列、无 goroutine）。

客户端用 `jsonrpc.NewTCPClient(addr, jsonrpc.WithNotificationHandler(fn))` 接收服务端推送；`fn(method, params)` 在读循环上回调，不要阻塞。

### SSE：HTTP 单向推送

`httptransport.RegisterSSE` 在普通 GET 路由上输出 `text/event-stream`，wire 处理复用 Hertz 官方 `protocol/sse`。它先发送一条 `:connected` 注释帧，让空闲流也能立即返回响应头。**鉴权、限流、追踪、访问日志全部照旧生效**（就是普通路由）；gosvc 开启了 Hertz 的断连感知，客户端断开时 `stream.Done()` 触发，handler 应尽快返回：

```go
application.RegisterHTTP(func(h *server.Hertz) {
    httptransport.RegisterSSE(h, "/api/v1/events", application.Metrics(),
        func(ctx context.Context, stream *httptransport.SSEStream) {
            if err := stream.Send("greet", payload); err != nil { return } // 客户端已断开
            heartbeat := time.NewTicker(httptransport.SSEHeartbeat)
            defer heartbeat.Stop()
            for {
                select {
                case <-stream.Done(): return
                case <-heartbeat.C:
                    if err := stream.Ping(); err != nil { return }
                }
            }
        })
})
```

慢消费者由 `http.writeTimeout` 兜底（写不出去就断）；长时间空闲请靠 `Ping` 心跳防代理掐连接。

### gRPC：streaming

原生能力：在 proto 里声明 `stream` RPC 即可，**unary 与 stream 拦截器链完全一致**（request-id / trace / logging / metrics / recovery / auth / rate limit）。日志和指标覆盖认证拒绝及 panic 恢复后的最终状态。注意 stream 的鉴权错误在首个 `Recv()` 上浮现（stream 惰性建立）。示例见 `examples/app` 的 `WatchGreetings`。

### 明确不做

- **WebSocket**：双向交互才需要，握手鉴权/子协议/心跳是一整套新面，等真实需求出现再说；
- **HTTP/2 server push**：已废弃的浏览器特性，与业务推送无关；
- JSON-RPC over HTTP 保持无状态请求-响应，不提供推送（要推送用 SSE 或 TCP）。

## 扩展点：Codec / 连接 Callback / Dispatcher 中间件

三者是同一模式——「库内留缝」，但层级不同：**Codec 管字节怎么变成调用**（传输内）、
**Callbacks 管连接何时生/死**（传输内）、**Middleware 管方法怎么被拦截**（Dispatcher 层，
天然覆盖 HTTP `/rpc` 与 TCP 全部路径）。全部默认关闭，不设时行为逐字节不变。

### TCP 帧方言：`tcp.Codec`

连接管理/背压/会话/优雅退出与线上方言解耦。自定义协议（如 stratum：无 `jsonrpc` 字段、
位置参数、错误是 `[code,msg,data]` 数组、通知 `id:null`）只需实现四个方法：

```go
type Codec interface {
    Name() string
    Decode(body []byte) (Call, error)                            // 一帧一调用（batch 留在默认路径）
    Encode(call Call, result any, callErr error) ([]byte, error) // 零 Call + 非空 callErr = 协议级错误
    EncodeNotification(method string, params any) ([]byte, error)
}

application, _ := gosvc.New(cfg, gosvc.WithTCPCodec(stratum.Codec{}))
```

接线面：`busy/notReady/tooLarge/internal` 服务端错误帧、`Session.Notify`、shutdown 广播
全部走 codec 输出方言；派发走 `Dispatcher.Invoke`（同一张方法表 + 中间件 + observer +
指标）。handler 返回的非 apierror 错误先被包装成 `internal`（防方言外漏内部文本），
未注册方法原样透传 `jsonrpc.ErrMethodNotFound` 哨兵由 codec 映射。注意 `Encode` 会在
事件循环上为协议级错误帧调用，必须快且非阻塞。

### 连接生命周期：`tcp.Callbacks`

惰性 Session + reaper 适合「可能订阅」的连接；按连接建注册表（如矿机表）需要可靠的
生/死事件。`OnDisconnect` 对端 EOF、服务端 reject、推送策略、shutdown 一律**恰好一次**
（基于 netpoll `AddCloseCallback`，覆盖自关闭路径——event-loop 级 OnDisconnect 只管对端
关闭，不够）：

```go
gosvc.WithTCPCallbacks(callbacks) // OnConnect 后连接的请求 ctx 以其返回值为父
```

- `OnConnect` panic → recover + 断开连接；`OnDisconnect` panic → 仅记日志；
- 有 Callbacks 时 Session **急建**（OnConnect 即可拿到推送句柄），无 Callbacks 保持惰性；
- `OnDisconnect` 的 reason：对端关闭 = nil；服务端关闭 = 对应 apierror；draining 中 =
  `tcp.ErrServerShutdown`。

### 逐方法拦截：`jsonrpc.Middleware`

```go
// 等价于在 RegisterJSONRPC 里调用 d.UseFor(...)
gosvc.WithJSONRPCMiddleware(myMiddleware...)
d.Use(func(next jsonrpc.HandlerFunc) jsonrpc.HandlerFunc { ... })      // 全局
d.UseFor("mining.submit", func(next jsonrpc.HandlerFunc) jsonrpc.HandlerFunc {
    return func(ctx context.Context, params json.RawMessage) (any, error) {
        if s := tcp.SessionFromContext(ctx); s == nil || !authorized(s) {
            return nil, apierror.New(apierror.KindPermissionDenied, "authorize first")
        }
        return next(ctx, params) // params 原样透传，不假设命名参数
    }
})
```

中间件运行在传输 recover 之内、ctx 增强（request_id/trace）之后：panic 变 internal
error、日志字段可用。TCP 默认不鉴权——per-method middleware 正是补 TCP 鉴权的自然位置。
`SetObserver`（指标缝）与 middleware 并存：收敛成内置 middleware 会丢掉默认路径上
malformed 请求的指标覆盖，违背零变化约束，故保留。

### 派发控制与传输身份

- **串行派发**：协议有「同一连接内先后依赖」（stratum 的 `authorize` → `submit`）时，
  codec 实现可选接口 `tcp.Serial`（`SerialPerConn() bool`）——该连接的帧严格按到达顺序
  执行（每连接 done-channel 链实现，零额外 goroutine）；默认路径保持并发，因为 JSON-RPC
  客户端本就必须容忍响应乱序。
- **传输身份**：`jsonrpc.TransportFromContext(ctx)` 返回 `"tcp"` / `"http"`，中间件可按
  传输分支（例如 TCP 无传输层鉴权 → 严格模式）。
- **按传输方法表**：`gosvc.WithTCPDispatcher(d)` 给 TCP 一张独立方法表（app 自建并注册），
  把 `mining.*` 这类方言方法挡在 HTTP `/rpc` 之外，也让两个表各自 `Use` 实现按传输的
  中间件隔离。观测零配置：`New` 会给独立表装上默认 RPC 指标 observer（`Invoke`/`Serve`
  都会计入 `gosvc_jsonrpc_requests_total`），除非 app 在 `New` 之前 `SetObserver`
  换成了自己的。默认仍共享。

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
handlerTimeout = "0s"    # 可选：单请求业务预算（0 关闭）；handler 需响应 ctx 取消

# CORS（浏览器客户端用）。allowOrigins = ["*"] 面向公共 API；也可列具体源。
# allowCredentials 与 "*" 同用时回显请求源（浏览器拒绝 "*" + credentials）；
# maxAge 缓存 preflight 响应（0 不发该 header，每个请求都会触发 preflight）。
[http.cors]
enabled = true
allowOrigins = ["*"]
allowCredentials = false
maxAge = "10m"

# 可选：HTTP listener 上的 TLS（certFile/keyFile 必须成对出现）。修改需重启。
# [http.tls]
# certFile = "certs/server.crt"
# keyFile = "certs/server.key"

[grpc]
host = "0.0.0.0"
port = 9090
shutdownTimeout = "10s"

# 可选：gRPC listener 上的 TLS。修改需重启。
# [grpc.tls]
# certFile = "certs/server.crt"
# keyFile = "certs/server.key"

[tcp]
enabled = false
host = "0.0.0.0"
port = 7070
workers = 4
queueSize = 1024
maxFrameBytes = 1048576
readTimeout = "60s"
handlerTimeout = "5s"     # 单个请求的业务处理预算
shutdownTimeout = "5s"
notifyQueueSize = 256     # 每连接出站 notification 队列上限
notifyPolicy = "drop"     # 队列满时：drop（丢弃+计数）| disconnect（断开慢消费者）

[admin]
host = "127.0.0.1"
port = 6060
# 可选：设置后所有 admin 请求都要求 "Authorization: Bearer <token>"。
# 仅在 loopback / 内网监听时才可以留空。修改需重启。
# token = "change-me"

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
- 使用：`app.Postgres()` 返回当前池的快照；需要跨热重载安全执行的查询用
  `app.WithPostgres(func(db *postgres.DB) error { ... })`，运行时会在回调结束后才关闭旧池。

### 就绪探针

`readyz` 会聚合命名依赖探针（PostgreSQL 启用时自动注册），返回逐项状态：

```json
{"status":"not_ready","checks":{"postgres":"dial tcp 127.0.0.1:5432: connect: connection refused"}}
```

应用也可以注册自己的探针：

```go
app.Health().AddCheck("redis", func(ctx context.Context) error { return redisClient.Ping(ctx).Err() })
```

JSON-RPC 客户端可以调内置方法 `system.health` 获取同样内容（`{"ready":..."checks":{...}}`）。
业务 HTTP `/readyz` 也检查这些依赖，但只返回简要状态；具体错误留在 admin 端口。
gRPC health 的 `Check` / `List` 实时检查相同依赖，覆盖整体及已注册服务；`Watch` 通过每秒一次的共享检查更新状态，退出时立即发布 `NOT_SERVING`。
依赖探针共享最长 3 秒的检查预算，并须遵守 ctx；HTTP / gRPC 健康端点不消耗业务限流额度。
容器健康检查从进程内自拨推荐用 `health.Probe(ctx, url)`（示例 app 的 `-healthcheck` 旗标即基于它）。

### TCP 指标

TCP 传输的拒绝路径在 Prometheus 上有独立序列：`gosvc_tcp_rejects_total{reason}`
（`not_ready` / `busy` / `too_large` / `internal`），加上 `gosvc_tcp_connections` 连接数 gauge；
成功派发的方法仍由共享的 `gosvc_jsonrpc_requests_total` 覆盖。

### 热更新时重建连接

`storage.*` 变化时运行时会**新建连接 → 切换 → 优雅关闭旧连接**（recorder 先 flush 再关闭）：

- 新连接建立失败（例如 DSN 写错、数据库不可达）时保留旧连接，日志记录 error，手动重载返回错误，服务不中断；
- **有效配置同步回滚**：`app.Config()` 与 `/debug/config` 始终描述真实在用的连接，不会展示一个从未装上的池；下次重载会再次尝试该变更；
- `app.Store()` / `app.Postgres()` 返回调用时的快照，不应长期缓存；在
  `app.WithStore` / `app.WithPostgres` 回调内使用连接，热重载会等待回调结束；
- 需要长期持有 recorder 时使用 `app.StableRecorder()`，它会随存储切换；
- 池指标通过 provider 采集，切换后无需重新注册。

## 运维端点（admin 端口）

| 端点 | 说明 |
|---|---|
| `GET /healthz` / `GET /readyz` | 存活 / 就绪（含依赖探针明细；k8s 探针可用 `app -healthcheck <url>`，实现即 `health.Probe`） |
| `GET /metrics` | Prometheus（HTTP / JSON-RPC / gRPC / TCP 拒绝与连接数 / Go runtime） |
| `GET /debug/pprof/*` | CPU、heap、goroutine |
| `GET/PUT /debug/loglevel` | 查看/修改日志级别 |
| `GET /debug/logstats` | 采样计数 |
| `GET /debug/config` | 当前生效配置（密钥脱敏） |
| `POST /debug/reload` | 手动触发配置重载（ConfigMap 场景） |
| `GET /debug/ts?metric=...&minutes=60` | Redis 分钟桶查询 |

pprof、配置查看和 reload 都是高权限操作：**设置 `[admin].token` 后所有 admin 请求都要求
`Authorization: Bearer <token>`**（constant-time 比较，`GOSVC_ADMIN_TOKEN` 环境变量亦可），
token 在 `/debug/config` 中脱敏。不设 token 时请保持 admin 只绑 loopback / 内网；
`deploy/docker-compose.yml` 也把 6060 映射限制在 `127.0.0.1`。

## 示例应用

```bash
make run          # 启动 examples/app（四协议 + 可观测性）
make bench        # rest 压测
go test ./...     # 单元 + 端到端（含热更新、采样、认证、admin token）
(cd examples/kitex && go run ./cmd -mode server)   # Kitex 示例（独立 module，在其目录内运行）
```

`examples/app` 演示了推荐的分层：`config.go`（嵌入配置）、`bindings.go`（三协议绑定）、
`greeter/`（领域服务）、`cmd/`（入口与压测器）、`e2e_test.go`（端到端测试）。

## 部署

```bash
make docker
docker compose -f deploy/docker-compose.yml up --build
```

distroless + 非 root；admin 端口默认只绑 `127.0.0.1`（compose 同样只映射到宿主机 loopback）；
TCP 传输不做 TLS（netpoll 需要裸连接），请在网关终止；HTTP / gRPC 监听器可用 `[http.tls]` / `[grpc.tls]`
直接启用 TLS（修改需重启，证书热轮换见 Roadmap）。

## 设计取舍

- **框架类型出现在 API 里**（`*server.Hertz`、`*grpc.Server`）：换取零额外抽象和完整的框架能力；
  想换框架时只需替换 transport 包；
- **一个共享 Dispatcher**：HTTP `/rpc` 与 TCP 方法只注册一次，指标也只记一份；批量默认上限 128
  防止单个请求无限占用 worker；batch 内部顺序执行，一个慢方法会拖慢同批的后续请求；
- **认证需显式开启**：默认 `auth.enabled = false`，而 HTTP/gRPC 默认监听所有地址；
  对外部署应启用认证或在可信网关完成鉴权。启用后 healthz/readyz 为白名单，
  `WithPublicPaths` 既接受字面路径也接受路由模式（`/api/v1/orders/:id`）；
  Dispatcher 层可用 `auth.JSONRPCMiddleware` 为 TCP 等无 header 的路径补齐鉴权；
- **gRPC 同时链 unary 与 stream 拦截器**：request id / trace / logging / metrics / recovery / auth /
  rate limit 对 streaming RPC 同样生效；
- **限流 key 用直连对端 IP**（HTTP `RemoteAddr`、gRPC `peer`、TCP 去端口），而不是可被
  `X-Forwarded-For` 伪造的 `ClientIP`：直连暴露时无法通过换头绕过；部署在可信代理后面意味着
  共享一个桶（fail-closed），需要按真实客户端限流就在代理层做；
- **TCP 传输不做认证**：定位是内网高性能通道（网关终止 TLS/做认证）；对外请走 HTTP/gRPC，
  或在 Dispatcher 层用 `auth.JSONRPCMiddleware` 逐方法补齐；
- **HTTP/gRPC 可选 TLS，TCP 不做**：`[http.tls]` 使用 Hertz 的 TLS / standard transport，
  `[grpc.tls]` 使用 gRPC credentials（含 ALPN h2）；证书启动时加载，修改配置需重启。TCP 依赖 netpoll 的裸连接，TLS 需要整个握手状态机进事件循环，
  仍然交给网关终止；
- **TCP 帧长逐帧限制**：解析累计长度超过 `maxFrameBytes` 后拒绝并断连；netpoll 仍可能先缓冲已到达的字节，因此它不是 socket 级内存配额；
  not-ready / 限流路径回一帧后立即断连，避免 level-triggered 事件循环自旋；
- **优雅退出 join 到底**：每个传输的 `Serve` 等自己的 drain 结束才返回；业务 handler 若长期阻塞且忽略 `ctx`，仍会阻止 `Run` 返回；
  `http.handlerTimeout` / `tcp.handlerTimeout` 都只把预算写进 ctx，不强制中断阻塞的 handler；
- **进程级全局只碰一次**：Hertz 的 `hlog` 与 OpenTelemetry 的全局 TracerProvider 都是进程级的，
  库在首次构造时安装并动态跟随 `slog.SetDefault`；因此一个进程只应跑一个 `gosvc.App`（测试除外）；
- **热更新只覆盖库拥有的字段**：应用自己的段通过 `WithOnReload` 处理；storage 重建失败时有效配置回滚，
  两处 reload（fsnotify + 手动端点）串行执行；
- **客户端提供的 `X-Request-ID` 原样使用**：用于日志关联（输出已转义防注入），但意味着调用方可以
  伪造关联 id；不需要时用 `logging.NewRequestID()` 自行生成；
- **配置加载用泛型方法**：编译期保证 `*T` 可配置，避免运行时类型断言；
- **Kitex 示例是独立 module**（`examples/kitex/go.mod`）：Kitex 工具链与其依赖（thriftgo、
  dynamicgo 等十几个包）完全不进入库的 `go.mod`；测试经 `make test` / CI 单独运行。

## Roadmap

- [ ] 前端静态资源 embed 辅助（Hertz StaticFS + 构建产物）
- [ ] 把 trace_id 注入到 OTLP 日志导出（logs signal）
- [ ] 配置热更新支持 TLS 证书轮换

## 写一个新服务

- 逐步指南：**[docs/new-service.md](docs/new-service.md)**（配置、入口、协议绑定、存储、测试、部署清单）
- 可运行示例：`examples/app`；OpenCode 用户可加载技能 `gosvc-new-service`

## 重命名模块

```bash
scripts/rename-module.sh github.com/Tokimorphling/gosvc
make proto kitex     # 重新生成示例的 protobuf / Kitex 代码（kitex 在 examples/kitex 内执行）
```
