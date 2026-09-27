# 用 gosvc 写一个新 Web 服务

这份指南带你从零创建一个基于 `gosvc` 的服务：四协议（REST / JSON-RPC 2.0 / gRPC / TCP）、
日志、指标、追踪、认证、限流、存储连接与热更新全部由库提供，你只写业务。

> 完整可运行参考：`examples/app/`（greeter 示例，含端到端测试）。

## 0. 前置

- Go **1.27+**（库使用泛型方法与 `errors.AsType`）；
- 库可 import：发布到你的仓库后 `go get github.com/Tokimorphling/gosvc@latest`，本地开发可用 `replace`：

  ```bash
  go mod edit -replace github.com/Tokimorphling/gosvc=/path/to/gosvc
  ```

## 1. 创建模块

```bash
mkdir myservice && cd myservice
go mod init github.com/you/myservice
go get github.com/Tokimorphling/gosvc
```

推荐目录：

```
myservice/
├── config.toml          # 运行时配置（TOML）
├── main.go              # 加载配置、组装、Run
├── config.go            # Config 嵌入 gosvc.Config + 应用段
├── version.go           # 版本变量（ldflags 注入）
├── service/             # 领域逻辑（不依赖任何传输）
├── bindings.go          # HTTP / JSON-RPC / gRPC 绑定
├── Dockerfile
└── Makefile
```

## 2. 配置：嵌入 + 严格模式

```go
// config.go
package main

import (
	"errors"

	"github.com/Tokimorphling/gosvc"
)

type Config struct {
	gosvc.Config                       // 运行时配置（http/grpc/tcp/log/auth/storage/...）

	Database DatabaseConfig `json:"database" toml:"database"`
}

type DatabaseConfig struct {
	DSN string `json:"dsn" toml:"dsn"`
}

// SetDefaults 先套用运行时默认值，再补应用默认值。
func (c *Config) SetDefaults() {
	c.Config.SetDefaults()
	if c.Database.DSN == "" {
		c.Database.DSN = "postgres://localhost/app?sslmode=disable"
	}
}

// Validate 先跑运行时校验，再加应用校验。
func (c *Config) Validate() error {
	if err := c.Config.Validate(); err != nil {
		return err
	}
	if c.Database.DSN == "" {
		return errors.New("database.dsn is required")
	}
	return nil
}
```

```toml
# config.toml
[service]
name = "myservice"
env = "dev"

[http]
host = "0.0.0.0"
port = 8080

[admin]
host = "127.0.0.1"
port = 6060

[log]
level = "info"
format = "auto"

[database]
dsn = "postgres://user:pass@localhost/app?sslmode=disable"
```

环境变量覆盖（前缀可配）：`MYAPP_SERVICE_NAME`、`MYAPP_HTTP_ADDR`、`MYAPP_AUTH_API_KEYS`、
`MYAPP_REDIS_ADDR`、`MYAPP_POSTGRES_DSN`、`MYAPP_OTLP_ENDPOINT`、`MYAPP_LOG_LEVEL`、`MYAPP_ADMIN_TOKEN`。

> admin 端口暴露 pprof / 配置 / reload 等高权限端点：绑在内网或 loopback，或设置
> `[admin].token`（`Authorization: Bearer <token>`）后再暴露。

> `Strict: true` 会拒绝未知键——启动时抓拼写错误；热更新走非严格模式，好让应用段与运行时配置共存。

## 3. 入口：加载 → 组装 → 注册 → Run

```go
// main.go
package main

import (
	"context"
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

const (
	configPath = "config.toml"
	envPrefix  = "MYAPP"
)

func main() {
	cfg, err := (gosvc.Source{Path: configPath, EnvPrefix: envPrefix, Strict: true}).Load[Config]()
	if err != nil {
		slog.Error("load config", "error", err)
		os.Exit(1)
	}

	logHandle, err := logging.New(cfg.Log, cfg.Service.Name, cfg.Service.Env, Version)
	if err != nil {
		slog.Error("build logger", "error", err)
		os.Exit(1)
	}
	slog.SetDefault(logHandle.Logger()) // 让 context-scoped logger 共用同一条 handler 链

	app, err := gosvc.New(&cfg.Config,
		gosvc.WithLogger(logHandle),
		gosvc.WithVersion(FullVersion()),
		gosvc.WithHotReload(configPath, envPrefix),
	)
	if err != nil {
		slog.Error("build runtime", "error", err)
		os.Exit(1)
	}

	// 运行时已建好连接池：直接拿 app.Postgres() / app.Store() / app.Metrics()
	svc := service.New(app.Postgres())

	if err := app.RegisterHTTP(func(h *server.Hertz) {
		registerHTTP(h, svc)
	}); err != nil {
		slog.Error("register http", "error", err)
		os.Exit(1)
	}

	if err := app.RegisterJSONRPC(func(d *jsonrpc.Dispatcher) {
		registerJSONRPC(d, svc)
	}); err != nil {
		slog.Error("register jsonrpc", "error", err)
		os.Exit(1)
	}

	if err := app.RegisterGRPC(func(s *ggrpc.Server) {
		orderv1.RegisterOrderServiceServer(s, newOrderServer(svc))
	}); err != nil {
		slog.Error("register grpc", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := app.Run(ctx); err != nil {
		slog.Error("run", "error", err)
		os.Exit(1)
	}
}
```

```go
// version.go
package main

import "fmt"

var (
	Version   = "dev"
	Commit    = "none"
	BuildTime = "unknown"
)

func FullVersion() string {
	return fmt.Sprintf("%s (commit=%s built=%s)", Version, Commit, BuildTime)
}
```

构建时注入（Makefile）：

```make
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT  ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
LDFLAGS := -s -w \
  -X main.Version=$(VERSION) -X main.Commit=$(COMMIT) \
  -X main.BuildTime=$(shell date -u +%Y-%m-%dT%H:%M:%SZ)
build:
	CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o bin/myservice .
```

## 4. 领域层与协议绑定

领域层不 import 任何传输包，错误用 `apierror`：

```go
// service/service.go
package service

import (
	"context"

	"github.com/Tokimorphling/gosvc/apierror"
)

type Order struct {
	ID   int64  `json:"id"`
	Item string `json:"item"`
}

type GetRequest struct {
	ID int64 `json:"id"`
}

type Service struct{ /* db, cache, ... */ }

func (s *Service) Get(ctx context.Context, id int64) (*Order, error) {
	if id <= 0 {
		return nil, apierror.New(apierror.KindInvalidArgument, "id must be positive")
	}
	order := lookup(ctx, id) // 你的业务
	if order == nil {
		return nil, apierror.Newf(apierror.KindNotFound, "order %d not found", id)
	}
	return order, nil
}
```

**REST**（Hertz handler + 统一错误映射）：

```go
// bindings.go
func registerHTTP(h *server.Hertz, svc *service.Service) {
	api := h.Group("/api/v1")
	api.GET("/orders/:id", func(ctx context.Context, c *app.RequestContext) {
		id, err := strconv.ParseInt(c.Param("id"), 10, 64)
		if err != nil {
			httptransport.WriteError(ctx, c, apierror.New(apierror.KindInvalidArgument, "id must be an integer"))
			return
		}
		order, err := svc.Get(ctx, id)
		if err != nil {
			httptransport.WriteError(ctx, c, err)
			return
		}
		c.JSON(200, order)
	})
}
```

**JSON-RPC**（类型化注册；同一套方法自动同时服务 HTTP `/rpc` 与 TCP）：

```go
func registerJSONRPC(d *jsonrpc.Dispatcher, svc *service.Service) {
	d.RegisterTyped("orders.get", func(ctx context.Context, req service.GetRequest) (*service.Order, error) {
		return svc.Get(ctx, req.ID)
	})
}
```

**gRPC**（生成代码 + `grpctransport.ToStatus`）：

```go
func (s *orderServer) Get(ctx context.Context, req *orderv1.GetRequest) (*orderv1.GetResponse, error) {
	order, err := s.svc.Get(ctx, req.GetId())
	if err != nil {
		return nil, grpctransport.ToStatus(err)
	}
	return &orderv1.GetResponse{Id: order.ID, Item: order.Item}, nil
}
```

错误映射（各协议自动一致，内部错误不泄漏）：

| Kind | HTTP | JSON-RPC | gRPC |
|---|---|---|---|
| `invalid_argument` | 400 | -32602 | `InvalidArgument` |
| `unauthenticated` | 401 | -32006 | `Unauthenticated` |
| `not_found` | 404 | -32001 | `NotFound` |
| `conflict` | 409 | -32002 | `AlreadyExists` |
| `permission_denied` | 403 | -32003 | `PermissionDenied` |
| `rate_limited` | 429 | -32005 | `ResourceExhausted` |
| `unavailable` | 503 | -32004 | `Unavailable` |
| `internal` | 500 | -32603 | `Internal` |

## 5. 存储

```toml
[storage.postgres]
enabled = true
dsn = "postgres://user:pass@localhost/app?sslmode=disable"
maxOpenConns = 16

[storage.redis]
enabled = true
mode = "sentinel"          # single | cluster | sentinel
addrs = ["10.0.0.1:26379", "10.0.0.2:26379"]
masterName = "mymaster"
prefix = "myservice"
bucketTtl = "25h"
```

- `app.Postgres()` → `*postgres.DB`（`*sql.DB` 包装）；池指标 `gosvc_db_pool_*` 自动导出；
  Postgres 启用时 `/readyz` 自动带该依赖探针；
- `app.Store()` → Redis（分钟桶时序），`app.Recorder()` 用于自定义指标：

  ```go
  if recorder := app.Recorder(); recorder != nil {
      _ = recorder.Incr(ctx, "orders.created", 1)
  }
  ```
- 自定义探针：

  ```go
  app.Health().AddCheck("kafka", func(ctx context.Context) error { return producer.Ping(ctx) })
  ```

## 6. 可观测性

- **日志**：`log.format = "auto"`（dev 彩色 / 其它 JSON）；高 QPS 打开采样：
  `[log.sampling] enabled = true`；请求日志可独立走 sink：

  ```toml
  [log.access]
  enabled = true
  output = "file"
  [log.access.file]
  path = "logs/access.log"
  ```
- **指标**：`app.Metrics()` 注册自定义指标（Prometheus），`/metrics` 抓取；
- **追踪**：`[telemetry] enabled = true` + `otlpEndpoint`；HTTP/gRPC/TCP 的请求日志自动带
  `trace_id`/`span_id`，可直接从日志跳转到 Jaeger/Tempo。

## 7. 认证

```toml
[auth]
enabled = true
apiKeys = ["key-1"]

[auth.jwt]
secret = "at-least-16-chars"
issuer = "myservice"
```

- HTTP 读 `X-API-Key` 或 `Authorization: Bearer <jwt>`；gRPC 读同名 metadata；
- `/healthz`、`/readyz` 与 gRPC health/reflection 默认放行，其它公开路径用
  `gosvc.WithPublicPaths("/metrics")` 追加；
- 业务里取身份：`identity := auth.FromIdentity(ctx)`。

## 8. 热更新

带 `gosvc.WithHotReload(path, envPrefix)` 启动后，文件变化（含 k8s ConfigMap 的原子替换）自动生效：

| 变更 | 行为 |
|---|---|
| `log.*`（级别/格式/sink/轮转/采样/access 的格式与级别） | 热生效 |
| `auth.*` | 热生效（凭据原子替换） |
| `limiter.*` | 热生效 |
| `storage.*` | **重建连接**：新连接就绪后切换，旧连接优雅关闭；失败保留旧连接并记错误 |
| `service` / `http`（含 `http.cors`）/ `grpc` / `tcp`（含 `handlerTimeout`、`notifyQueueSize`、`notifyPolicy`）/ `admin`（含 `token`）/ `telemetry` / `log.access.enabled` | 需要重启，重载日志会列出 |

应用自己的段用 `WithOnReload` 处理：

```go
gosvc.WithOnReload(func(cfg *gosvc.Config) error {
	// 注意：这里拿到的是运行时配置；应用段请自行重新加载
	return nil
})
```

手动触发：`curl -X POST http://127.0.0.1:6060/debug/reload`；查看生效配置（脱敏）：
`curl http://127.0.0.1:6060/debug/config`。

## 9. 测试

- 领域层单测不依赖网络；
- 端到端：直接 `gosvc.New` + 临时端口（`port = 0`）+ 真实 HTTP/gRPC 调用，
  参考 `examples/app/e2e_test.go`（覆盖认证、采样、热更新、存储重建、trace 关联）；
- CI 至少跑 `go vet ./... && go test -race ./...`。

## 10. 部署检查清单

- [ ] `Source.Strict = true`（启动时抓配置拼写错误）
- [ ] 生产 `log.format = "json"`、`output = "stdout"`（平台收集；裸机再加 file + 轮转）
- [ ] 高流量：`log.sampling.enabled = true`、`log.access.enabled = true`
- [ ] `telemetry.enabled = true` 且 `sampleRatio` 按流量调（如 0.1）
- [ ] `auth.enabled = true`，密钥用 `*_AUTH_API_KEYS` 环境变量注入
- [ ] `storage.*` 配置 + `/readyz` 探针验证
- [ ] admin 端口只绑内网（默认 `127.0.0.1`），要暴露就设置 `admin.token`
- [ ] TCP 传输（如启用）在网关终止 TLS
- [ ] 需要推送时按传输选型：TCP `Session.Notify`（内部）/ SSE `RegisterSSE`（浏览器）/ gRPC `stream` RPC；订阅端点鉴权随现有中间件链
- [ ] 容器探针用 `myservice -healthcheck http://127.0.0.1:6060/healthz`

## 11. 常见坑

| 现象 | 原因 / 解决 |
|---|---|
| 请求日志没有 `trace_id` | 未启用 tracing，或没调用 `logging.WithTrace(ctx)`（库内已自动，自定义传输需自己加） |
| 采样不生效 | 采样只作用于被 handler 接收的记录：日志级别要允许该级别 |
| `app.Recorder()` 不为 nil 但 Redis 已禁用 | 用 `store.Nilable` 处理 typed-nil；库内部已处理，应用自定义时注意 |
| 改了 `log.access.enabled` 没生效 | 该开关需要重启；格式/输出/级别是热更新的 |
| 热更新后存储连接没变 | 看重载日志 `changed` / `restartRequired`；重建失败会保留旧连接并记 error |
