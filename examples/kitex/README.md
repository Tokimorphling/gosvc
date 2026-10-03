# Kitex 可选层示例

这个目录演示如何在本模板之上加一层 **服务间 RPC**（Kitex），同时保持核心服务不受影响：

- 这是一个 **独立 Go module**（`examples/kitex/go.mod`）：Kitex 工具链及其依赖
  完全不进入主库 `go.mod`，主库的依赖体积不受影响；
- 主库任何代码都不 import Kitex，主二进制不会变大；
- IDL 在 `idl/echo.thrift`，生成代码在 `api/echo/`；
- `cmd/` 提供手动运行的 server/client（在本目录内运行）。

## 生成代码

```bash
# 一次性安装 CLI
go install github.com/cloudwego/kitex/tool/cmd/kitex@latest

# 修改 IDL 后重新生成（在本目录内运行；等价于在仓库根目录 make kitex）
cd examples/kitex
kitex -module github.com/Tokimorphling/gosvc/examples/kitex -gen-path api idl/echo.thrift
```

## 运行

```bash
cd examples/kitex
go run ./cmd -mode server -addr 127.0.0.1:9091
go run ./cmd -mode client -addr 127.0.0.1:9091 -message hi
go test ./...
```

## 服务发现

`resolver.go` 里的 `StaticResolver` 实现了 `discovery.Resolver`，用于演示扩展点。
生产环境换成社区实现即可，接口不变：

```go
import (
    etcd "github.com/kitex-contrib/registry-etcd"
    "github.com/cloudwego/kitex/client"
)

r, err := etcd.NewEtcdResolver([]string{"127.0.0.1:2379"})
cli, err := echoservice.NewClient("echo", client.WithResolver(r))
```

## 晋升为模板的一等公民

如果 Kitex 成为常用形态，可以按下面步骤把它接进主程序（约半小时）：

1. 把 `transport/kitex` 作为新 transport 包，仿照 `transport/grpc` 的
   `Options`/`Serve(ctx)`/`Addr()` 结构包装 `echoservice.NewServer`；
2. 在 `config.Config` 增加 `KitexConfig`（enabled/host/port/shutdownTimeout），并在 `Validate` 中校验；
3. 在 `app.go` 里按 `cfg.Kitex.Enabled` 构建并加入 `errgroup`；
4. 中间件直接复用领域层与 `apierror` 映射：Kitex 的 `server.WithMiddleware` 与 gRPC 拦截器一一对应；
5. 日志：`klog.SetLogger(...)` 桥接到本模板的 slog（参考 `transport/http/hlog_adapter.go`）。

## 注意

- Kitex 默认使用 netpoll 传输，与模板里 `transport/tcp` 同源；
- Kitex 自带连接池、超时、熔断与重试（`kitex-contrib`），适合内部 RPC，不适合直接承载
  REST/JSON-RPC 这类对外协议；
- 独立 module 已把 Kitex 依赖隔离在本目录；若把示例接进主程序，主库 `go.mod` 才会引入
  这些依赖，确认团队接受后再晋升。
