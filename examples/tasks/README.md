# 待办事项服务 sample

这是一个可以直接运行的小型业务服务，无需数据库或 Redis。支持创建、查询、列出、完成和删除任务；REST 与 JSON-RPC 调用同一个 `Service`。

任务保存在进程内存中，重启后清空；这个示例用于演示框架接入，不承担持久化。所有监听器默认只绑定本机。原来的 `examples/app` 保留为 gRPC、TCP、推送和存储等完整能力的示例。

## 启动

在仓库根目录执行：

```bash
go run ./examples/tasks/cmd/tasks -c examples/tasks/config.toml
```

HTTP / JSON-RPC 使用 `127.0.0.1:8080`，admin 使用 `127.0.0.1:6060`。`127.0.0.1:9090` 提供框架自带的 gRPC health/reflection，本示例未注册业务 gRPC 方法。按 Ctrl-C 或发送 SIGTERM 后，由框架负责退出。

## 试用 REST

```bash
# 创建：返回 201 和 {"id":1,"title":"上线第一个服务","done":false}
curl -sS http://127.0.0.1:8080/api/v1/tasks \
  -H 'Content-Type: application/json' \
  -d '{"title":"上线第一个服务"}'

# 列出 / 查询
curl -sS http://127.0.0.1:8080/api/v1/tasks
curl -sS http://127.0.0.1:8080/api/v1/tasks/1

# 完成：重复调用仍然成功
curl -sS -X POST http://127.0.0.1:8080/api/v1/tasks/1/complete

# 删除
curl -sS -X DELETE http://127.0.0.1:8080/api/v1/tasks/1
```

空标题、标题超长和非法 ID 返回 400；不存在的任务返回 404；达到任务容量上限返回 409，删除任务后可以继续创建。

## 试用 JSON-RPC

```bash
curl -sS http://127.0.0.1:8080/rpc \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tasks.create","params":{"title":"通过 RPC 创建"}}'

curl -sS http://127.0.0.1:8080/rpc \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":2,"method":"tasks.list","params":{}}'
```

方法为 `tasks.create/get/list/complete/delete`。`get`、`complete`、`delete` 的 params 为 `{"id":1}`。两种协议共享同一份状态和错误分类。

## 框架接管的部分

```bash
curl -sS http://127.0.0.1:8080/healthz
curl -sS http://127.0.0.1:8080/readyz
curl -sS http://127.0.0.1:6060/metrics
curl -sS http://127.0.0.1:6060/debug/config
```

日志、请求 ID、指标、超时、认证、限流、配置加载、运行时配置热更新和优雅退出由 gosvc 提供。开发者只定义业务配置、业务方法和协议绑定。

通过环境变量启用认证，并在 curl 中加入 `-H 'X-API-Key: local-demo-key'`：

```bash
TASKS_AUTH_API_KEYS=local-demo-key go run ./examples/tasks/cmd/tasks -c examples/tasks/config.toml
```

端口可用 `TASKS_HTTP_ADDR`、`TASKS_GRPC_ADDR`、`TASKS_ADMIN_ADDR` 覆盖。`[tasks]` 业务配置在启动时读取，修改后需重启；日志、认证、限流的热更新遵循框架规则。

## 文件分工

| 文件 | 内容 |
|---|---|
| `service.go` | 业务规则、并发安全的内存状态；不依赖传输包 |
| `app.go` | 注册 REST 和 JSON-RPC，映射参数和响应 |
| `config.go` / `config.toml` | 业务配置及运行时配置 |
| `cmd/tasks/main.go` | 加载配置、构建应用、接收信号 |
| `service_test.go` / `app_test.go` | 业务测试和跨协议集成测试 |

```bash
go test -race ./examples/tasks/...
go build -o bin/tasks ./examples/tasks/cmd/tasks
./bin/tasks -healthcheck http://127.0.0.1:6060/readyz
```
