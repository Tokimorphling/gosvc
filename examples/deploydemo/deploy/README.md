# 临时生产验收部署

本目录的 unit 用于一次性验收：不配置开机启用、不自动重启，并在 15 分钟后停止。
它以 DynamicUser 运行，监听三个 loopback 端口，只允许 loopback 网络通信，并设置内存与 CPU 上限。

框架由本模块的 go.mod 锁定到通过 `go get github.com/Tokimorphling/gosvc@main` 获取的版本，未使用本地 replace。
构建入口为 `./cmd/deploydemo`；ARM64 Linux 示例：

```bash
GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -trimpath \
  -ldflags '-s -w -X main.version=your-release' \
  -o gosvc-demo ./cmd/deploydemo
```

服务端布局：

- `/opt/gosvc-demo/gosvc-demo`：只读二进制。
- `/etc/gosvc-demo/config.toml`：生产配置。
- `/etc/gosvc-demo/demo.env`：root 0600，包含随机 `DEMO_AUTH_API_KEYS`，不入库。
- `/var/lib/gosvc-demo/tasks.json`：systemd 管理的私有持久化数据。

公网验收只通过现有 Caddy 的 HTTPS 路径反代；整个路径加临时 Basic Authentication，业务 API 仍要求 API key。
配置使用带标记的独立路由块，修改前备份，validate 成功后 reload。公开地址不能映射工作区、调试器、admin 或 gRPC 端口。

每次验收必须准备独立的超时清理任务，并在完成后停止 unit、删除临时 Caddy 路由、撤销临时凭据。
确认进程和三个监听端口均已消失、Caddy 恢复原配置，再清理上传目录与临时 SSH 会话。
不得把此临时 unit 当作长期运行配置直接启用。
