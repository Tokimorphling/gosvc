# 核心路径性能记录

测量日期：2026-10-04。环境：Apple M4、darwin/arm64、Go 1.27.1，`GOMAXPROCS=8`。
基线包含 TLS、readiness、退出预算等修复，尚未应用注册表快照、泛型调度器和探针快照重构。前后使用相同基准场景，各测 5 次，每次 300ms，下表取中位数；未同时运行 race 测试或 lint。

| 场景 | 基线 | 整理后 | 说明 |
|---|---:|---:|---|
| Dispatcher Invoke，无 observer | 37.06 ns/op | 7.127 ns/op | 0 alloc/op；省去读锁及无用计时 |
| Dispatcher Invoke，有 observer | 54.46 ns/op | 46.66 ns/op | 0 alloc/op；observer 为无操作函数 |
| Dispatcher 并发 Invoke，有 observer | 186.5 ns/op | 12.32 ns/op | 8 个 P 的吞吐均摊时间，不是请求延迟 |
| JSON-RPC Serve，有 observer | 1,153 ns/op | 1,184 ns/op | 包含 JSON 编解码；均为 25 alloc/op，没有观察到整体改善 |
| Readiness，无依赖 | 154.9 ns/op | 2.901 ns/op | 272 B / 4 alloc → 0 B / 0 alloc |
| Readiness，8 个立即成功的探针及明细 | 375.5 ns/op | 270.8 ns/op | 608 B / 6 alloc 不变；省去复制探针表 |
| 工作池，1024 个普通等待任务 | 127.239 μs/批 | 20.342 μs/批 | 包含入队、调度、等待同步，无业务 IO |
| 工作池，1024 个同 key 串行等待任务 | 152.164 μs/批 | 41.073 μs/批 | 同样包含入队和排空；保持串行顺序 |

这些数字只描述局部路径。observer 基准未包含真实 Prometheus 写入；队列基准不计启动时预分配；无依赖 readiness 不代表数据库探针的 IO 延迟。JSON-RPC 完整消息路径仍主要花在校验、编解码和分配上，不能把 Invoke 的改善比例直接当成整个服务的性能提升。

## 复现

```bash
make bench-core

# 等价命令
go test ./jsonrpc ./health ./internal/workerpool \
  -run '^$' \
  -bench 'BenchmarkDispatcher|BenchmarkReadiness|BenchmarkPoolBacklog' \
  -benchmem -benchtime=300ms -count=5 -cpu=8
```

原始输出：[基线](benchmarks/2026-10-04-before.txt)、[整理后](benchmarks/2026-10-04-after.txt)。源码基准随仓库保留，性能数据不作为 CI 的固定时间阈值，避免不同硬件和系统负载造成误报。

组件依赖也做了检查：此环境下 `go list -deps ./push` 的包数（含标准库）从 280 降为 41。它反映 push 不再通过观测实现间接引用 Prometheus、JSON-RPC 和 HTTP 客户端，不代表应用最终二进制同比缩小。
