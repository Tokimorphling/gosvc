# 组件边界与并发约束

`gosvc.App` 是组合入口，负责组装具体组件、注册协议和协调生命周期。底层组件不反向引用 `App`，业务服务通过参数、context 和窄接口使用能力。

| 组件 | 职责 | 依赖边界 |
|---|---|---|
| `app.go` / `options.go` | 组装、运行、公开访问器及选项 | 允许依赖各具体组件 |
| `app_registration.go` | HTTP、gRPC、JSON-RPC、admin 注册 | 泛型 helper 统一状态校验，保留各协议的具体类型 |
| `app_shutdown.go` / `internal/shutdown` | 清理 hook、共享 deadline、资源释放 | 不进入每次请求的业务派发路径 |
| `app_storage.go` / `app_reload.go` | 连接代际、lease、配置生效规则 | 资源关闭必须等待持有者退出 |
| `jsonrpc/registry.go` | 方法及 middleware 注册、表版本发布 | 不依赖 transport、认证或 Prometheus |
| `jsonrpc/dispatcher.go` | 协议校验、派发、批次和错误响应 | 通过函数 observer 输出观测结果 |
| `transport/*` | 收发、协议适配、中间件及连接生命周期 | 调用共享业务方法，不拥有领域规则 |
| `push` | 泛型事件广播、队列、背压与订阅生命周期 | 只依赖 `Sink` / `Observer` 小接口及状态工具，无 Prometheus 或 JSON-RPC 依赖 |
| `observability` | Prometheus registry 与指标实现 | 实现消费方需要的观测接口 |
| `health` | 生命周期标志、依赖探针、检查预算 | 与具体服务器、数据库驱动无关 |
| `auth` | 凭据校验与身份 context | 返回统一错误；同包的 JSON-RPC adapter 依赖 dispatcher API |
| `config` | 配置结构、默认值、文件与环境覆盖、验证 | 不创建连接或服务器；格式及日志级别校验使用 `slogx` |
| `logging` / `slogx` | 日志装配与 handler、采样、sink 切换 | `slogx` 只依赖标准库；`logging` 适配配置与 trace context |
| `ratelimit` | 每 key 可变 token bucket、过期回收 | 不识别具体传输，传输负责选择 key |
| `store` / `store/*` | recorder/query 小接口及具体连接器 | 传输依赖接口；具体 Redis/PostgreSQL 连接由组合入口管理 |
| `internal/workerpool` | 有界任务执行、按 key 串行调度 | 不引用 TCP 类型；TCP 使用 `Pool[*connState]` |
| `state` / `apierror` | 泛型不可变快照、统一错误分类 | 基础组件，依赖标准库 |

## 泛型放在哪里

- 业务边界：`RegisterTyped[Req, Resp]`、`Client.Call[Req, Resp]`、`Broker[T]`。
- 状态边界：`Snapshot[T]` 发布不可变值；调用方不能原地修改已经发布的 map、slice 或指针内容。
- 调度边界：`Pool[K comparable]` 和内部 scheduler 使用具体 key 类型；零值 key 也能参与串行调度。
- 存储边界：lease helper 接收 `*T`，编译器保证连接是指针，直接比较 nil，不再装箱成 `any` 比较。
- 注册边界：一个泛型 helper 维护所有传输一致的注册时机规则。

JSON-RPC 动态方法表仍使用 `HandlerFunc`，不同传输的推送目标仍使用 `Sink`。这些位置需要容纳异构实现；泛型保证调用方类型，接口承担运行时分派。配置文件解析、日志任意属性和 typed-nil 兼容辅助中的反射也保留在各自适用位置。

## 读多写少的状态

Dispatcher 的注册锁只串行化写入。写入复制需要改变的表或切片，发布新的原子快照；请求读取一次方法表，并使用该版本的 handler 和 observer。中间件链通过 `sync.OnceValue` 惰性构建，构建过程中不持有注册锁，构造器可以读取方法表或注册其他方法。

新增方法不会清空现有链；`UseFor` 只使目标方法的链失效；全局 `Use` 会为所有方法创建新链版本。在途请求完成旧版本，之后的请求读取新版本。写入和第一次构建承担额外分配，稳定调用不为方法表加锁或分配。频繁动态注册大量方法时，写入复制的成本仍需评估。

健康探针也按不可变切片发布注册变更。`Check(ctx)` 生成独立诊断明细，供 admin 和 `system.health` 使用；`Healthy(ctx)` 执行相同探针但不创建明细，供 HTTP/gRPC 状态检查使用。无探针时无需创建 timer。已经更短的调用方 deadline 直接复用。gRPC 服务名称在注册结束后缓存，探针不反复构造服务描述符 map。

## 有界调度

工作池预分配 `queueSize` 个等待槽位，并维护空闲、ready 和每个串行 key 的等待队列。队列只链接槽位索引，入队、出队、让同 key 后继就绪都是 O(1)，不扫描其他 key 或搬移剩余任务。处理完成后，下一个同 key 任务进入 ready 队列尾部；不同 key 有机会执行，同 key 保持提交顺序。

队列容量是全局等待任务数，包含被同 key 前驱阻塞的任务，不包含正在运行的任务。预分配提高启动时的固定内存占用，换取运行期间可预测的队列成本。slot 出队后立即清除闭包与连接 key 引用，避免无谓延长它们的存活期。

## 生命周期与可变资源

连接池 lease、recorder 切换、订阅登记和 token bucket 仍使用锁。它们有可变状态和资源关闭顺序，直接替换为原子指针会破坏持有者的生命周期。中间件与 observer 也必须遵守其并发契约。

`push.WithMetrics` / `SetMetrics` 接收 `push.Observer`；现有传入 `*observability.Metrics` 的普通调用无需修改，也可传入独立监控实现。若保存这两个 API 的精确函数类型，需要把参数类型改为 `push.Observer`。observer 必须并发安全、快速返回，不能重入 broker。

## 验证

回归测试覆盖注册和请求并发、在途快照、middleware 构造器重入、独立链失效、串行 key 顺序、队列容量及槽位复用、探针注册并发、指标实现替换。完整传输行为继续由原有 race 和端到端测试验证。

运行 `make bench-core` 查看局部开销；方法、数据及限制见 [性能记录](performance.md)。真实服务的 JSON 编解码、日志、指标、下游 IO 和业务开销需单独压测。
