# 生产环境临时验收记录

日期：2026-10-04（Asia/Shanghai）。状态：**测试完成，服务及公网入口已撤销**。

## 实际部署方式

- 独立 Go 模块，通过 `go get github.com/Tokimorphling/gosvc@main` 获取框架。
- 锁定版本：`v0.0.0-20261004064739-9962c21fcab9`，未使用本地 replace。
- 编译：Go 1.27.1、Linux ARM64、CGO disabled；静态资源嵌入二进制。
- 在已有 Caddy 的真实 Linux 服务器上运行原生 systemd 服务，通过现有 HTTPS 域名的 `/gosvc-demo/` 临时路径访问。
- 本次验证的是原生二进制部署，没有把此次结果当作 Docker 镜像验收。

## 安全与资源边界

- 三个监听器都绑定 loopback：18080 / 19090 / 16060。
- Caddy 对整个临时路径使用随机凭据的 Basic Authentication；业务 API 另需独立随机 API key。
- 明文临时凭据仅保存在服务器 root 0600 文件中，未写入源码、URL 或浏览器资产。
- DynamicUser、只读系统目录、私有临时目录、无额外 capability。
- cgroup 网络规则只允许 localhost；没有 SSH 端口转发或 workspace 映射。
- CPU 上限半核，内存上限 128 MiB，Go 软限制 80 MiB。
- 设置独立的 10 分钟清理计时器及服务自身的 15 分钟运行上限；正常验收在计时器到期前主动完成清理。

## 通过的检查

1. 公网未认证访问首页和脚本返回 401。
2. 通过网关认证但缺少 API key 的业务请求仍返回 401。
3. 真实 HTTPS 下首页、CSS、JS 加载成功，CSP 使用同源脚本策略。
4. admin /metrics、/debug/config 和 workspace 路径不通过 demo HTTP 入口公开。
5. REST 创建和完成任务成功，JSON-RPC 可读取同一份状态。
6. 实际 systemd restart 后，任务 ID、标题和完成状态均保留。
7. 测试任务已删除，数据恢复为空任务集。
8. systemd 在重启后的观察点报告 MemoryCurrent / MemoryPeak 为 9,715,712 字节；这是单次观察值，不是容量承诺。

本地还通过了三个模块的 lint、demo race 测试、桌面/手机静态视觉检查，以及带路径前缀的接口与重启持久化验证。

## 关闭确认

- 服务进程停止，18080 / 19090 / 16060 均不再监听。
- Caddy 配置内容的 SHA-256 与部署前完全一致，临时路由已删除。
- 临时认证凭据、systemd unit、服务器二进制、配置、测试数据和上传目录均已移除。
- 临时 SSH / 本地预览进程已结束，没有保留访问工作区的连接。
- 服务器仅保留 root 私有的非敏感验收结果和原始 Caddy 备份，以供审计。

重新运行应重新生成凭据并重新经过部署验收；本记录不表示存在一个仍开放的 demo 服务。
