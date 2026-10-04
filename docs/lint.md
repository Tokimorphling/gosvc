# Go 代码检查与现代化

VS Code Go 插件通过 gopls 提供类型检查和编辑器诊断。`errors.As can be simplified using AsType[...]` 来自 Go 官方 `modernize` 分析器的 `errorsastype` 规则。它建议使用泛型返回目标值，省去声明 target 再传二级指针的写法。

Go 1.26 起，`go fix` 已内置现代化分析器；本项目要求 Go 1.27。无需为这类修复额外安装插件。

## 检查命令

| 命令 | 作用 |
|---|---|
| `make lint` | 格式、vet、modernize、gopls 和 golangci-lint 的完整只读检查 |
| `make modernize-check` | 用 `go fix -diff` 报告建议，存在改动时失败，不写文件 |
| `make modernize` | 应用 `go fix` 默认修复，再检查是否还有剩余建议 |
| `make lint-editor` | gopls hint 及更高等级的完整文件扫描 |
| `make vet` | 两个模块的 `go vet` |
| `make race` | 两个模块的 race 测试 |

所有目标覆盖根模块与独立的 `examples/kitex` 模块。`./...` 本身不会跨越嵌套的 go.mod 边界。
CI 使用同一个 `make lint` 入口。

现代化修复可以先预览再应用：

```bash
go fix -diff ./...
go fix ./...
go tool fix help
go tool fix help errorsastype
```

某次修复可能暴露新的简化机会，若检查仍有建议，审阅后再次执行。`go fix` 没有覆盖的 gopls 提示需要单独处理；不能把自动修复当成全部检查。

## 工具分工

| 工具 / 分析器 | 常见检查 |
|---|---|
| 编译器、gopls 类型检查 | 类型不匹配、未定义标识符、错误的泛型约束 |
| `go vet` | Printf 参数、复制锁、遗漏 cancel、错误的 WaitGroup 用法、struct tag |
| gopls 额外分析器 | nilness、未使用参数/写入、遗漏 Scanner.Err 或 Rows.Err、不必要的类型实参 |
| Staticcheck | 无效赋值、不可达逻辑、废弃 API、错误的标准库使用与部分性能问题 |
| modernize | errors.AsType、整数 range、WaitGroup.Go、strings.SplitSeq、slices.Sort/Backward、reflect.TypeFor 等 |
| golangci-lint | 聚合项目配置的 errcheck、govet、ineffassign、staticcheck、unused 等 |
| gofmt | 格式，不判断程序逻辑 |

gopls 既有默认启用的分析器，也有可选的分析器。并非所有建议都代表 bug，例如 `rangeint` 通常是更简洁的写法，`stringsseq` 还可能省去中间切片。测试和 race 检查继续承担动态行为验证。

## 版本与范围

- Makefile 固定 gopls v0.23.0 和 golangci-lint v2.14.0，`go run tool@version` 不把工具加入业务 go.mod。
- `scripts/check-gopls.sh` 扫描 Git 中的已跟踪及未忽略的新 Go 文件，并跳过标准 generated-code 标记的文件。
- gopls 的 check 命令发现诊断时也可能返回 0；脚本会检查诊断输出，确保 CI 真正失败。
- 工具使用项目的 Go 版本、当前平台和默认 build tags。平台、build tags、工具版本及个人编辑器设置不同，都可能带来额外提示。
- `go fix` 不修改生成文件；protobuf / Kitex 生成代码应通过对应生成器更新。

## 保持行为

修复前查看 diff，尤其注意 JSON 编码、测试取消时机和共享切片语义。例如 time.Time 字段上的 `omitempty` 在标准 JSON 编码中不起作用；移除它保留现有行为，换成 `omitzero` 会改变零值字段是否输出。这里选择保留行为。

部分分析器默认关闭，可能涉及不同的性能权衡或行为差异；项目没有用“开启全部规则”代替逐项判断。功能性负例测试也继续保留，例如 nil context 输入以表驱动用例表达。

官方参考：[gopls 分析器](https://go.dev/gopls/analyzers)、[modernize 规则](https://pkg.go.dev/golang.org/x/tools/go/analysis/passes/modernize)、[go fix 介绍](https://go.dev/blog/gofix)。
