# S3 文件操作 sample

这个命令使用 `store/object` 接口和 `store/s3` 连接器，演示流式文件传输、查询及预签名 URL。需要一个已经创建好的 bucket，程序不会创建 bucket 或修改策略。

修改 `config.toml` 中的 endpoint、region 和 bucket。文件默认指向本机 `9000` 端口上的 S3 兼容服务；使用 AWS S3 时删除 endpoint 与 usePathStyle，并填写 bucket 所在 region。

在仓库根目录执行。凭据可以通过 `GOSVC_S3_ACCESS_KEY_ID` / `GOSVC_S3_SECRET_ACCESS_KEY` 注入，也可以使用 AWS 标准凭据链（环境变量、配置文件或角色）。不要把真实密钥写入示例文件。

```bash
# 上传文件；也可以使用 -file - 从 stdin 流式上传。
go run ./examples/s3 -op put -key demo/report.pdf -file ./report.pdf -content-type application/pdf

# 查询与流式下载
go run ./examples/s3 -op head -key demo/report.pdf
go run ./examples/s3 -op get -key demo/report.pdf -file ./downloaded.pdf

# 列举一页；后续页把响应 nextToken 传给 -token。
go run ./examples/s3 -op list -prefix demo/

# 返回 URL、HTTP method、必须携带的 headers 及最晚失效时间。
go run ./examples/s3 -op presign-get -key demo/report.pdf -expires 10m
go run ./examples/s3 -op presign-put -key demo/upload.pdf -content-type application/pdf

# 删除指定对象
go run ./examples/s3 -op delete -key demo/report.pdf
```

在实际服务中使用 `app.Objects()`，它会跟随热更新自动切换客户端；不需要像这个独立命令一样自行创建连接器。完整配置、生命周期与行为边界见 [S3 使用说明](../../docs/s3.md)。
