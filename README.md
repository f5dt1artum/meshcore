# MeshCore

这是一个面向微服务与分布式架构的微服务与分布式架构内核。长期目标是提供服务注册与发现、负载均衡、超时重试熔断舱壁、限流、RPC 契约演进、事件驱动与 Saga、幂等去重、分布式追踪和配置灰度，把微服务治理沉淀为可复用内核。

仓库采用 Go，当前冻结基线只提供进程健康检查。后续能力必须通过独立题目逐步实现；每个题目都应定义可观察的公共行为、兼容边界和失败语义，不得依赖未公开内部 API。

## 启动

```bash
go run ./cmd/meshcore
```

服务默认监听 `127.0.0.1:8080`。可通过 `MESHCORE_ADDR` 修改监听地址。`GET /healthz` 返回 JSON 健康状态。

## 服务注册与发现

注册表保存在进程内存中，重启后为空。实例以租约形式存活，到期未续期即从发现结果中消失。

- `PUT /v1/services/{service}/instances/{instance}`：登记或覆盖实例，请求体为 `endpoint`、`version`、`zone`、`weight`（1–100）、`ttlSeconds`（1–300）与可选字符串键值 `metadata`。首次登记返回 201，覆盖存活实例返回 200；响应包含实例信息、`leaseToken` 与 RFC3339 格式的 `expiresAt`。覆盖会签发新令牌并使旧令牌立即失效。
- `POST /v1/services/{service}/instances/{instance}/heartbeat`：在 `X-Meshcore-Lease` 头中携带令牌续期，成功返回 204，过期时间从受理时重新计算。
- `DELETE /v1/services/{service}/instances/{instance}`：携带同一令牌注销，成功返回 204。实例不存在或已过期返回 404 `instance_not_found`；令牌缺失或不匹配返回 409 `lease_conflict`。
- `GET /v1/discovery/{service}`：返回当前快照，可用 `version`、`zone` 查询参数精确筛选；`instances` 按实例名字典序排列，未知服务返回 200 与空列表。

service 与 instance 名称限 1–64 个 ASCII 字母、数字、点、下划线或连字符。无效参数返回 400 `validation_error`，已知资源的其他方法返回 405 并设置 `Allow`。

## 验证

```bash
go test ./...
```

熔断限流与事件投递等能力仍刻意留空，以便后续任务从已冻结事实出发独立设计并验证。
