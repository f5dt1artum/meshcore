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

- `PUT /v1/services/{service}/instances/{instance}`：登记或覆盖实例，请求体为 `endpoint`、`version`、`zone`、`weight`（1–100）、`ttlSeconds`（1–300）与可选字符串键值 `metadata`，可选携带 `healthPolicy`（`failureThreshold` 与 `successThreshold`，均为 1–10 的整数）与 `maxConcurrency`（1–10000 的整数，省略表示不限制并发）。首次登记返回 201，覆盖存活实例返回 200；响应包含实例信息、`healthStatus`、`leaseToken` 与 RFC3339 格式的 `expiresAt`。覆盖会签发新令牌并使旧令牌立即失效，同时重置健康状态、计数与 sequence，旧实例上的许可也立即失效。未配置 `healthPolicy` 的实例 `healthStatus` 为 `disabled`，照常参与路由；配置后初始为 `healthy`。
- `POST /v1/services/{service}/instances/{instance}/heartbeat`：在 `X-Meshcore-Lease` 头中携带令牌续期，成功返回 204，过期时间从受理时重新计算。心跳不改变健康状态。
- `POST /v1/services/{service}/instances/{instance}/health`：携带同一令牌上报健康结果，请求体仅含非负整数 `sequence` 与 `status`（`pass` 或 `fail`），成功返回 204 且不续租。仅更大的 `sequence` 推进状态；相同 `sequence` 与 `status` 幂等返回 204，相同 `sequence` 不同 `status` 返回 409 `health_report_conflict`，更小的 `sequence` 返回 409 `stale_health_report`。`pass` 累加连续成功并清零失败计数，`fail` 反之；连续失败达到 `failureThreshold` 转为 `unhealthy`，连续成功达到 `successThreshold` 恢复 `healthy`。实例不存在或已过期返回 404 `instance_not_found`，令牌缺失或不匹配返回 409 `lease_conflict`，未配置策略返回 409 `health_check_disabled`。
- `DELETE /v1/services/{service}/instances/{instance}`：携带同一令牌注销，成功返回 204。实例不存在或已过期返回 404 `instance_not_found`；令牌缺失或不匹配返回 409 `lease_conflict`。
- `GET /v1/discovery/{service}`：返回当前快照，可用 `version`、`zone` 查询参数精确筛选；`instances` 按实例名字典序排列，未知服务返回 200 与空列表。`unhealthy` 实例不出现在结果中；筛选后无 `healthy` 或 `disabled` 实例时返回 200 与空列表。
- `GET /v1/resolve/{service}?key=...`：按路由键用一致性哈希（加权 rendezvous hashing）从当前快照中选择单个存活且非 `unhealthy` 的实例，可叠加 `version`、`zone` 精确筛选。`key` 经 URL 解码后须为 1–256 字节的合法 UTF-8。成功返回 200，回显 `service`、解码后的 `key` 与公开实例表示（不含 `leaseToken`）；相同实例名称与权重集合下选择结果与登记顺序无关，权重决定长期份额，增删实例只迁移受影响的键。无匹配的可用实例返回 503 `no_available_instance`，不回退到其他版本或故障域。选择不创建也不延长租约，也不占用并发名额。

## 准入许可

调用方在发起实际请求前可获取一个短期调用许可，许可占用实例的并发名额（实例登记 `maxConcurrency` 时）。

- `POST /v1/admission/{service}/acquire`：请求体为 `key`（1–256 字节合法 UTF-8）、`permitTTLSeconds`（1–300 的整数）与可选 `version`、`zone`（语义同 `/v1/resolve`，提供时不得为空）。系统从存活、非 `unhealthy` 且符合筛选的实例中按加权 rendezvous 得分降序选择首个有空余名额的实例，未配置 `maxConcurrency` 的实例始终可接收许可；选择与占用名额原子完成，有效许可数不超过配额。成功返回 201，响应包含 `service`、解码后的 `key`、公开实例表示、唯一不透明的 `permitToken` 与 RFC3339 格式的 `expiresAt`。许可到期自动释放名额；获取许可不延长实例租约，也不改变健康状态。筛选后没有可路由实例返回 503 `no_available_instance`；有匹配实例但均达配额返回 429 `concurrency_limited`，不回退到其他版本或 zone。
- `DELETE /v1/admission/{service}/permits/{permitToken}`：释放许可，有效许可返回 204；未知、已过期或因实例覆盖、注销、租约到期而失效的令牌返回 404 `permit_not_found`。

实例转为 `unhealthy` 后不再获得新许可，已有许可继续计数，恢复 `healthy` 后仍受配额约束。

service 与 instance 名称限 1–64 个 ASCII 字母、数字、点、下划线或连字符。无效参数返回 400 `validation_error`，已知资源的其他方法返回 405 并设置 `Allow`。

## 验证

```bash
go test ./...
```

熔断限流与事件投递等能力仍刻意留空，以便后续任务从已冻结事实出发独立设计并验证。
