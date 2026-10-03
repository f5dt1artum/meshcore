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

- `PUT /v1/services/{service}/instances/{instance}`：登记或覆盖实例，请求体为 `endpoint`、`version`、`zone`、`weight`（1–100）、`ttlSeconds`（1–300）与可选字符串键值 `metadata`，可选携带 `healthPolicy`（`failureThreshold` 与 `successThreshold`，均为 1–10 的整数）、可选 `maxConcurrency`（1–10000 的整数，省略表示不限制并发）与可选 `circuitBreaker`（`failureThreshold` 1–20 与 `openSeconds` 1–300，两项均为必填整数）。首次登记返回 201，覆盖存活实例返回 200；响应包含实例信息、`healthStatus`、`leaseToken` 与 RFC3339 格式的 `expiresAt`，配置了 `maxConcurrency` 时公开实例表示中回显该值。覆盖会签发新令牌并使旧令牌立即失效，同时重置健康状态、计数与 sequence，并重置熔断器；覆盖同名实例视为新实例，针对旧记录的调用许可立即失效并释放名额。未配置 `healthPolicy` 的实例 `healthStatus` 为 `disabled`，照常参与路由；配置后初始为 `healthy`。未配置 `circuitBreaker` 的实例受理行为完全不变；配置后熔断器从 `closed` 开始，其状态不出现在任何公开实例表示中。
- `POST /v1/services/{service}/instances/{instance}/heartbeat`：在 `X-Meshcore-Lease` 头中携带令牌续期，成功返回 204，过期时间从受理时重新计算。心跳不改变健康状态。
- `POST /v1/services/{service}/instances/{instance}/health`：携带同一令牌上报健康结果，请求体仅含非负整数 `sequence` 与 `status`（`pass` 或 `fail`），成功返回 204 且不续租。仅更大的 `sequence` 推进状态；相同 `sequence` 与 `status` 幂等返回 204，相同 `sequence` 不同 `status` 返回 409 `health_report_conflict`，更小的 `sequence` 返回 409 `stale_health_report`。`pass` 累加连续成功并清零失败计数，`fail` 反之；连续失败达到 `failureThreshold` 转为 `unhealthy`，连续成功达到 `successThreshold` 恢复 `healthy`。实例不存在或已过期返回 404 `instance_not_found`，令牌缺失或不匹配返回 409 `lease_conflict`，未配置策略返回 409 `health_check_disabled`。
- `DELETE /v1/services/{service}/instances/{instance}`：携带同一令牌注销，成功返回 204。实例不存在或已过期返回 404 `instance_not_found`；令牌缺失或不匹配返回 409 `lease_conflict`。
- `GET /v1/discovery/{service}`：返回当前快照，可用 `version`、`zone` 查询参数精确筛选；`instances` 按实例名字典序排列，未知服务返回 200 与空列表。`unhealthy` 实例不出现在结果中；筛选后无 `healthy` 或 `disabled` 实例时返回 200 与空列表。
- `GET /v1/resolve/{service}?key=...`：按路由键用一致性哈希（加权 rendezvous hashing）从当前快照中选择单个存活且非 `unhealthy` 的实例，可叠加 `version`、`zone` 精确筛选。`key` 经 URL 解码后须为 1–256 字节的合法 UTF-8。成功返回 200，回显 `service`、解码后的 `key` 与公开实例表示（不含 `leaseToken`）；相同实例名称与权重集合下选择结果与登记顺序无关，权重决定长期份额，增删实例只迁移受影响的键。无匹配的可用实例返回 503 `no_available_instance`，不回退到其他版本或故障域。选择不创建也不延长租约。解析只读，不占用并发名额。
- `POST /v1/admission/{service}/acquire`：在解析语义之上申请一个短期调用许可。请求体含 `key`、`permitTTLSeconds`（1–300 的整数）与可选 `version`、`zone`，`key`/`version`/`zone` 的校验与 `/v1/resolve` 一致。系统从存活、非 `unhealthy` 且符合筛选的实例中，按加权 rendezvous 得分降序选择首个有空余名额的实例；未配置 `maxConcurrency` 的实例始终可接收许可。选择与占用原子完成，有效许可数不会超过配额。熔断器只影响受理：按原排名跳过 `open` 实例以及已有未完成探测许可的 `half_open` 实例，继续尝试后续候选；`half_open` 且探测许可已结束的实例在有名额时发出唯一探测许可，探测未结束前不再向其发放许可。成功返回 201，响应包含 `service`、解码后的 `key`、公开实例表示、唯一不透明的 `permitToken` 与 RFC3339 格式 `expiresAt`。许可到期自动释放名额；获取许可不延长实例租约，也不改变健康状态。筛选后没有可路由实例时返回 503 `no_available_instance`；存在匹配的存活健康实例但它们全部仅被熔断器阻止时返回 503 `circuit_open`；存在通过熔断检查的候选但均已达到配额时返回 429 `concurrency_limited`；不回退到其他版本或 zone。
- `DELETE /v1/admission/{service}/permits/{permitToken}`：主动释放许可，有效许可返回 204。未知、已过期，或因实例被覆盖、注销、租约到期而失效的令牌返回 404 `permit_not_found`。释放 `half_open` 的探测许可会重新开始一个 open 周期；释放普通许可不改变熔断状态。
- `POST /v1/admission/{service}/permits/{permitToken}/complete`：上报有效许可的结果，请求体只能包含 `outcome`，取值为 `success` 或 `failure`。成功返回 204，立即释放并发名额并使令牌失效；重复完成、之后的 DELETE、未知、过期、服务不匹配或已释放的令牌均返回 404 `permit_not_found`；无效请求体返回 400 `validation_error`。

## 实例熔断

熔断器按实例独立工作，与健康策略互不影响：熔断不改变 `healthStatus`，健康状态也不改变熔断状态；发现与解析完全无视熔断，仍按原规则返回实例。实例从 `closed` 开始，完成结果按许可受理顺序计数：`closed` 下 `success` 清零连续失败计数，连续 `failure` 达到 `failureThreshold` 时立即进入 `open`。`open` 持续 `openSeconds`，期间不发放新许可，但已经发出的普通许可照常完成或释放，其结果只释放名额、不移动熔断器，熔断本身也不取消或续租这些许可。期满后实例转为 `half_open`：下一次符合条件的 acquire 在该实例获得唯一一个探测许可（仍占用一个并发名额），探测结果出来之前不再向该实例发放许可；探测 `success` 恢复 `closed` 并清零计数，探测 `failure` 立即重新开始一个完整 open 周期，探测许可被 DELETE 主动释放或自身到期时也重新进入 open。覆盖实例会重置熔断器并沿用既有许可失效规则；注销、租约到期与进程重启都会丢弃熔断状态。

实例变为 `unhealthy` 后不再获得新许可，但其上已有许可继续计入配额；恢复 `healthy` 后仍受同一配额约束。

实例熔断 `open` 后不再获得新许可，已有许可同样继续计入配额且不受影响；熔断器不对外暴露内部状态，只通过 acquire 的受理结果可观察。

service 与 instance 名称限 1–64 个 ASCII 字母、数字、点、下划线或连字符。无效参数返回 400 `validation_error`，已知资源的其他方法返回 405 并设置 `Allow`。

## 验证

```bash
go test ./...
```

限流与事件投递等能力仍刻意留空，以便后续题目从已冻结事实出发独立设计并验证。
