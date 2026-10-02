# MeshCore

这是一个面向微服务与分布式架构的微服务与分布式架构内核。长期目标是提供服务注册与发现、负载均衡、超时重试熔断舱壁、限流、RPC 契约演进、事件驱动与 Saga、幂等去重、分布式追踪和配置灰度，把微服务治理沉淀为可复用内核。

仓库采用 Go，当前基线冻结了进程健康检查；服务注册与发现为进程内内存实现，进程重启后注册表为空。后续能力必须通过独立题目逐步实现；每个题目都应定义可观察的公共行为、兼容边界和失败语义，不得依赖未公开内部 API。

## 启动

```bash
go run ./cmd/meshcore
```

服务默认监听 `127.0.0.1:8080`。可通过 `MESHCORE_ADDR` 修改监听地址。`GET /healthz` 返回 JSON 健康状态，注册状态不影响健康检查。

## 服务注册与发现

所有注册表数据保存在进程内存中，进程重启后为空；租约由服务端签发的不透明令牌保护，所有操作并发安全。

### 登记实例

```
PUT /v1/services/{service}/instances/{instance}
```

请求体：

```json
{
  "endpoint": "10.0.0.1:8080",
  "version": "v1.2.3",
  "zone": "cn-east-1a",
  "weight": 50,
  "ttlSeconds": 60,
  "metadata": {"build": "abc123"}
}
```

- `service`、`instance`：1–64 个 ASCII 字母、数字、`.`、`_`、`-`。
- `endpoint`、`version`、`zone`：非空字符串；`metadata` 为可选字符串键值。
- `weight`：1–100；`ttlSeconds`：1–300。
- 首次登记返回 `201`；覆盖存活的同名实例返回 `200`，并签发新的 `leaseToken`，旧令牌立即失效。
- 响应包含实例信息、非空 `leaseToken` 和 RFC3339 格式的 `expiresAt`。

### 续期与注销

```
POST   /v1/services/{service}/instances/{instance}/heartbeat
DELETE /v1/services/{service}/instances/{instance}
```

两者都通过 `X-Meshcore-Lease` 头携带令牌，成功返回 `204`。续期的到期时间从受理时重新计算。

- 实例不存在或已过期：`404 {"error":{"code":"instance_not_found"}}`。
- 实例存活但令牌缺失或不匹配：`409 {"error":{"code":"lease_conflict"}}`。
- 已覆盖、注销实例的旧令牌请求不能延长或恢复租约；到期实例立即从发现结果中消失。

### 发现

```
GET /v1/discovery/{service}?version=v1&zone=cn-east-1a
```

返回当前存活实例的 JSON 快照，`instances` 按 `instance` 字典序排列，`version` 与 `zone` 为精确筛选。未知服务或无匹配结果均返回 `200` 与空列表。

### 错误约定

- 无效路径参数、字段、JSON 或查询参数：`400 validation_error`。
- 已知资源上的其他方法：`405 method_not_allowed`，并设置 `Allow` 头。
- 所有错误响应均为 `application/json`，形如 `{"error":{"code":"..."}}`。

## 验证

```bash
go test ./...
go test -race ./...
```

当前基线刻意不包含熔断限流与事件投递语义的实现，以便后续任务从已冻结事实出发独立设计并验证这些能力。
