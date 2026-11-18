# service-discovery-watcher
JavaScript 服务发现订阅库，处理版本缺口、乱序事件、陈旧 endpoint 和全量重建

## 用法

```js
import { Watcher } from './src/watcher.js';

const watcher = new Watcher({
  fetchSnapshot: () => registry.fetchFullSnapshot(), // 拉取完整快照
  onPublish: (view) => router.update(view.endpoints), // 只在一致状态下触发
  onResync: ({ reason }) => metrics.inc('resync', { reason }),
});

await watcher.start();
stream.on('event', (ev) => watcher.handleEvent(ev)); // 增量事件
setInterval(() => watcher.sweepExpired(), 1000);     // 租约清理
```

运行测试：`node --test`

## 订阅一致性模型

消费者（`onPublish` / `getView()`）只能看到两类状态：

1. **完整快照**：从注册中心一次性拉取并整体替换本地表；
2. **连续增量**：从当前版本 `v` 出发，严格按 `v+1, v+2, ...` 顺序应用的事件结果。

### 事件判定规则

| 事件条件 | 处理 |
| --- | --- |
| `event.version === version + 1` 且 epoch 相同 | 应用并发布 |
| `event.version <= version` 且 epoch 相同 | 重复/迟到事件，幂等丢弃 |
| `event.version > version + 1` | 版本缺口或乱序：暂停发布，重新拉取完整快照 |
| `event.registryEpoch` 不同 | 注册中心重启/恢复：暂停发布，全量重建 |

### 暂停与恢复

触发重新同步后，watcher 进入 `syncing` 状态：

- 期间到达的所有增量事件一律丢弃（新快照会覆盖它们；若快照之后仍有缺口，会再次触发同步，最终收敛）；
- 期间不向消费者发布任何中间状态，`getView()` 保持最后一次发布的一致视图；
- 快照拉取失败会按 `retryDelayMs` 重试，恢复发布前消费者不会看到不完整数据。

### 租约与心跳

每个 endpoint 携带 `leaseTtlMs` 与 `lastHeartbeat`。`heartbeat` 事件只刷新心跳时间、不触发发布；`sweepExpired(now)` 移除 `now - lastHeartbeat > leaseTtlMs` 的 endpoint 并发布新视图。删除后重新注册的 endpoint 以最新 `upsert` 数据为准。

### 典型场景

注册中心偶尔先发 v7 删除、再补发 v6 更新：watcher 在 v7 处发现缺口，暂停发布并拉取全量快照；迟到的 v6 因版本低于当前版本被丢弃，已停止的实例不会被重新加入路由表。
