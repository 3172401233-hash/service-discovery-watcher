import { test } from 'node:test';
import assert from 'node:assert/strict';
import { Watcher, WatcherState } from '../src/watcher.js';

const EPOCH = 'epoch-1';

function endpoint(id, extra = {}) {
  return { id, host: '127.0.0.1', port: 8080, leaseTtlMs: 1000, lastHeartbeat: 0, ...extra };
}

/** 构造一个可编程的注册中心假实现。 */
function makeRegistry(initialSnapshot) {
  return {
    snapshot: initialSnapshot,
    fetchCount: 0,
    async fetchSnapshot() {
      this.fetchCount += 1;
      return this.snapshot;
    },
  };
}

function makeWatcher(registry, extra = {}) {
  const published = [];
  const resyncs = [];
  const watcher = new Watcher({
    fetchSnapshot: () => registry.fetchSnapshot(),
    onPublish: (view) => published.push(view),
    onResync: (info) => resyncs.push(info),
    ...extra,
  });
  return { watcher, published, resyncs };
}

test('完整快照发布后，连续增量按序应用', async () => {
  const registry = makeRegistry({ registryEpoch: EPOCH, version: 1, endpoints: [endpoint('a')] });
  const { watcher, published } = makeWatcher(registry);
  await watcher.start();

  assert.equal(watcher.state, WatcherState.LIVE);
  assert.equal(published.length, 1);
  assert.deepEqual(published[0].endpoints.map((e) => e.id), ['a']);

  watcher.handleEvent({ registryEpoch: EPOCH, version: 2, type: 'upsert', endpoint: endpoint('b') });
  watcher.handleEvent({ registryEpoch: EPOCH, version: 3, type: 'delete', endpoint: { id: 'a' } });

  assert.equal(watcher.version, 3);
  assert.equal(published.length, 3);
  assert.deepEqual(published.at(-1).endpoints.map((e) => e.id), ['b']);
});

test('重复事件被幂等丢弃，不产生额外发布', async () => {
  const registry = makeRegistry({ registryEpoch: EPOCH, version: 1, endpoints: [endpoint('a')] });
  const { watcher, published, resyncs } = makeWatcher(registry);
  await watcher.start();

  const ev = { registryEpoch: EPOCH, version: 2, type: 'upsert', endpoint: endpoint('b') };
  watcher.handleEvent(ev);
  watcher.handleEvent(ev); // 重复
  watcher.handleEvent({ registryEpoch: EPOCH, version: 1, type: 'delete', endpoint: { id: 'a' } }); // 迟到旧事件

  assert.equal(watcher.version, 2);
  assert.equal(published.length, 2);
  assert.deepEqual(watcher.getView().endpoints.map((e) => e.id).sort(), ['a', 'b']);
  assert.equal(resyncs.length, 0);
});

test('缺失版本（缺口）触发重新拉取快照，期间不发布中间状态', async () => {
  const registry = makeRegistry({ registryEpoch: EPOCH, version: 1, endpoints: [endpoint('a')] });
  const { watcher, published, resyncs } = makeWatcher(registry);
  await watcher.start();
  assert.equal(registry.fetchCount, 1);

  // 注册中心前进到 v5，但 watcher 只收到 v4（丢了 v2、v3）
  registry.snapshot = { registryEpoch: EPOCH, version: 5, endpoints: [endpoint('a'), endpoint('c')] };
  watcher.handleEvent({ registryEpoch: EPOCH, version: 4, type: 'upsert', endpoint: endpoint('c') });

  // 等异步 resync 完成
  await new Promise((r) => setImmediate(r));

  assert.equal(resyncs.length, 1);
  assert.equal(resyncs[0].reason, 'version-gap');
  assert.equal(registry.fetchCount, 2);
  assert.equal(watcher.version, 5);
  // 消费者只看到：初始快照 + 重建后的快照，没有 v4 的部分状态
  assert.equal(published.length, 2);
  assert.deepEqual(published.at(-1).endpoints.map((e) => e.id).sort(), ['a', 'c']);

  // 重建后可以从新版本继续连续应用
  watcher.handleEvent({ registryEpoch: EPOCH, version: 6, type: 'delete', endpoint: { id: 'a' } });
  assert.deepEqual(watcher.getView().endpoints.map((e) => e.id), ['c']);
});

test('乱序事件（先 v7 后 v6）不会把已删除的 endpoint 加回来', async () => {
  // 场景：注册中心先补发 v7 delete，watcher 本地还是 v5
  const registry = makeRegistry({
    registryEpoch: EPOCH,
    version: 5,
    endpoints: [endpoint('old'), endpoint('keep')],
  });
  const { watcher, published } = makeWatcher(registry);
  await watcher.start();

  // v7 是 delete old，但本地期望 v6 → 缺口 → resync
  registry.snapshot = { registryEpoch: EPOCH, version: 7, endpoints: [endpoint('keep')] };
  watcher.handleEvent({ registryEpoch: EPOCH, version: 7, type: 'delete', endpoint: { id: 'old' } });
  await new Promise((r) => setImmediate(r));

  // 随后迟到的 v6 upsert old 必须被丢弃（版本低于当前）
  watcher.handleEvent({ registryEpoch: EPOCH, version: 6, type: 'upsert', endpoint: endpoint('old') });

  assert.deepEqual(watcher.getView().endpoints.map((e) => e.id), ['keep']);
  for (const view of published) {
    assert.ok(!view.endpoints.some((e) => e.id === 'old' && view.version >= 6));
  }
});

test('端点租约过期后被清理并发布', async () => {
  let now = 1000;
  const registry = makeRegistry({
    registryEpoch: EPOCH,
    version: 1,
    endpoints: [endpoint('a', { lastHeartbeat: 1000, leaseTtlMs: 500 }), endpoint('b', { lastHeartbeat: 1000, leaseTtlMs: 5000 })],
  });
  const { watcher, published } = makeWatcher(registry, { now: () => now });
  await watcher.start();

  now = 1601; // a 过期（1601-1000 > 500），b 未过期
  const removed = watcher.sweepExpired();

  assert.deepEqual(removed, ['a']);
  assert.deepEqual(watcher.getView().endpoints.map((e) => e.id), ['b']);
  assert.equal(published.length, 2);

  // 心跳续租后不再过期
  watcher.handleEvent({ registryEpoch: EPOCH, version: 2, type: 'heartbeat', endpoint: { id: 'b', lastHeartbeat: 1601 } });
  now = 6500;
  assert.deepEqual(watcher.sweepExpired(), []);
  assert.deepEqual(watcher.getView().endpoints.map((e) => e.id), ['b']);
});

test('删除后重新注册（re-registration）以新数据为准', async () => {
  const registry = makeRegistry({ registryEpoch: EPOCH, version: 1, endpoints: [endpoint('a', { port: 8080 })] });
  const { watcher } = makeWatcher(registry);
  await watcher.start();

  watcher.handleEvent({ registryEpoch: EPOCH, version: 2, type: 'delete', endpoint: { id: 'a' } });
  assert.equal(watcher.getView().endpoints.length, 0);

  watcher.handleEvent({ registryEpoch: EPOCH, version: 3, type: 'upsert', endpoint: endpoint('a', { port: 9090 }) });
  const view = watcher.getView();
  assert.equal(view.endpoints.length, 1);
  assert.equal(view.endpoints[0].port, 9090);
});

test('注册中心重启（epoch 变化）触发全量重建', async () => {
  const registry = makeRegistry({ registryEpoch: EPOCH, version: 10, endpoints: [endpoint('a')] });
  const { watcher, published, resyncs } = makeWatcher(registry);
  await watcher.start();

  // 注册中心重启：新 epoch，版本从 1 重新计数
  registry.snapshot = { registryEpoch: 'epoch-2', version: 1, endpoints: [endpoint('b')] };
  watcher.handleEvent({ registryEpoch: 'epoch-2', version: 1, type: 'upsert', endpoint: endpoint('b') });
  await new Promise((r) => setImmediate(r));

  assert.equal(resyncs.at(-1).reason, 'registry-recovery');
  assert.equal(watcher.version, 1);
  assert.deepEqual(watcher.getView().endpoints.map((e) => e.id), ['b']);

  // 旧 epoch 的事件一律丢弃
  watcher.handleEvent({ registryEpoch: EPOCH, version: 11, type: 'upsert', endpoint: endpoint('zombie') });
  assert.deepEqual(watcher.getView().endpoints.map((e) => e.id), ['b']);
  assert.equal(published.length, 2);
});

test('消费者可见的版本序列：快照之后严格连续', async () => {
  const registry = makeRegistry({ registryEpoch: EPOCH, version: 1, endpoints: [] });
  const { watcher, published } = makeWatcher(registry);
  await watcher.start();

  for (let v = 2; v <= 5; v += 1) {
    watcher.handleEvent({ registryEpoch: EPOCH, version: v, type: 'upsert', endpoint: endpoint(`e${v}`) });
  }

  const versions = published.map((v) => v.version);
  assert.deepEqual(versions, [1, 2, 3, 4, 5]);
});

test('同步期间到达的事件被丢弃，由快照覆盖', async () => {
  let resolveFetch;
  const registry = {
    fetchCount: 0,
    fetchSnapshot() {
      this.fetchCount += 1;
      if (this.fetchCount === 1) {
        return Promise.resolve({ registryEpoch: EPOCH, version: 1, endpoints: [endpoint('a')] });
      }
      return new Promise((resolve) => { resolveFetch = resolve; });
    },
  };
  const { watcher, published } = makeWatcher(registry);
  await watcher.start();

  // 触发 resync，但快照还没返回
  watcher.handleEvent({ registryEpoch: EPOCH, version: 5, type: 'upsert', endpoint: endpoint('x') });
  assert.equal(watcher.state, WatcherState.SYNCING);

  // 同步期间的事件必须被忽略
  watcher.handleEvent({ registryEpoch: EPOCH, version: 6, type: 'upsert', endpoint: endpoint('y') });
  watcher.handleEvent({ registryEpoch: EPOCH, version: 7, type: 'delete', endpoint: { id: 'a' } });

  resolveFetch({ registryEpoch: EPOCH, version: 7, endpoints: [endpoint('a'), endpoint('x'), endpoint('y')] });
  await new Promise((r) => setImmediate(r));

  assert.equal(watcher.state, WatcherState.LIVE);
  assert.equal(watcher.version, 7);
  assert.equal(published.length, 2);
  assert.deepEqual(published.at(-1).endpoints.map((e) => e.id).sort(), ['a', 'x', 'y']);
});

test('快照拉取失败时保持暂停并重试', async () => {
  let attempts = 0;
  const registry = {
    fetchSnapshot() {
      attempts += 1;
      if (attempts === 1) {
        return Promise.resolve({ registryEpoch: EPOCH, version: 1, endpoints: [endpoint('a')] });
      }
      if (attempts === 2) {
        return Promise.reject(new Error('registry unavailable'));
      }
      return Promise.resolve({ registryEpoch: EPOCH, version: 3, endpoints: [endpoint('a'), endpoint('b')] });
    },
  };
  const { watcher, published } = makeWatcher(registry, { retryDelayMs: 5 });
  await watcher.start();

  watcher.handleEvent({ registryEpoch: EPOCH, version: 3, type: 'upsert', endpoint: endpoint('b') });
  await new Promise((r) => setTimeout(r, 50));

  assert.ok(attempts >= 3);
  assert.equal(watcher.state, WatcherState.LIVE);
  assert.equal(watcher.version, 3);
  assert.equal(published.length, 2);
  assert.deepEqual(published.at(-1).endpoints.map((e) => e.id).sort(), ['a', 'b']);
});
