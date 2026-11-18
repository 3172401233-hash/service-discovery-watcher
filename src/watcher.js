/**
 * 服务发现 Watcher。
 *
 * 一致性模型：
 * - 消费者只能看到两种状态：完整快照，或从快照出发版本严格连续应用增量后的状态。
 * - 发现版本缺口、乱序事件或注册中心恢复（epoch 变化）消息时，立即暂停发布，
 *   重新拉取完整快照，安装成功后才恢复发布。
 * - 同步（resync）期间到达的事件一律丢弃：新快照会覆盖它们，
 *   若快照之后仍有缺口，会再次触发 resync，最终收敛。
 */

export const WatcherState = Object.freeze({
  IDLE: 'idle',
  SYNCING: 'syncing',
  LIVE: 'live',
});

export class Watcher {
  /**
   * @param {object} options
   * @param {() => Promise<{registryEpoch: string, version: number, endpoints: Array<object>}>} options.fetchSnapshot
   *        从注册中心拉取完整快照。
   * @param {(view: {registryEpoch: string, version: number, endpoints: Array<object>}) => void} [options.onPublish]
   *        状态发布回调，只在一致状态下触发。
   * @param {() => number} [options.now] 时钟，便于测试注入。
   * @param {number} [options.retryDelayMs] 快照拉取失败后的重试间隔。
   * @param {(event: object) => void} [options.onResync] 触发重新同步时的通知回调（观测用）。
   */
  constructor({ fetchSnapshot, onPublish, now, retryDelayMs = 50, onResync } = {}) {
    if (typeof fetchSnapshot !== 'function') {
      throw new TypeError('fetchSnapshot is required');
    }
    this._fetchSnapshot = fetchSnapshot;
    this._onPublish = typeof onPublish === 'function' ? onPublish : () => {};
    this._onResync = typeof onResync === 'function' ? onResync : () => {};
    this._now = typeof now === 'function' ? now : () => Date.now();
    this._retryDelayMs = retryDelayMs;

    this._state = WatcherState.IDLE;
    this._registryEpoch = null;
    this._version = 0;
    /** @type {Map<string, object>} endpointId -> endpoint 记录 */
    this._table = new Map();
    this._retryTimer = null;
    this._syncGeneration = 0;
  }

  get state() {
    return this._state;
  }

  get version() {
    return this._version;
  }

  /** 当前对消费者可见的一致视图。syncing 期间保持最后一次发布的视图。 */
  getView() {
    return this._buildView();
  }

  async start() {
    if (this._state !== WatcherState.IDLE) return;
    await this._resync('start');
  }

  stop() {
    if (this._retryTimer) {
      clearTimeout(this._retryTimer);
      this._retryTimer = null;
    }
    this._syncGeneration += 1;
    this._state = WatcherState.IDLE;
  }

  /**
   * 处理注册中心推送的增量事件。
   * @param {{registryEpoch: string, version: number, type: string, endpoint?: object}} event
   */
  handleEvent(event) {
    if (!event || typeof event !== 'object') return;
    if (this._state !== WatcherState.LIVE) {
      // 同步期间丢弃事件，等待快照覆盖。
      return;
    }

    if (event.registryEpoch !== this._registryEpoch) {
      // 注册中心重启 / 恢复消息：版本号不再可信，全量重建。
      this._resync('registry-recovery');
      return;
    }

    if (event.version <= this._version) {
      // 重复或迟到的事件，幂等丢弃。
      return;
    }

    if (event.version !== this._version + 1) {
      // 版本缺口 / 乱序：暂停发布，重新拉快照。
      this._resync('version-gap');
      return;
    }

    const changed = this._applyEvent(event);
    this._version = event.version;
    if (changed) this._publish();
  }

  /**
   * 清理租约过期的 endpoint。基于每条记录的最后心跳时间与 TTL 判断。
   * @param {number} [now]
   * @returns {string[]} 被移除的 endpoint id。
   */
  sweepExpired(now = this._now()) {
    if (this._state !== WatcherState.LIVE) return [];
    const removed = [];
    for (const [id, ep] of this._table) {
      const ttl = ep.leaseTtlMs;
      if (typeof ttl !== 'number' || ttl <= 0) continue;
      const lastHeartbeat = typeof ep.lastHeartbeat === 'number' ? ep.lastHeartbeat : 0;
      if (now - lastHeartbeat > ttl) {
        this._table.delete(id);
        removed.push(id);
      }
    }
    if (removed.length > 0) this._publish();
    return removed;
  }

  _applyEvent(event) {
    const ep = event.endpoint;
    switch (event.type) {
      case 'upsert': {
        if (!ep || typeof ep.id !== 'string') return false;
        this._table.set(ep.id, { ...ep });
        return true;
      }
      case 'delete': {
        if (!ep || typeof ep.id !== 'string') return false;
        return this._table.delete(ep.id);
      }
      case 'heartbeat': {
        if (!ep || typeof ep.id !== 'string') return false;
        const existing = this._table.get(ep.id);
        if (!existing) return false;
        existing.lastHeartbeat =
          typeof ep.lastHeartbeat === 'number' ? ep.lastHeartbeat : this._now();
        // 心跳不改变 endpoint 集合，不触发发布。
        return false;
      }
      default:
        return false;
    }
  }

  async _resync(reason) {
    if (this._state === WatcherState.SYNCING) return;
    this._state = WatcherState.SYNCING;
    const generation = ++this._syncGeneration;
    if (reason !== 'start') {
      this._onResync({ reason, fromVersion: this._version });
    }

    let snapshot;
    try {
      snapshot = await this._fetchSnapshot();
    } catch {
      snapshot = null;
    }

    if (generation !== this._syncGeneration || this._state === WatcherState.IDLE) {
      return; // 已 stop() 或期间又触发了新的 resync。
    }

    if (!snapshot || typeof snapshot.version !== 'number' || !Array.isArray(snapshot.endpoints)) {
      // 拉取失败：稍后重试，期间保持暂停发布。
      this._retryTimer = setTimeout(() => {
        this._retryTimer = null;
        if (this._state === WatcherState.SYNCING) {
          this._state = WatcherState.LIVE; // 允许 _resync 再次进入
          this._resync('retry');
        }
      }, this._retryDelayMs);
      if (typeof this._retryTimer.unref === 'function') this._retryTimer.unref();
      return;
    }

    this._installSnapshot(snapshot);
  }

  _installSnapshot(snapshot) {
    this._registryEpoch = snapshot.registryEpoch;
    this._version = snapshot.version;
    this._table = new Map();
    for (const ep of snapshot.endpoints) {
      if (ep && typeof ep.id === 'string') {
        this._table.set(ep.id, { ...ep });
      }
    }
    this._state = WatcherState.LIVE;
    this._publish();
  }

  _buildView() {
    return {
      registryEpoch: this._registryEpoch,
      version: this._version,
      endpoints: Object.freeze(
        [...this._table.values()].map((ep) => Object.freeze({ ...ep })),
      ),
    };
  }

  _publish() {
    this._onPublish(this._buildView());
  }
}
