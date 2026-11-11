import { Debug } from "../utils";
import { getNodeKey } from "./util";
import * as calculateSlot from "../utils/calculateSlot";
import * as EventEmitter from "events";
import ShardedSubscriber from "./ShardedSubscriber";
import { ClusterOptions } from "./ClusterOptions";
import { ReplyError } from "redis-errors";
const debug = Debug("cluster:subscriberGroup");

type Channels = Array<string | Buffer>;

// Error replies that pass once the cluster settles. A retry can succeed.
const TRANSIENT_REPLY_ERRORS = [
  "MOVED",
  "ASK",
  "CLUSTERDOWN",
  "TRYAGAIN",
  "LOADING",
];

/**
 * Whether the server refused the command for good (e.g. NOPERM), so a retry
 * would get the same reply.
 */
function isRefusal(error: Error): boolean {
  return (
    error instanceof ReplyError &&
    !TRANSIENT_REPLY_ERRORS.some((prefix) => error.message.startsWith(prefix))
  );
}

function isSameChannel(a: string | Buffer, b: string | Buffer): boolean {
  return typeof a === "string" && typeof b === "string"
    ? a === b
    : Buffer.from(a).equals(Buffer.from(b));
}

/**
 * Redis distinguishes between "normal" and sharded PubSub. When using the normal PubSub feature,
 * exactly one subscriber exists per cluster instance because the Redis cluster bus forwards
 * messages between shards. Sharded PubSub removes this limitation by making each shard
 * responsible for its own messages.
 *
 * This class coordinates one ShardedSubscriber per master node in the cluster, providing
 * sharded PubSub support while keeping the public API backward compatible.
 *
 * `channels` is the wanted state. Every reset() moves the subscribers towards
 * it. A subscriber is one connection and is never reused, so each failure
 * gets one of three reactions:
 *
 * - The connection closed: the next reset replaces the subscriber and
 *   subscribes the new one to the node's channels.
 * - The server refused a channel for good: the channel is no longer wanted.
 *   The connection stays.
 * - A connect or a subscribe failed in a way a retry can fix: the node backs
 *   off. Resets leave it alone until the backoff ends, then retry it.
 */
export default class ClusterSubscriberGroup {
  private shardedSubscribers: Map<string, ShardedSubscriber> = new Map();
  private clusterSlots: string[][] = [];
  // Simple [min, max] slot ranges aren't enough because you can migrate single slots
  private subscriberToSlotsIndex: Map<string, number[]> = new Map();
  private channels: Map<number, Array<string | Buffer>> = new Map();
  private failedAttemptsByNode: Map<string, number> = new Map();
  // Nodes that are backing off, with the timer that ends the backoff.
  private retryTimers: Map<string, NodeJS.Timeout> = new Map();

  // Retry strategy
  private static readonly MAX_RETRY_ATTEMPTS = 10;
  private static readonly MAX_BACKOFF_MS = 2000;
  private static readonly BASE_BACKOFF_MS = 100;

  /**
   * Register callbacks
   *
   * @param cluster
   */
  constructor(
    private readonly subscriberGroupEmitter: EventEmitter,
    private readonly options: ClusterOptions
  ) {}

  /**
   * Get the responsible subscriber.
   *
   * @param slot
   */
  getResponsibleSubscriber(slot: number): ShardedSubscriber | undefined {
    const nodeKey = this.clusterSlots[slot][0];
    const sub = this.shardedSubscribers.get(nodeKey);

    if (sub && sub.subscriberStatus === "idle") {
      this.startSubscriber(sub);
    }

    return sub;
  }

  /**
   * Adds a channel for which this subscriber group is responsible
   *
   * @param channels
   */
  addChannels(channels: (string | Buffer)[]): number {
    const slot = calculateSlot(channels[0]);

    // Check if the all channels belong to the same slot and otherwise reject the operation
    for (const c of channels) {
      if (calculateSlot(c) !== slot) {
        return -1;
      }
    }

    // A channel is tracked once, however often it is subscribed. The cluster
    // can send the same SSUBSCRIBE again when it retries a command.
    const slotChannels = [...(this.channels.get(slot) ?? [])];

    for (const channel of channels) {
      if (!slotChannels.some((c) => isSameChannel(c, channel))) {
        slotChannels.push(channel);
      }
    }

    this.channels.set(slot, slotChannels);

    return Array.from(this.channels.values()).reduce(
      (sum, array) => sum + array.length,
      0
    );
  }

  /**
   * Removes channels for which the subscriber group is responsible by optionally unsubscribing
   * @param channels
   */
  removeChannels(channels: (string | Buffer)[]): number {
    const slot = calculateSlot(channels[0]);

    // Check if the all channels belong to the same slot and otherwise reject the operation
    for (const c of channels) {
      if (calculateSlot(c) !== slot) {
        return -1;
      }
    }

    const slotChannels = this.channels.get(slot);

    if (slotChannels) {
      const updatedChannels = slotChannels.filter(
        (c) => !channels.some((channel) => isSameChannel(c, channel))
      );
      this.channels.set(slot, updatedChannels);
    }

    return Array.from(this.channels.values()).reduce(
      (sum, array) => sum + array.length,
      0
    );
  }

  /**
   * Unsubscribe every sharded subscriber from all of its shard channels.
   *
   * A zero-argument `SUNSUBSCRIBE` carries no channel to compute a target slot
   * from, so it can't be routed to a single subscriber. Redis unsubscribes the
   * connection from all of its shard channels in that case; we mirror that by
   * fanning the command out to every subscriber that currently holds channels.
   *
   * @returns the number of remaining shard subscriptions (always 0)
   */
  async sunsubscribeAll(): Promise<number> {
    const pending: { nodeKey: string; result: Promise<unknown> }[] = [];

    for (const [nodeKey, subscriber] of this.shardedSubscribers) {
      if (!this.hasSubscribedChannels(nodeKey)) {
        continue;
      }

      const redis = subscriber.getInstance();
      if (!redis) {
        continue;
      }

      // Fire SUNSUBSCRIBE on every subscriber with channels (non-ready ones queue it
      // and clear on reconnect); only await ready ones so a dead shard can't hang us.
      const result = redis.sunsubscribe();
      if (redis.status === "ready") {
        pending.push({ nodeKey, result });
      } else {
        result.catch(() => {});
      }
    }

    const settled = await Promise.allSettled(pending.map((p) => p.result));
    const failed = pending.filter((_, i) => settled[i].status === "rejected");

    if (failed.length > 0) {
      // Keep the tracked channels so a retry (SUNSUBSCRIBE is idempotent) still targets these shards.
      throw new Error(
        `SUNSUBSCRIBE failed on ${failed.map((p) => p.nodeKey).join(", ")}`
      );
    }

    this.channels.clear();

    return 0;
  }

  /**
   * Disconnect all subscribers and clear some of the internal state.
   */
  stop() {
    for (const s of this.shardedSubscribers.values()) {
      s.stop();
    }

    for (const timer of this.retryTimers.values()) {
      clearTimeout(timer);
    }

    // Clear subscriber instances and pending retries.
    // Channels are preserved for resubscription on reconnect, and the failed
    // attempts so the backoff keeps growing across reconnects.
    this.retryTimers.clear();
    this.shardedSubscribers.clear();
    this.subscriberToSlotsIndex.clear();
  }

  /**
   * Start all not yet started subscribers
   */
  start() {
    const startPromises = [];
    for (const s of this.shardedSubscribers.values()) {
      if (this.shouldStartSubscriber(s)) {
        startPromises.push(this.startSubscriber(s));

        this.subscriberGroupEmitter.emit("+subscriber");
      }
    }
    return Promise.all(startPromises);
  }

  /**
   * Moves the subscribers towards the given topology: stops the ones that
   * are closed or no longer own slots, creates the missing ones, and
   * subscribes each to the channels it should hold.
   *
   * Nothing here waits on the network. A subscribe sent before the
   * connection is ready waits in the connection's offline queue, so a
   * connect that hangs only affects its own node.
   */
  async reset(clusterSlots: string[][], clusterNodes: any[]): Promise<void> {
    const hasTopologyChanged = this._refreshSlots(clusterSlots);
    let hasChanged = hasTopologyChanged;

    for (const [nodeKey, shardedSubscriber] of this.shardedSubscribers) {
      const ownsSlots = this.subscriberToSlotsIndex.has(nodeKey);

      if (ownsSlots && this.retryTimers.has(nodeKey)) {
        continue;
      }

      if (ownsSlots && shardedSubscriber.isHealthy()) {
        continue;
      }

      debug("Removing subscriber for %s", nodeKey);
      shardedSubscriber.stop();
      this.shardedSubscribers.delete(nodeKey);
      this.subscriberGroupEmitter.emit("-subscriber");
      hasChanged = true;

      if (!ownsSlots) {
        this.clearRetry(nodeKey);
      }
    }

    const startPromises = [];

    for (const nodeKey of this.subscriberToSlotsIndex.keys()) {
      if (this.retryTimers.has(nodeKey)) {
        continue;
      }

      let sub = this.shardedSubscribers.get(nodeKey);
      const isNew = !sub;

      if (!sub) {
        const redis = clusterNodes.find((node) => {
          return getNodeKey(node.options) === nodeKey;
        });

        if (!redis) {
          debug("Failed to find node for key %s", nodeKey);
          continue;
        }

        debug("Creating new subscriber for %s", nodeKey);
        sub = new ShardedSubscriber(
          this.subscriberGroupEmitter,
          redis.options,
          this.options.redisOptions,
          this.options.shardedSubscriberHealthCheckInterval
        );

        this.shardedSubscribers.set(nodeKey, sub);
        this.subscriberGroupEmitter.emit("+subscriber");
      }

      if (this.shouldStartSubscriber(sub)) {
        startPromises.push(this.startSubscriber(sub));
      }

      // A new subscriber holds no channels, a topology change can move
      // channels between nodes, and a node whose backoff ended has a failed
      // subscribe to retry.
      const isRetryDue = this.failedAttemptsByNode.has(nodeKey);

      if (isNew || hasTopologyChanged || isRetryDue) {
        this.resubscribe(nodeKey);
        hasChanged = true;
      }
    }

    if (hasChanged) {
      Promise.all(startPromises).then(() => {
        this.subscriberGroupEmitter.emit("subscribersReady");
      });
    }
  }

  /**
   * Tracks the reply to an SSUBSCRIBE that `sub` sent for `channels`, and
   * reacts to a failure. Both a user's SSUBSCRIBE and a resubscribe go
   * through here.
   *
   * A user's command also rejects the user's promise. A resubscribe has no
   * caller, so its errors are emitted instead.
   *
   * Resolves to whether the channels need no further attempt.
   */
  watchSubscribe(
    sub: ShardedSubscriber,
    channels: Channels,
    reply: Promise<unknown>,
    isResubscribe = false
  ): Promise<boolean> {
    const nodeKey = sub.getNodeKey();
    const redis = sub.getInstance();

    return reply.then(
      () => true,
      (error: Error) => {
        if (isRefusal(error)) {
          // The server refuses the whole command for one bad channel, so
          // retry the channels one by one to keep the ones it accepts.
          if (isResubscribe && channels.length > 1) {
            return Promise.all(
              channels.map((channel) => this.subscribe(sub, [channel]))
            ).then((results) => results.every(Boolean));
          }

          debug("ssubscribe refused on node %s: %s", nodeKey, error);
          this.removeChannels(channels);

          if (isResubscribe) {
            this.subscriberGroupEmitter.emit("subscriberError", error);
          }

          return true;
        }

        // A closed connection is replaced by the reset its "end" triggers.
        if (!redis || sub.getInstance() !== redis || redis.status !== "ready") {
          return false;
        }

        debug("Failed to ssubscribe on node %s: %s", nodeKey, error);

        if (isResubscribe) {
          this.subscriberGroupEmitter.emit("subscriberError", error);
        }

        this.scheduleRetry(nodeKey);

        // A resubscribe that got no reply (e.g. a command timeout) ran on a
        // connection that cannot be trusted. Close it so it is replaced once
        // the backoff ends. A user's command can also fail in the cluster
        // layer, on a healthy connection, so its channels are only retried.
        if (isResubscribe && !(error instanceof ReplyError)) {
          redis.disconnect();
        }

        return false;
      }
    );
  }

  /**
   * Refreshes the subscriber-related slot ranges
   *
   * Returns false if no refresh was needed
   *
   * @param targetSlots
   */
  private _refreshSlots(targetSlots: string[][]): boolean {
    //If there was an actual change, then reassign the slot ranges
    // Also rebuild if subscriberToSlotsIndex is empty (e.g., after stop() was called)
    if (
      this._slotsAreEqual(targetSlots) &&
      this.subscriberToSlotsIndex.size > 0
    ) {
      debug(
        "Nothing to refresh because the new cluster map is equal to the previous one."
      );

      return false;
    }

    debug("Refreshing the slots of the subscriber group.");

    //Rebuild the slots index
    this.subscriberToSlotsIndex = new Map();

    for (let slot = 0; slot < targetSlots.length; slot++) {
      const node: string = targetSlots[slot][0];

      if (!this.subscriberToSlotsIndex.has(node)) {
        this.subscriberToSlotsIndex.set(node, []);
      }
      this.subscriberToSlotsIndex.get(node).push(Number(slot));
    }

    //Update the cached slots map
    this.clusterSlots = JSON.parse(JSON.stringify(targetSlots));

    return true;
  }

  /**
   * Subscribes the subscriber of `nodeKey` to the channels of its slots.
   * The node has recovered once every subscribe succeeded.
   */
  private resubscribe(nodeKey: string) {
    const sub = this.shardedSubscribers.get(nodeKey);

    if (!sub || !sub.isStarted()) {
      return;
    }

    const subscriberSlots = this.subscriberToSlotsIndex.get(nodeKey) ?? [];
    const results = subscriberSlots
      .map((slot) => this.channels.get(slot))
      .filter((channels) => channels && channels.length > 0)
      .map((channels) => this.subscribe(sub, channels));

    Promise.all(results).then((succeeded) => {
      if (
        succeeded.every(Boolean) &&
        this.shardedSubscribers.get(nodeKey) === sub &&
        !this.retryTimers.has(nodeKey)
      ) {
        this.failedAttemptsByNode.delete(nodeKey);
      }
    });
  }

  private subscribe(sub: ShardedSubscriber, channels: Channels) {
    const redis = sub.getInstance();

    if (!redis) {
      return Promise.resolve(false);
    }

    return this.watchSubscribe(
      sub,
      channels,
      redis.ssubscribe(...channels),
      true
    );
  }

  /**
   * Deep equality of the cluster slots objects
   *
   * @param other
   * @private
   */
  private _slotsAreEqual(other: string[][]) {
    if (this.clusterSlots === undefined) {
      return false;
    } else {
      return JSON.stringify(this.clusterSlots) === JSON.stringify(other);
    }
  }

  /**
   * Starts a subscriber. A failed connect is reported and retried after a
   * backoff.
   */
  private startSubscriber(sub: ShardedSubscriber): Promise<void> {
    const nodeKey = sub.getNodeKey();

    return sub.start().then(
      () => {
        // A start resolves too when stop() cut it short, without connecting.
        // A node with channels has recovered once it resubscribed.
        if (
          sub.subscriberStatus === "connected" &&
          !this.hasSubscribedChannels(nodeKey)
        ) {
          this.failedAttemptsByNode.delete(nodeKey);
        }
      },
      (error) => {
        this.subscriberGroupEmitter.emit("subscriberError", error);
        this.scheduleRetry(nodeKey);
      }
    );
  }

  /**
   * Puts a node in backoff. Resets leave the node alone until the backoff
   * ends. Then a slots refresh is requested, and the reset that follows
   * replaces the subscriber if it is closed, or resubscribes it.
   */
  private scheduleRetry(nodeKey: string) {
    if (this.retryTimers.has(nodeKey)) {
      return;
    }

    const failedAttempts = (this.failedAttemptsByNode.get(nodeKey) || 0) + 1;
    this.failedAttemptsByNode.set(nodeKey, failedAttempts);

    const attempts = Math.min(
      failedAttempts,
      ClusterSubscriberGroup.MAX_RETRY_ATTEMPTS
    );
    const backoff = Math.min(
      ClusterSubscriberGroup.BASE_BACKOFF_MS * 2 ** attempts,
      ClusterSubscriberGroup.MAX_BACKOFF_MS
    );
    const jitter = Math.floor((Math.random() - 0.5) * (backoff * 0.5));
    const delay = Math.max(0, backoff + jitter);

    debug("Subscriber for %s failed. Retrying in %dms", nodeKey, delay);

    this.retryTimers.set(
      nodeKey,
      setTimeout(() => {
        this.retryTimers.delete(nodeKey);
        this.subscriberGroupEmitter.emit("refreshSlots");
      }, delay)
    );
  }

  private clearRetry(nodeKey: string) {
    clearTimeout(this.retryTimers.get(nodeKey));
    this.retryTimers.delete(nodeKey);
    this.failedAttemptsByNode.delete(nodeKey);
  }

  private shouldStartSubscriber(sub: ShardedSubscriber): boolean {
    if (sub.isStarted()) {
      return false;
    }

    if (!sub.isLazyConnect()) {
      return true;
    }

    return this.hasSubscribedChannels(sub.getNodeKey());
  }

  /**
   * Whether the subscriber owning `nodeKey` currently holds any active channels.
   */
  private hasSubscribedChannels(nodeKey: string): boolean {
    const subscriberSlots = this.subscriberToSlotsIndex.get(nodeKey);

    if (!subscriberSlots) {
      return false;
    }

    return subscriberSlots.some((slot) => {
      const channels = this.channels.get(slot);
      return Boolean(channels && channels.length > 0);
    });
  }
}
