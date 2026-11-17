import { EventEmitter } from "events";
import { ReplyError } from "redis-errors";
import Command from "../Command";
import { Debug } from "../utils";
const debug = Debug("cluster:subscriberGroup:shardedSubscriber:healthCheck");

// Fraction of the interval randomly added to a connection's ping period.
const JITTER_RATIO = 0.1;

export interface HealthCheckedConnection extends EventEmitter {
  sendCommand(command: Command): unknown;
  disconnect(): void;
}

/**
 * Disconnects a connection that stops answering commands.
 *
 * Before ready, the handshake must complete within one interval. After ready,
 * PING is sent once per interval, plus up to 10% random jitter chosen per
 * connection, and must be answered before the next one. A sharded message
 * received within the last interval stands in for the PING, so a busy
 * connection is not pinged. A PING already sent must still be answered.
 * A `commandTimeout` shorter than the interval is extended for the PING, so
 * the interval alone decides. Replies arrive in order, so a PING queued behind
 * a stuck command hangs too. A server error reply (for example NOPERM) still
 * proves the node answers, while a local rejection does not, so only the
 * former counts as an answer.
 */
export default class ShardedSubscriberHealthCheck {
  private timer: NodeJS.Timeout | undefined;
  private ready = false;
  private pendingPing: Command | undefined;
  private lastMessageAt = 0;

  constructor(
    private readonly instance: HealthCheckedConnection,
    private readonly intervalMs: number,
    private readonly nodeKey: string
  ) {
    instance.on("connect", this.arm);
    instance.on("ready", this.onReady);
    instance.on("close", this.stop);
    // DataHandler emits smessageBuffer for every sharded message, while
    // smessage is emitted only when it has listeners.
    instance.on("smessageBuffer", this.onMessage);
  }

  stop = () => {
    clearTimeout(this.timer);
    this.timer = undefined;
    this.ready = false;
    this.pendingPing = undefined;
    this.lastMessageAt = 0;
  };

  private onMessage = () => {
    this.lastMessageAt = Date.now();
  };

  private arm = () => {
    this.stop();
    this.timer = setTimeout(this.onFire, this.intervalMs);
  };

  private onReady = () => {
    this.ready = true;
    if (!this.timer) {
      return;
    }
    // Subscribers that reconnect together would otherwise ping in lockstep.
    clearTimeout(this.timer);
    const jitterMs = Math.floor(Math.random() * this.intervalMs * JITTER_RATIO);
    this.timer = setTimeout(this.onFire, this.intervalMs + jitterMs);
  };

  private onFire = () => {
    if (!this.ready || this.pendingPing) {
      const what = this.ready ? "PING" : "handshake";
      this.stop();
      debug(
        "%s left %s unanswered for %dms, disconnecting",
        this.nodeKey,
        what,
        this.intervalMs
      );
      this.instance.disconnect();
      return;
    }

    if (Date.now() - this.lastMessageAt < this.intervalMs) {
      this.timer?.refresh();
      return;
    }

    const ping = new Command("PING");
    this.pendingPing = ping;
    const answered = () => {
      if (this.pendingPing === ping) {
        this.pendingPing = undefined;
      }
    };
    ping.promise.then(answered, (err) => {
      if (err instanceof ReplyError) {
        answered();
      }
    });
    this.instance.sendCommand(ping);
    ping.extendTimeout(this.intervalMs);
    this.timer?.refresh();
  };
}
