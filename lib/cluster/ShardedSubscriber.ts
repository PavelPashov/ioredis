import EventEmitter = require("events");
import { getConnectionName, getNodeKey, RedisOptions } from "./util";
import { Debug, defaults } from "../utils";
import Redis from "../Redis";
import { ClusterOptions } from "./ClusterOptions";
import { ReplyError } from "redis-errors";
import ShardedSubscriberHealthCheck from "./ShardedSubscriberHealthCheck";
const debug = Debug("cluster:subscriberGroup:shardedSubscriber");

const SubscriberStatus = {
  IDLE: "idle",
  STARTING: "starting",
  CONNECTED: "connected",
  STOPPING: "stopping",
  ENDED: "ended",
} as const;

type SubscriberStatus = typeof SubscriberStatus[keyof typeof SubscriberStatus];

const ALLOWED_STATUS_UPDATES: Record<SubscriberStatus, SubscriberStatus[]> = {
  [SubscriberStatus.IDLE]: [
    SubscriberStatus.STARTING,
    SubscriberStatus.STOPPING,
    SubscriberStatus.ENDED,
  ],
  [SubscriberStatus.STARTING]: [
    SubscriberStatus.CONNECTED,
    SubscriberStatus.STOPPING,
    SubscriberStatus.ENDED,
  ],
  [SubscriberStatus.CONNECTED]: [
    SubscriberStatus.STOPPING,
    SubscriberStatus.ENDED,
  ],
  [SubscriberStatus.STOPPING]: [SubscriberStatus.ENDED],
  [SubscriberStatus.ENDED]: [],
};

// Error replies that pass once the cluster settles. A retry can succeed.
const TRANSIENT_REPLY_ERRORS = [
  "MOVED",
  "ASK",
  "CLUSTERDOWN",
  "TRYAGAIN",
  "LOADING",
];

/**
 * Whether the server refused the command for good, so a retry on a new
 * connection would get the same reply.
 */
function isRefusal(error: Error): boolean {
  return (
    error instanceof ReplyError &&
    !TRANSIENT_REPLY_ERRORS.some((prefix) => error.message.startsWith(prefix))
  );
}

export default class ShardedSubscriber {
  private readonly nodeKey: string;
  private status: SubscriberStatus = SubscriberStatus.IDLE;
  private instance: Redis | null = null;
  private connectPromise: Promise<void> | null = null;
  private lazyConnect: boolean;
  private finishStart: (() => void) | null = null;
  private subscribeFailed = false;
  private readonly healthCheck: ShardedSubscriberHealthCheck | null;

  // Store listener references for cleanup
  private readonly messageListeners: Map<string, (...args: any[]) => void> =
    new Map();

  constructor(
    private readonly emitter: EventEmitter,
    options: RedisOptions,
    redisOptions?: ClusterOptions["redisOptions"],
    healthCheckInterval?: number,
  ) {
    this.instance = new Redis(
      defaults(
        {
          enableReadyCheck: false,
          enableOfflineQueue: true,
          connectionName: getConnectionName(
            "ssubscriber",
            options.connectionName,
          ),
          /**
           * Disable auto reconnection for subscribers.
           * The ClusterSubscriberGroup will handle the reconnection.
           */
          retryStrategy: null,
          enableAutoPipelining: false,
          lazyConnect: true,
          // Maintenance notifications are currently only supported on
          // standalone clients.
          maintNotifications: "disabled" as const,
        },
        options,
        redisOptions,
      ),
    );

    this.lazyConnect = redisOptions?.lazyConnect ?? true;

    this.nodeKey = getNodeKey(options);

    // Register listeners
    this.instance.on("end", this.onEnd);
    this.instance.on("error", this.onError);
    this.instance.on("moved", this.onMoved);

    for (const event of ["smessage", "smessageBuffer"]) {
      const listener = (...args: any[]) => {
        this.emitter.emit(event, ...args);
      };
      this.messageListeners.set(event, listener);
      this.instance.on(event, listener);
    }

    this.healthCheck = healthCheckInterval
      ? new ShardedSubscriberHealthCheck(
          this.instance,
          healthCheckInterval,
          this.nodeKey,
        )
      : null;
  }

  async start(): Promise<void> {
    if (this.connectPromise) {
      return this.connectPromise;
    }

    if (
      this.status === SubscriberStatus.STARTING ||
      this.status === SubscriberStatus.CONNECTED
    ) {
      return;
    }

    if (this.status === SubscriberStatus.ENDED || !this.instance) {
      throw new Error(
        `Sharded subscriber ${this.nodeKey} cannot be restarted once ended.`,
      );
    }

    this.updateStatus(SubscriberStatus.STARTING);
    // stop() removes the instance's listeners, so connect() would never
    // settle. Finish the start instead, so a reset waiting on it can finish.
    const stopped = new Promise<void>((resolve) => {
      this.finishStart = resolve;
    });
    this.connectPromise = Promise.race([this.instance.connect(), stopped]);

    try {
      await this.connectPromise;
      this.updateStatus(SubscriberStatus.CONNECTED);
    } catch (err) {
      this.updateStatus(SubscriberStatus.ENDED);
      throw err;
    } finally {
      this.finishStart = null;
      this.connectPromise = null;
    }
  }

  stop(): void {
    this.healthCheck?.stop();
    this.updateStatus(SubscriberStatus.STOPPING);

    if (this.finishStart) {
      this.finishStart();
    }

    if (this.instance) {
      this.instance.disconnect();
      this.instance.removeAllListeners();
      this.messageListeners.clear();
      this.instance = null;
    }

    this.updateStatus(SubscriberStatus.ENDED);
    debug("stopped %s", this.nodeKey);
  }

  /**
   * Subscribes to the channels of each slot once the connection is ready.
   * Each entry of `channelsBySlot` holds the channels of one slot.
   *
   * Channels the server refuses for good (e.g. NOPERM) are reported through
   * `onRefused`, so the caller can stop tracking them. The connection is
   * fine, so the subscriber stays healthy.
   *
   * Any other failure on a live connection leaves the subscriber without
   * those channels, so it is marked unhealthy for the next reset to replace
   * it. `onFailed` is called once, so the caller can schedule that reset.
   * `onSucceeded` is called when no slot failed that way.
   */
  subscribe(
    channelsBySlot: Array<Array<string | Buffer>>,
    handlers: {
      onSucceeded: () => void;
      onFailed: (error: Error) => void;
      onRefused: (channels: Array<string | Buffer>, error: Error) => void;
    },
  ): void {
    const redis = this.instance;

    if (!redis || redis.status === "end") {
      return;
    }

    const subscribeChannels = (
      channels: Array<string | Buffer>,
    ): Promise<void> =>
      redis.ssubscribe(...channels).then(
        () => undefined,
        (err) => {
          if (!isRefusal(err)) {
            throw err;
          }

          // The server refuses the whole command for one bad channel, so
          // retry the channels one by one to keep the ones it accepts.
          if (channels.length > 1) {
            return Promise.all(
              channels.map((channel) => subscribeChannels([channel])),
            ).then(() => undefined);
          }

          debug("ssubscribe refused on node %s: %s", this.nodeKey, err);
          handlers.onRefused(channels, err);
        },
      );

    const send = () => {
      Promise.all(channelsBySlot.map(subscribeChannels)).then(
        () => {
          if (this.instance === redis && !this.subscribeFailed) {
            handlers.onSucceeded();
          }
        },
        (err) => {
          debug("Failed to ssubscribe on node %s: %s", this.nodeKey, err);

          // A stopped subscriber or a closed connection is already replaced.
          if (
            this.instance !== redis ||
            redis.status !== "ready" ||
            this.subscribeFailed
          ) {
            return;
          }

          this.subscribeFailed = true;
          handlers.onFailed(err);
        },
      );
    };

    if (redis.status === "ready") {
      send();
    } else {
      redis.once("ready", send);
    }
  }

  isStarted(): boolean {
    return (
      [
        SubscriberStatus.CONNECTED,
        SubscriberStatus.STARTING,
      ] as SubscriberStatus[]
    ).includes(this.status);
  }

  get subscriberStatus(): SubscriberStatus {
    return this.status;
  }

  isHealthy(): boolean {
    return (
      (this.status === SubscriberStatus.IDLE ||
        this.status === SubscriberStatus.CONNECTED ||
        this.status === SubscriberStatus.STARTING) &&
      this.instance !== null &&
      !this.subscribeFailed
    );
  }

  getInstance(): Redis | null {
    return this.instance;
  }

  getNodeKey(): string {
    return this.nodeKey;
  }

  isLazyConnect(): boolean {
    return this.lazyConnect;
  }

  private onEnd = () => {
    // A subscriber that never connected is retried by the failed start, after
    // a backoff. Only a lost connection needs to be replaced right away.
    const wasConnected = this.status === SubscriberStatus.CONNECTED;
    this.updateStatus(SubscriberStatus.ENDED);
    this.emitter.emit("-node", this.instance, this.nodeKey, wasConnected);
  };

  private onError = (error: Error) => {
    this.emitter.emit("nodeError", error, this.nodeKey);
  };

  private onMoved = () => {
    this.emitter.emit("moved");
  };

  private updateStatus(nextStatus: SubscriberStatus): void {
    if (this.status === nextStatus) {
      return;
    }

    if (!ALLOWED_STATUS_UPDATES[this.status].includes(nextStatus)) {
      debug(
        "Invalid status transition for %s: %s -> %s",
        this.nodeKey,
        this.status,
        nextStatus,
      );

      return;
    }

    this.status = nextStatus;
  }
}
