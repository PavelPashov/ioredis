import type { DatabaseConfig, TestConfig } from "./utils/test.util";
import {
  getConfig,
  setupClients,
  setupMultipleClients,
  TestContext,
  wait,
} from "./utils/test.util";

import { FaultInjectorClient } from "./utils/fault-injector";
import { TestCommandRunner } from "./utils/command-runner";
import { CHANNELS, CHANNELS_BY_SLOT } from "./utils/test.util";
import { assert } from "chai";
import { getCreateDatabaseConfig } from "./utils/db-configs";

describe("Sharded Pub/Sub E2E", () => {
  let ctx: TestContext;
  let faultInjectorClient: FaultInjectorClient;

  before(() => {
    const initialConfig = getConfig();
    ctx = new TestContext(initialConfig.clientConfig);
    faultInjectorClient = new FaultInjectorClient(
      initialConfig.faultInjectorUrl
    );
  });

  beforeEach(async () => {
    try {
      const { action_id: createActionId } =
        await faultInjectorClient.createDatabase(
          getCreateDatabaseConfig("cluster")
        );

      const result = await faultInjectorClient.waitForAction(createActionId);
      ctx.registerConfig(result.output as unknown as DatabaseConfig);
    } catch (error) {
      console.error("Error creating database", error);
    }
  });

  afterEach(async () => {
    await ctx.runCleanups();

    try {
      const { action_id: deleteActionId } =
        await faultInjectorClient.deleteDatabase(ctx.getClientConfig().bdbId);

      await faultInjectorClient.waitForAction(deleteActionId);
    } catch (error) {
      console.error("Error deleting database", error);
    }
  });

  describe("Single Subscriber", () => {
    it("should receive messages published to multiple channels", async () => {
      let { subscriber, publisher, messageTracker, ready, cleanup } =
        await setupClients(ctx.getClientConfig());

      ctx.registerCleanup(cleanup);

      await ready();

      for (const channel of CHANNELS) {
        await subscriber.ssubscribe(channel);
      }

      subscriber.on("smessage", (channelName, _) => {
        messageTracker.incrementReceived(channelName);
      });

      const { controller, result } =
        TestCommandRunner.publishMessagesUntilAbortSignal(
          publisher,
          CHANNELS,
          messageTracker
        );

      // Wait for 10 seconds, while publishing messages
      await wait(10_000);
      controller.abort();
      await result;

      for (const channel of CHANNELS) {
        assert.strictEqual(
          messageTracker.getChannelStats(channel)?.received,
          messageTracker.getChannelStats(channel)?.sent
        );
      }
    });

    it("should resume publishing and receiving after failover", async () => {
      const config = ctx.getClientConfig();

      const { subscriber, publisher, messageTracker, ready, cleanup } =
        await setupClients(config, {
          subscriberOverrides: { slotsRefreshInterval: -1 },
          publisherOverrides: { slotsRefreshInterval: -1 },
        });

      ctx.registerCleanup(cleanup);

      await ready();

      for (const channel of CHANNELS) {
        await subscriber.ssubscribe(channel);
      }

      subscriber.on("smessage", (channelName, _) => {
        messageTracker.incrementReceived(channelName);
      });

      // Trigger failover twice
      for (let i = 0; i < 2; i++) {
        // Start publishing messages
        const { controller: publishAbort, result: publishResult } =
          TestCommandRunner.publishMessagesUntilAbortSignal(
            publisher,
            CHANNELS,
            messageTracker
          );

        // Trigger failover during publishing
        const { action_id: failoverActionId } =
          await faultInjectorClient.triggerAction({
            type: "failover",
            parameters: {
              bdb_id: config.bdbId.toString(),
              cluster_index: 0,
            },
          });

        // Wait for failover to complete
        await faultInjectorClient.waitForAction(failoverActionId);

        publishAbort.abort();
        await publishResult;

        const totalSent = CHANNELS.reduce(
          (acc, channel) => acc + messageTracker.getChannelStats(channel)!.sent,
          0
        );
        const totalReceived = CHANNELS.reduce(
          (acc, channel) =>
            acc + messageTracker.getChannelStats(channel)!.received,
          0
        );

        assert.ok(
          totalReceived <= totalSent,
          `Total received (${totalReceived}) should be <= total sent (${totalSent})`
        );

        // Wait for 2 seconds before resuming publishing
        await wait(2_000);

        messageTracker.reset();

        const {
          controller: afterFailoverController,
          result: afterFailoverResult,
        } = TestCommandRunner.publishMessagesUntilAbortSignal(
          publisher,
          CHANNELS,
          messageTracker
        );

        await wait(10_000);
        afterFailoverController.abort();
        await afterFailoverResult;

        for (const channel of CHANNELS) {
          const sent = messageTracker.getChannelStats(channel)!.sent;
          const received = messageTracker.getChannelStats(channel)!.received;
          assert.ok(sent > 0, `Channel ${channel} should have sent messages`);
          assert.ok(
            received > 0,
            `Channel ${channel} should have received messages`
          );
          assert.strictEqual(
            messageTracker.getChannelStats(channel)!.received,
            messageTracker.getChannelStats(channel)!.sent,
            `Channel ${channel} received (${received}) should equal sent (${sent}) once resumed after failover`
          );
        }
      }
    });

    it("should NOT receive messages after sunsubscribe", async () => {
      const { subscriber, publisher, messageTracker, ready, cleanup } =
        await setupClients(ctx.getClientConfig());

      ctx.registerCleanup(cleanup);

      await ready();

      for (const channel of CHANNELS) {
        await subscriber.ssubscribe(channel);
      }

      subscriber.on("smessage", (channelName, _) => {
        messageTracker.incrementReceived(channelName);
      });

      const { controller, result } =
        TestCommandRunner.publishMessagesUntilAbortSignal(
          publisher,
          CHANNELS,
          messageTracker
        );

      // Wait for 5 seconds, while publishing messages
      await wait(5_000);
      controller.abort();
      await result;

      for (const channel of CHANNELS) {
        assert.strictEqual(
          messageTracker.getChannelStats(channel)?.received,
          messageTracker.getChannelStats(channel)?.sent
        );
      }

      // Reset message tracker
      messageTracker.reset();

      const unsubscribeChannels = [
        CHANNELS_BY_SLOT["1000"],
        CHANNELS_BY_SLOT["8000"],
        CHANNELS_BY_SLOT["16000"],
      ];

      for (const channel of unsubscribeChannels) {
        await subscriber.sunsubscribe(channel);
      }

      const {
        controller: afterUnsubscribeController,
        result: afterUnsubscribeResult,
      } = TestCommandRunner.publishMessagesUntilAbortSignal(
        publisher,
        CHANNELS,
        messageTracker
      );

      // Wait for 5 seconds, while publishing messages
      await wait(5_000);
      afterUnsubscribeController.abort();
      await afterUnsubscribeResult;

      for (const channel of unsubscribeChannels) {
        assert.strictEqual(
          messageTracker.getChannelStats(channel)?.received,
          0,
          `Channel ${channel} should not have received messages after unsubscribe`
        );
      }

      // All other channels should have received messages
      const stillSubscribedChannels = CHANNELS.filter(
        (channel) => !unsubscribeChannels.includes(channel as any)
      );

      for (const channel of stillSubscribedChannels) {
        assert.ok(
          messageTracker.getChannelStats(channel)!.received > 0,
          `Channel ${channel} should have received messages`
        );
      }
    });
  });

  describe("Multiple Subscribers", () => {
    it("should receive messages published to multiple channels", async () => {
      const { publishers, subscribers, messageTracker, cleanup, ready } =
        await setupMultipleClients(ctx.getClientConfig(), {
          publisherCount: 1,
          subscriberCount: 2,
        });

      ctx.registerCleanup(cleanup);

      await ready();

      for (const subscriber of subscribers) {
        for (const channel of CHANNELS) {
          await subscriber.ssubscribe(channel);
        }

        subscriber.on("smessage", (channelName, _) => {
          messageTracker.incrementReceived(channelName);
        });
      }

      const { controller, result } =
        TestCommandRunner.publishMessagesUntilAbortSignal(
          publishers[0],
          CHANNELS,
          messageTracker
        );

      // Wait for 10 seconds, while publishing messages
      await wait(10_000);
      controller.abort();
      await result;

      for (const channel of CHANNELS) {
        const sent = messageTracker.getChannelStats(channel)!.sent;
        const received = messageTracker.getChannelStats(channel)!.received;

        assert.ok(sent > 0, `Channel ${channel} should have sent messages`);
        assert.ok(
          received > 0,
          `Channel ${channel} should have received messages`
        );
        assert.strictEqual(
          received,
          sent * subscribers.length,
          `Channel ${channel} received (${received}) should equal sent (${sent}) * number of subscribers (${subscribers.length})`
        );
      }
    });

    it("should resume publishing and receiving after failover", async () => {
      const { publishers, subscribers, messageTracker, cleanup, ready } =
        await setupMultipleClients(ctx.getClientConfig(), {
          publisherCount: 1,
          subscriberCount: 2,
        });

      ctx.registerCleanup(cleanup);

      await ready();

      for (const subscriber of subscribers) {
        for (const channel of CHANNELS) {
          await subscriber.ssubscribe(channel);
        }

        subscriber.on("smessage", (channelName, _) => {
          messageTracker.incrementReceived(channelName);
        });
      }

      // Start publishing messages
      const { controller: publishAbort, result: publishResult } =
        TestCommandRunner.publishMessagesUntilAbortSignal(
          publishers[0],
          CHANNELS,
          messageTracker
        );

      // Trigger failover during publishing
      const { action_id: failoverActionId } =
        await faultInjectorClient.triggerAction({
          type: "failover",
          parameters: {
            bdb_id: ctx.getClientConfig().bdbId.toString(),
            cluster_index: 0,
          },
        });

      // Wait for failover to complete
      await faultInjectorClient.waitForAction(failoverActionId);

      publishAbort.abort();
      await publishResult;

      const totalSent = CHANNELS.reduce(
        (acc, channel) => acc + messageTracker.getChannelStats(channel)!.sent,
        0
      );
      const totalReceived = CHANNELS.reduce(
        (acc, channel) =>
          acc + messageTracker.getChannelStats(channel)!.received,
        0
      );

      assert.ok(
        totalReceived <= totalSent * subscribers.length,
        `Total received (${totalReceived}) should be <= total sent (${totalSent})`
      );

      // Wait for 2 seconds before resuming publishing
      await wait(2_000);

      messageTracker.reset();

      const {
        controller: afterFailoverController,
        result: afterFailoverResult,
      } = TestCommandRunner.publishMessagesUntilAbortSignal(
        publishers[0],
        CHANNELS,
        messageTracker
      );

      await wait(10_000);
      afterFailoverController.abort();
      await afterFailoverResult;

      for (const channel of CHANNELS) {
        const sent = messageTracker.getChannelStats(channel)!.sent;
        const received = messageTracker.getChannelStats(channel)!.received;
        assert.ok(sent > 0, `Channel ${channel} should have sent messages`);
        assert.ok(
          received > 0,
          `Channel ${channel} should have received messages by subscriber 1`
        );
        assert.strictEqual(
          received,
          sent * subscribers.length,
          `Channel ${channel} received (${received}) should equal sent (${sent}) once resumed after failover by subscriber 2`
        );
      }
    });
  });
});
