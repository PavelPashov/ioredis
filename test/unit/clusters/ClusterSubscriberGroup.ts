import { expect } from "chai";
import { EventEmitter } from "events";
import * as sinon from "sinon";
import ClusterSubscriberGroup from "../../../lib/cluster/ClusterSubscriberGroup";
import Redis from "../../../lib/Redis";

describe("ClusterSubscriberGroup", () => {
  it("removes every copy of a channel with equal bytes", () => {
    const group = new ClusterSubscriberGroup(new EventEmitter(), {});
    group.addChannels(["news{tag}", Buffer.from("news{tag}")]);
    group.addChannels([Buffer.from("news{tag}"), "control{tag}"]);

    expect(group.removeChannels([Buffer.from("news{tag}")])).to.equal(1);
    expect(group.removeChannels(["control{tag}"])).to.equal(0);
  });

  it("keeps distinct binary channels even when their UTF-8 strings match", () => {
    const group = new ClusterSubscriberGroup(new EventEmitter(), {});
    const first = Buffer.concat([Buffer.from("{tag}"), Buffer.from([0xfe])]);
    const second = Buffer.concat([Buffer.from("{tag}"), Buffer.from([0xff])]);
    expect(first.toString()).to.equal(second.toString());
    group.addChannels([first, second]);

    expect(group.removeChannels([Buffer.from(first)])).to.equal(1);
    expect(group.removeChannels([first])).to.equal(1);
    expect(group.removeChannels([Buffer.from(second)])).to.equal(0);
  });

  it("keeps channels unchanged when an unsubscribe spans slots", () => {
    const group = new ClusterSubscriberGroup(new EventEmitter(), {});
    group.addChannels([Buffer.from("news{tag}")]);

    expect(group.removeChannels(["news{tag}", "news{other}"])).to.equal(-1);
    expect(group.removeChannels(["news{tag}"])).to.equal(0);
  });

  describe("connect backoff", () => {
    let clock: sinon.SinonFakeTimers;
    const realSetImmediate = setImmediate;
    const flushPromises = () =>
      new Promise((resolve) => realSetImmediate(resolve));

    beforeEach(() => {
      clock = sinon.useFakeTimers();
    });

    afterEach(() => {
      sinon.restore();
      clock.restore();
    });

    it("keeps growing when the group is stopped between failed starts", async () => {
      sinon
        .stub(Redis.prototype, "connect")
        .rejects(new Error("connect failed"));
      sinon.stub(Redis.prototype, "ssubscribe").rejects(new Error("closed"));

      const emitter = new EventEmitter();
      const refreshSlots = sinon.spy();
      emitter.on("refreshSlots", refreshSlots);
      emitter.on("subscriberError", () => {});

      const group = new ClusterSubscriberGroup(emitter, {});
      group.addChannels(["news{tag}"]);
      const slots = new Array(16384).fill(["127.0.0.1:30001"]);
      const nodes = [{ options: { host: "127.0.0.1", port: 30001 } }];

      // First failure backs off for 200ms, give or take 50ms.
      await group.reset(slots, nodes);
      await flushPromises();
      clock.tick(250);
      expect(refreshSlots.callCount, "first retry").to.equal(1);

      group.stop();

      // Second failure backs off for 400ms, give or take 100ms.
      await group.reset(slots, nodes);
      await flushPromises();
      clock.tick(299);
      expect(refreshSlots.callCount, "second retry came early").to.equal(1);
      clock.tick(201);
      expect(refreshSlots.callCount, "second retry").to.equal(2);
    });

    it("leaves a node alone while it backs off", async () => {
      const connect = sinon
        .stub(Redis.prototype, "connect")
        .rejects(new Error("connect failed"));
      sinon.stub(Redis.prototype, "ssubscribe").rejects(new Error("closed"));

      const emitter = new EventEmitter();
      emitter.on("subscriberError", () => {});

      const group = new ClusterSubscriberGroup(emitter, {});
      group.addChannels(["news{tag}"]);
      const slots = new Array(16384).fill(["127.0.0.1:30001"]);
      const nodes = [{ options: { host: "127.0.0.1", port: 30001 } }];

      await group.reset(slots, nodes);
      await flushPromises();
      await group.reset(slots, nodes);
      await flushPromises();
      expect(connect.callCount, "reconnected during the backoff").to.equal(1);

      clock.tick(250);
      await group.reset(slots, nodes);
      await flushPromises();
      expect(connect.callCount, "reconnected after the backoff").to.equal(2);

      group.stop();
    });
  });
});
