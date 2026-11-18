import {expect} from "chai";
import {Cluster} from "../../lib";
import {MASTERS, SlotAdmin} from "./helpers/slots";

const host = "127.0.0.1";
const port: number = MASTERS[0];

/**
 * Wait for a specified time
 *
 * @param ms
 */
function sleep(ms: number) {
    return new Promise(resolve => setTimeout(resolve, ms));
}


describe("cluster:ClusterSubscriberGroup", () => {

    it("should unsubscribe from the given channel", async () => {

        const cluster: Cluster = new Cluster([{host: host, port: port}], {shardedSubscribers: true});

        // Subscribe to the three channels
        const subscribeCount = await cluster.ssubscribe(
          "channel:1:{1}",
          "channel:2:{1}",
          "channel:3:{1}"
        );
        expect(subscribeCount, "Should be subscribed to 3 channels.").to.equal(3);

        // Publish a message to one of the channels
        const publishValue = await cluster.spublish(
          "channel:2:{1}",
          "This is a test message to channel 2."
        );
        expect(
          publishValue,
          "Should have published a message to channel:2:{1} and expect one subscriber."
        ).to.be.eql(1);

        await sleep(500);

        // Unsubscribe from one of the channels
        const unsubscribeCount = await cluster.sunsubscribe("channel:2:{1}");
        expect(unsubscribeCount, "Should be subscribed from 2 channels.").to.equal(
          2
        );

        await sleep(500);

        // Publish a message to the channel from which we unsubscribed
        const value = await cluster.spublish(
          "channel:2:{1}",
          "This is a test message to channel 2."
        );

        expect(
          value,
          "Published a second message to channel:2:{1} and expect to have nobody listening."
        ).to.be.eql(0);

        await sleep(1000);
        await cluster.disconnect();
    });

    it("works when ssubscribe only works for keys that map to the same slot", async () => {

        const cluster: Cluster = new Cluster([{host: host, port: port}], {shardedSubscribers: true});

        //Register the callback
        cluster.on("smessage", (channel, message) => {
            console.log(message);
            expect(message.startsWith("This is a test message")).to.be.true;
        });

        //Subscribe to the channels on different slots
        cluster.ssubscribe("channel{my}:1", "channel{yours}:2").then( ( count: number ) => {
            //Should not be called
            expect(true).to.equal(false);
        }).catch( (err) => {
            expect(err.toString().conaints("CROSSSLOT")).to.be.true;
        });

        //Subscribe to the channels on the same slot
        cluster.ssubscribe("channel{my}:1", "channel{my}:2").then( ( count: number ) => {
            console.log(count);
            expect(count).to.equal(2);
        }).catch( (err) => {
            expect(true).to.equal(false);
        });

        //Subscribe once again on the other slot
        cluster.ssubscribe("channel{yours}:2").then( ( count: number ) => {
            console.log(count);
            expect(count).to.equal(1);
        }).catch( (err) => {
            expect(true).to.equal(false);
        });

        //Publish messages
        cluster.spublish("channel{my}:1", "This is a test message to my first channel.").then((value: number) => {
            console.log("Published a message to channel{my}:1");
            expect(value).to.be.eql(1);
        });

        cluster.spublish("channel{my}:2", "This is a test message to my second channel.").then((value: number) => {
            console.log("Published a message to channel{my}:2");
            expect(value).to.be.eql(1);
        });

        cluster.spublish("channel{yours}:2", "This is a test message to your second channel.").then((value: number) => {
            console.log("Published a message to channel{yours}:2");
            expect(value).to.be.eql(1);
        });

        //Give it some time to process messages and then disconnect
        await sleep(1000);
        await cluster.disconnect();
    });


    it("works when you can receive published messages to all primary nodes after having subscribed", async () => {

        // 0. Prepare the publisher and the subscriber
        const publisher: Cluster = new Cluster([{host: host, port: port}]);

        //-- Publisher
        // Verify that the cluster is configured with 3 master nodes
        publisher.on("ready", () => {
            expect(publisher.nodes("master").length).to.eql(3);
        });

        //-- Subscriber
        const subscriber: Cluster = new Cluster([{host: host, port: port}], {shardedSubscribers: true});
        let totalNumMessages = 0;

        // Register the subscriber callback
        subscriber.on("smessage", (channel, message) => {
            console.log(message);
            expect(message.startsWith("This is a test message")).to.eql(true);
            expect(message.endsWith(channel + ".")).to.eql(true);
            totalNumMessages++;
            expect(totalNumMessages).to.lte(3);
        });

        //Verify that we did not get more than 3 subscribers
        let numSubs = 0;
        subscriber.on("+subscriber", () => {
            numSubs++
            expect(numSubs).to.lte(3);
        });

        //1. Construct 3 channel names, whereby the first one is expected to land on node 1, the second one on node 2, and so on
        const channels = ["channel:test:3", "channel:test:2", "channel:test:0"]

        for (const c of channels) {
            console.log("Trying to publish to channel:", c);

            //2. Subscribe to the channel
            await subscriber.ssubscribe(c)

            //3. Publish a message before initializing the message handling
            const numSubscribers = await publisher.spublish(c, "This is a test message  " + c + ".");
            expect(numSubscribers).to.eql(1);
        }

        //Give it some time to process messages and then disconnect
        await sleep(1000);
        subscriber.disconnect();
    });

    describe("after a slot migration", function () {
        this.timeout(30000);

        //The hash slot of interest, owned by the first master in the default layout
        const slot = 2318;
        const channel = "channel:test:3";
        const [source, target] = MASTERS;

        //Used as control connections for orchestrating the slot migration
        let slots: SlotAdmin;

        before(async () => {
            slots = await SlotAdmin.connect();
            //An interrupted earlier run can leave the slot on the target
            await slots.restoreSlot(slot, source);
        });

        afterEach(async () => {
            await slots.restoreSlot(slot, source);
        });

        after(() => {
            slots.disconnect();
        });

        it("receive messages on the channel after the slot was moved", async () => {
            const publisher: Cluster = new Cluster([{host: host, port: source}]);
            const subscriber: Cluster = new Cluster([{host: host, port: source}], {shardedSubscribers: true});

            //The subscription callback. We should receive both messages
            const messages: string[] = [];
            subscriber.on("smessage", (_channel, message) => {
                messages.push(message);
            });

            try {
                //Subscribe and then publish, and verify that the slot is on the source node
                await subscriber.ssubscribe(channel);
                expect(subscriber.slots[slot][0]).eql(`${host}:${source}`);
                await publisher.spublish(channel, "This is a test message #1 to slot "
                    + slot + " on channel " + channel + ".");
                expect(publisher.slots[slot][0]).eql(`${host}:${source}`);

                //Migrate the slot
                await slots.moveSlot(slot, source, target);

                //Trigger a topology update on the subscriber. This needs at least one moved response.
                //TODO: What if there is no traffic on the cluster connection?
                const status = await subscriber.set("match_slot{" + channel + "}", "channel 3");
                expect(status).to.eql("OK");
                expect(subscriber.slots[slot][0]).eql(`${host}:${target}`);

                //Wait a bit to let the subscriber resubscribe to previous channels
                await sleep(1000);

                const numSubscribers = await publisher.spublish(channel, "This is a test message #2 to slot "
                    + slot + " on channel " + channel + ".");
                expect(publisher.slots[slot][0]).eql(`${host}:${target}`);
                expect(numSubscribers).to.eql(1);

                await sleep(1000);
                expect(messages).to.have.length(2);
                expect(messages[0]).to.include("#1");
                expect(messages[1]).to.include("#2");
            } finally {
                subscriber.disconnect();
                publisher.disconnect();
            }
        });
    });
});
