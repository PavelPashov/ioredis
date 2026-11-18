import { expect } from "chai";
import Redis, { Cluster } from "../../lib";
import { MASTERS, SlotAdmin } from "./helpers/slots";

// Pipelined commands that get their own MOVED / ASK / TRYAGAIN reply while
// other commands in the same batch succeed (#2075). Each test moves real
// slots with CLUSTER SETSLOT + MIGRATE and puts them back afterwards.

describe("cluster:pipeline redirection", function () {
  this.timeout(30000);

  let slots: SlotAdmin;
  let admin: Record<number, Redis>;
  let borrowed: Array<{ slot: number; home: number }>;

  before(async () => {
    slots = await SlotAdmin.connect();
    admin = slots.nodes;
  });

  after(() => {
    slots.disconnect();
  });

  beforeEach(() => {
    borrowed = [];
  });

  afterEach(async () => {
    for (const { slot, home } of borrowed.reverse()) {
      await slots.restoreSlot(slot, home);
    }
  });

  /** Marks the slot as migrating and moves only the given keys. */
  async function startMigration(
    slot: number,
    from: number,
    to: number,
    keys: string[]
  ) {
    borrowed.push({ slot, home: from });
    await slots.startMigration(slot, from, to, keys);
  }

  async function moveSlot(slot: number, from: number, to: number) {
    borrowed.push({ slot, home: from });
    await slots.moveSlot(slot, from, to);
  }

  /** Distinct hash tags in distinct slots, all owned by `port`. */
  async function tagsOn(port: number, count: number, prefix: string) {
    const found: Array<{ tag: string; slot: number }> = [];
    for (let i = 0; found.length < count; i++) {
      if (i > 2000) throw new Error(`could not find ${count} tags on ${port}`);
      const tag = `${prefix}-${Date.now()}-${i}`;
      const slot = Number(await admin[port].cluster("KEYSLOT", tag));
      if (
        !found.some((f) => f.slot === slot) &&
        (await slots.ownerOf(slot)) === port
      ) {
        found.push({ tag, slot });
      }
    }
    return found;
  }

  function slotOwnerSeenBy(cluster: Cluster, slot: number) {
    return (cluster as any).slots[slot]?.[0];
  }

  const [source, target] = MASTERS;

  [
    { name: "autopipelined", enableAutoPipelining: true },
    { name: "explicit pipeline", enableAutoPipelining: false },
  ].forEach(({ name, enableAutoPipelining }) => {
    it(`${name}: applies a moved write once on the new owner and learns the route`, async () => {
      const [stay, move] = await tagsOn(source, 2, "redir-write");
      const cluster = new Cluster([{ host: "127.0.0.1", port: source }], {
        enableAutoPipelining,
      });
      try {
        await cluster.get(`{${stay.tag}}`);
        await moveSlot(move.slot, source, target);

        const send = () =>
          enableAutoPipelining
            ? Promise.all([
                cluster.incr(`{${stay.tag}}n`),
                cluster.incr(`{${move.tag}}n`),
              ])
            : cluster
                .pipeline()
                .incr(`{${stay.tag}}n`)
                .incr(`{${move.tag}}n`)
                .exec()
                .then((result) => result.map(([err, value]) => err || value));

        expect(await send()).to.eql([1, 1]);
        expect(await admin[source].get(`{${stay.tag}}n`)).to.eql("1");
        expect(await admin[target].get(`{${move.tag}}n`)).to.eql("1");
        expect(slotOwnerSeenBy(cluster, move.slot)).to.eql(
          `127.0.0.1:${target}`
        );
      } finally {
        cluster.disconnect();
      }
    });
  });

  it("does not fail a read whose slot did not move", async () => {
    const [stay, move] = await tagsOn(source, 2, "redir-read");
    await admin[source].set(`{${stay.tag}}`, "stays");
    await admin[source].set(`{${move.tag}}`, "moves");
    const cluster = new Cluster([{ host: "127.0.0.1", port: source }], {
      enableAutoPipelining: true,
    });
    try {
      await cluster.get(`{${stay.tag}}`);
      await moveSlot(move.slot, source, target);

      const results = await Promise.allSettled([
        cluster.get(`{${stay.tag}}`),
        cluster.get(`{${move.tag}}`),
      ]);
      expect(results).to.eql([
        { status: "fulfilled", value: "stays" },
        { status: "fulfilled", value: "moves" },
      ]);
    } finally {
      cluster.disconnect();
    }
  });

  it("follows ASK for one command during a migration without changing the route", async () => {
    const [stay, migrating] = await tagsOn(source, 2, "redir-ask");
    await admin[source].set(`{${migrating.tag}}moved`, "here");
    await admin[source].set(`{${migrating.tag}}kept`, "0");
    const cluster = new Cluster([{ host: "127.0.0.1", port: source }], {
      enableAutoPipelining: true,
    });
    try {
      await cluster.get(`{${stay.tag}}`);
      await startMigration(migrating.slot, source, target, [
        `{${migrating.tag}}moved`,
      ]);

      expect(
        await Promise.all([
          cluster.incr(`{${stay.tag}}n`),
          cluster.get(`{${migrating.tag}}moved`),
          cluster.incr(`{${migrating.tag}}kept`),
        ])
      ).to.eql([1, "here", 1]);
      expect(await admin[source].get(`{${stay.tag}}n`)).to.eql("1");
      expect(slotOwnerSeenBy(cluster, migrating.slot)).to.eql(
        `127.0.0.1:${source}`
      );
    } finally {
      cluster.disconnect();
    }
  });

  it("still retries a MULTI/EXEC on a moved slot as one transaction", async () => {
    const [stay, move] = await tagsOn(source, 2, "redir-multi");
    const cluster = new Cluster([{ host: "127.0.0.1", port: source }]);
    try {
      await cluster.get(`{${stay.tag}}`);
      await moveSlot(move.slot, source, target);

      const result = await cluster
        .multi()
        .incr(`{${move.tag}}a`)
        .incr(`{${move.tag}}b`)
        .exec();
      expect(result).to.eql([
        [null, 1],
        [null, 1],
      ]);
      expect(
        await admin[target].mget(`{${move.tag}}a`, `{${move.tag}}b`)
      ).to.eql(["1", "1"]);
    } finally {
      cluster.disconnect();
    }
  });
});
