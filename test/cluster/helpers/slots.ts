import Redis from "../../../lib";

const host = "127.0.0.1";

export const MASTERS = [3000, 3001, 3002];
export const ALL_NODES = [3000, 3001, 3002, 3003, 3004, 3005];

/**
 * Admin connections to every node of the local test cluster, for tests that
 * move slots with CLUSTER SETSLOT + MIGRATE. Tests that move a slot must put
 * it back with `restoreSlot` so the next run starts from the same layout.
 */
export class SlotAdmin {
  private constructor(
    readonly nodes: Record<number, Redis>,
    private readonly ids: Record<number, string>
  ) {}

  static async connect(): Promise<SlotAdmin> {
    const nodes: Record<number, Redis> = {};
    const ids: Record<number, string> = {};
    for (const port of ALL_NODES) {
      nodes[port] = new Redis(port, host);
    }
    for (const port of MASTERS) {
      ids[port] = (await nodes[port].cluster("MYID")) as string;
    }
    return new SlotAdmin(nodes, ids);
  }

  disconnect() {
    for (const port of ALL_NODES) this.nodes[port].disconnect();
  }

  async ownerOf(slot: number, port = MASTERS[0]): Promise<number> {
    const ranges = (await this.nodes[port].cluster("SLOTS")) as any[];
    for (const range of ranges) {
      if (slot >= range[0] && slot <= range[1]) return Number(range[2][1]);
    }
    throw new Error(`slot ${slot} has no owner`);
  }

  async keysOf(port: number, slot: number): Promise<string[]> {
    return (await this.nodes[port].cluster(
      "GETKEYSINSLOT",
      slot,
      100
    )) as string[];
  }

  /** Marks the slot as migrating and moves only the given keys. */
  async startMigration(slot: number, from: number, to: number, keys: string[]) {
    await this.nodes[to].cluster("SETSLOT", slot, "IMPORTING", this.ids[from]);
    await this.nodes[from].cluster("SETSLOT", slot, "MIGRATING", this.ids[to]);
    for (const key of keys) {
      await this.nodes[from].migrate(host, to, key, 0, 5000);
    }
  }

  async finishMigration(slot: number, from: number, to: number) {
    for (let keys: string[]; (keys = await this.keysOf(from, slot)).length; ) {
      for (const key of keys) {
        await this.nodes[from].migrate(host, to, key, 0, 5000);
      }
    }
    // SETSLOT NODE on the importing node bumps its config epoch. If that
    // node has not yet heard of another node's newer epoch (CLUSTER
    // BUMPEPOCH in an earlier test, or the previous move), both end up with
    // the same epoch, and resolving that collision can hand the slot back
    // to the old owner. Wait until every node knows the current epoch.
    await this.untilEpochsAgree();
    // The new owner first, as the CLUSTER SETSLOT documentation advises.
    for (const port of [to, ...MASTERS.filter((port) => port !== to)]) {
      await this.nodes[port].cluster("SETSLOT", slot, "NODE", this.ids[to]);
    }
    await this.untilAllNodesSee(slot, to);
  }

  async moveSlot(slot: number, from: number, to: number) {
    await this.startMigration(slot, from, to, []);
    await this.finishMigration(slot, from, to);
  }

  /** Works from any state a test can leave behind, including half migrated. */
  async restoreSlot(slot: number, home: number) {
    for (const port of MASTERS) {
      await this.nodes[port].cluster("SETSLOT", slot, "STABLE");
    }
    let owner = await this.ownerOf(slot, home);
    // A half migration leaves keys on a node that does not own the slot.
    // That node refuses to MIGRATE them, so hand it the slot first.
    for (const port of MASTERS) {
      if (port !== owner && (await this.keysOf(port, slot)).length) {
        await this.moveSlot(slot, owner, port);
        owner = port;
      }
    }
    if (owner !== home) {
      await this.moveSlot(slot, owner, home);
    }
  }

  private async untilEpochsAgree() {
    const deadline = Date.now() + 10000;
    for (;;) {
      const epochs = await Promise.all(
        ALL_NODES.map(async (port) => {
          const info = (await this.nodes[port].cluster("INFO")) as string;
          return /cluster_current_epoch:(\d+)/.exec(info)![1];
        })
      );
      if (epochs.every((epoch) => epoch === epochs[0])) return;
      if (Date.now() > deadline) {
        throw new Error(`nodes never agreed on the epoch: ${epochs}`);
      }
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
  }

  /** Replicas learn a new owner by gossip; wait so no node serves a stale map. */
  private async untilAllNodesSee(slot: number, owner: number) {
    const deadline = Date.now() + 10000;
    for (const port of ALL_NODES) {
      while ((await this.ownerOf(slot, port)) !== owner) {
        if (Date.now() > deadline) {
          throw new Error(`node ${port} never saw ${owner} own slot ${slot}`);
        }
        await new Promise((resolve) => setTimeout(resolve, 50));
      }
    }
  }
}
