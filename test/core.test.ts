import { expect, it, describe } from 'vitest';
import {
  FlowWindow,
  PriorityScheduler,
  PriorityTree,
  ROOT_STREAM,
  clampWeight,
  wireWeightToWeight,
} from '../src/index.js';

it('tracks credit', () => {
  const x = new FlowWindow(10);
  x.consume(4);
  expect(x.value).toBe(6);
});

describe('priority tree', () => {
  it('creates idle placeholder nodes for unknown dependencies', () => {
    const t = new PriorityTree();
    t.reprioritize({ streamId: 4, dependsOn: 99 });
    expect(t.stateOf(99)).toBe('idle');
    expect(t.stateOf(4)).toBe('idle');
    expect(t.parentOf(4)).toBe(99);
    t.assertInvariant();
  });

  it('promotes an idle placeholder when the real stream opens', () => {
    const t = new PriorityTree();
    t.reprioritize({ streamId: 4, dependsOn: 2, weight: 20 });
    t.open(2);
    expect(t.stateOf(2)).toBe('open');
    expect(t.weightOf(2)).toBe(16);
    expect(t.weightOf(4)).toBe(20);
    t.assertInvariant();
  });

  it('reparents a self-dependency to the root', () => {
    const t = new PriorityTree();
    t.open(2);
    t.reprioritize({ streamId: 2, dependsOn: 4 });
    t.reprioritize({ streamId: 2, dependsOn: 2, weight: 77 });
    expect(t.parentOf(2)).toBe(ROOT_STREAM);
    expect(t.weightOf(2)).toBe(77);
    t.assertInvariant();
  });

  it('breaks an ancestor-dependency cycle by lifting the intervening subtree', () => {
    const t = new PriorityTree();
    // root -> A -> B -> C
    t.reprioritize({ streamId: 1, dependsOn: 0 });
    t.reprioritize({ streamId: 2, dependsOn: 1 });
    t.reprioritize({ streamId: 3, dependsOn: 2 });
    // Try A depends on C (would form A -> C -> B -> A)
    t.reprioritize({ streamId: 1, dependsOn: 3 });
    t.assertInvariant();
    // RFC 7540 §5.3.1 Figure 7 result: B (with C subtree) lifts to A's old
    // parent (root), then A moves under C:
    //   root -> B -> C -> A
    expect(t.parentOf(2)).toBe(ROOT_STREAM);
    expect(t.parentOf(3)).toBe(2);
    expect(t.parentOf(1)).toBe(3);
    expect(t.reachable().sort()).toEqual([1, 2, 3]);
  });

  it('breaks a deep cycle while keeping every node reachable', () => {
    const t = new PriorityTree();
    for (const [s, d] of [
      [1, 0],
      [2, 1],
      [3, 2],
      [4, 3],
      [5, 4],
    ]) {
      t.reprioritize({ streamId: s, dependsOn: d });
    }
    // node 1 depends on descendant 5
    t.reprioritize({ streamId: 1, dependsOn: 5 });
    t.assertInvariant();
    expect(t.reachable().sort()).toEqual([1, 2, 3, 4, 5]);
    // path must be root -> 2 -> 3 -> 4 -> 5 -> 1
    expect(t.parentOf(2)).toBe(0);
    expect(t.parentOf(3)).toBe(2);
    expect(t.parentOf(4)).toBe(3);
    expect(t.parentOf(5)).toBe(4);
    expect(t.parentOf(1)).toBe(5);
  });

  it('moves all siblings atomically on an exclusive reprioritization', () => {
    const t = new PriorityTree();
    // root children 1,2,3
    t.open(1);
    t.open(2);
    t.open(3);
    t.reprioritize({ streamId: 4, dependsOn: 0, exclusive: true });
    t.assertInvariant();
    // 1,2,3 become children of 4, in order
    expect(t.childrenOf(0)).toEqual([4]);
    expect(t.childrenOf(4)).toEqual([1, 2, 3]);
    expect(t.parentOf(1)).toBe(4);
  });

  it('exclusive move on a non-root parent reparents that parents children', () => {
    const t = new PriorityTree();
    t.reprioritize({ streamId: 1, dependsOn: 0 });
    t.reprioritize({ streamId: 2, dependsOn: 1 });
    t.reprioritize({ streamId: 3, dependsOn: 1 });
    t.reprioritize({ streamId: 4, dependsOn: 1, exclusive: true });
    t.assertInvariant();
    expect(t.childrenOf(1)).toEqual([4]);
    expect(t.childrenOf(4).sort()).toEqual([2, 3]);
  });

  it('non-exclusive move keeps existing siblings', () => {
    const t = new PriorityTree();
    t.reprioritize({ streamId: 1, dependsOn: 0 });
    t.reprioritize({ streamId: 2, dependsOn: 0 });
    t.reprioritize({ streamId: 3, dependsOn: 1 });
    t.reprioritize({ streamId: 3, dependsOn: 0, weight: 30 });
    t.assertInvariant();
    expect(t.childrenOf(0).sort()).toEqual([1, 2, 3]);
    expect(t.childrenOf(1)).toEqual([]);
    expect(t.weightOf(3)).toBe(30);
  });

  it('re-hangs the subtree when an intermediate node closes', () => {
    const t = new PriorityTree();
    t.reprioritize({ streamId: 1, dependsOn: 0 });
    t.reprioritize({ streamId: 2, dependsOn: 1 });
    t.reprioritize({ streamId: 3, dependsOn: 2 });
    t.reprioritize({ streamId: 4, dependsOn: 2 });
    t.close(2);
    t.assertInvariant();
    expect(t.has(2)).toBe(false);
    expect(t.parentOf(3)).toBe(1);
    expect(t.parentOf(4)).toBe(1);
    expect(t.childrenOf(1).sort()).toEqual([3, 4]);
    expect(t.reachable().sort()).toEqual([1, 3, 4]);
  });

  it('re-hangs children under root when the top node closes', () => {
    const t = new PriorityTree();
    t.reprioritize({ streamId: 1, dependsOn: 0 });
    t.reprioritize({ streamId: 2, dependsOn: 1 });
    t.close(1);
    t.assertInvariant();
    expect(t.parentOf(2)).toBe(0);
  });

  it('removing a closed leaf loses nothing else', () => {
    const t = new PriorityTree();
    t.reprioritize({ streamId: 1, dependsOn: 0 });
    t.reprioritize({ streamId: 2, dependsOn: 1 });
    t.close(2);
    t.assertInvariant();
    expect(t.reachable()).toEqual([1]);
  });

  it('clamps weights into 1..256 and updates them dynamically', () => {
    expect(clampWeight(0)).toBe(1);
    expect(clampWeight(1000)).toBe(256);
    expect(wireWeightToWeight(0)).toBe(1);
    expect(wireWeightToWeight(255)).toBe(256);
    const t = new PriorityTree();
    t.open(7);
    t.setWeight(7, 5);
    expect(t.weightOf(7)).toBe(5);
    t.setWeight(7, 999);
    expect(t.weightOf(7)).toBe(256);
  });

  it('preserves the acyclic unique-reachability invariant under random ops', () => {
    const t = new PriorityTree();
    const ids = Array.from({ length: 30 }, (_, i) => i + 1);
    let seed = 1234567;
    const rnd = () => {
      // deterministic LCG
      seed = (seed * 1103515245 + 12345) & 0x7fffffff;
      return seed / 0x7fffffff;
    };
    ids.slice(0, 8).forEach((id, i) =>
      t.reprioritize({ streamId: id, dependsOn: i === 0 ? 0 : ids[i - 1] }),
    );
    for (let i = 0; i < 6000; i++) {
      const s = ids[Math.floor(rnd() * ids.length)];
      const r = rnd();
      if (r < 0.75) {
        let d = ids[Math.floor(rnd() * ids.length)];
        if (rnd() < 0.05) d = s; // self-dependency on purpose
        if (!t.has(s)) t.open(s);
        t.reprioritize({
          streamId: s,
          dependsOn: rnd() < 0.05 ? 0 : d,
          exclusive: rnd() < 0.2,
          weight: 1 + Math.floor(rnd() * 256),
        });
      } else {
        t.close(s);
        if (rnd() < 0.3) t.open(s); // recycle id
      }
      t.assertInvariant();
    }
    // every live node reachable exactly once
    const live = t.reachable();
    expect(new Set(live).size).toBe(live.length);
  });
});

describe('scheduler eligibility', () => {
  function readyScheduler() {
    const s = new PriorityScheduler(1_000_000);
    s.open(1, 1_000_000);
    s.open(2, 1_000_000);
    s.queue(1, 1000);
    s.queue(2, 1000);
    return s;
  }

  it('returns null when nothing is queued', () => {
    const s = new PriorityScheduler();
    s.open(1);
    expect(s.pick()).toBeNull();
  });

  it('only picks streams with DATA and a positive window', () => {
    const s = new PriorityScheduler(1_000_000);
    s.open(1, 0).queue(1, 100);
    s.open(2, 1_000_000).queue(2, 100);
    expect(s.pick()!.streamId).toBe(2);
    s.sent(2, 100);
    s.grant(1, 1000);
    expect(s.pick()!.streamId).toBe(1);
  });

  it('returns null when the connection window is exhausted', () => {
    const s = new PriorityScheduler(10);
    s.open(1, 1_000_000).queue(1, 100);
    const p = s.pick()!;
    s.sent(p.streamId, p.budget);
    expect(s.connectionWindowValue).toBe(0);
    expect(s.pick()).toBeNull();
    s.grantConnection(100);
    expect(s.pick()!.streamId).toBe(1);
  });

  it('never schedules idle placeholder nodes themselves', () => {
    const s = new PriorityScheduler(1_000_000);
    s.open(2, 1_000_000).queue(2, 100);
    s.reprioritize({ streamId: 2, dependsOn: 99 });
    // 99 is an idle shell; the request still reaches 2
    expect(s.pick()!.streamId).toBe(2);
  });

  it('schedules descendants through blocked ancestors (RFC 5.3.2 policy)', () => {
    const s = new PriorityScheduler(1_000_000);
    // root -> 1(blocked: no DATA/window) -> 2(ready)
    s.open(1, 0);
    s.open(2, 1_000_000);
    s.queue(2, 500);
    s.reprioritize({ streamId: 2, dependsOn: 1 });
    expect(s.pick()!.streamId).toBe(2);
  });

  it('treats a flow-control-blocked ancestor (DATA, zero window) as transparent', () => {
    const s = new PriorityScheduler(1_000_000);
    s.open(1, 0);
    s.open(2, 1_000_000);
    s.open(3, 1_000_000);
    s.queue(1, 100);
    s.queue(2, 100);
    s.queue(3, 100);
    s.reprioritize({ streamId: 2, dependsOn: 1 });
    const seen = new Set<number>();
    for (let i = 0; i < 4; i++) {
      const p = s.pick();
      if (!p) break;
      seen.add(p.streamId);
      s.sent(p.streamId, 100);
      s.queue(p.streamId, 100);
    }
    expect(seen.has(1)).toBe(false); // zero stream window, never picked
    expect(seen.has(2)).toBe(true); // served through the blocked ancestor
    expect(seen.has(3)).toBe(true); // independent sibling unaffected

    // Once the ancestor gets a window it is ready and dominates its subtree.
    s.grant(1, 1000);
    expect(s.pick()!.streamId).toBe(1);
  });

  it('a ready ancestor always wins over its descendants', () => {
    const s = new PriorityScheduler(1_000_000);
    s.open(1, 1_000_000).queue(1, 500);
    s.open(2, 1_000_000).queue(2, 500);
    s.reprioritize({ streamId: 2, dependsOn: 1 });
    expect(s.pick()!.streamId).toBe(1);
    s.sent(1, 500);
    expect(s.pick()!.streamId).toBe(2);
  });

  it('does not pick a closed stream even with DATA queued before close', () => {
    const s = readyScheduler();
    s.close(1);
    for (let i = 0; i < 20; i++) {
      const p = s.pick();
      if (!p) break;
      expect(p.streamId).not.toBe(1);
      s.sent(p.streamId, p.budget);
    }
  });

  it('deducts DATA, stream window and connection window on sent()', () => {
    const s = new PriorityScheduler(1000);
    s.open(1, 400).queue(1, 500);
    const p = s.pick()!;
    expect(p.budget).toBe(400);
    s.sent(1, p.budget);
    expect(s.queuedOf(1)).toBe(100);
    expect(s.windowOf(1)).toBe(0);
    expect(s.connectionWindowValue).toBe(600);
  });

  it('keeps scheduling a half-closed stream until its DATA is drained', () => {
    const s = new PriorityScheduler(100_000);
    s.open(1, 100_000).queue(1, 5_000).halfClose(1);
    expect(s.pick()!.streamId).toBe(1);
    s.sent(1, 5_000);
    expect(s.pick()).toBeNull();
  });

  it('never returns a zero-byte selection for a zero-window stream', () => {
    const s = new PriorityScheduler(1_000_000);
    s.open(1, 0).queue(1, 10);
    expect(s.pick()).toBeNull();
  });
});

describe('weighted shares', () => {
  const CHUNK = 1400;
  const WINDOW = 10 ** 10;

  function runShares(
    streams: { id: number; weight: number }[],
    rounds: number,
    reweightAt?: (step: number, s: PriorityScheduler) => void,
  ) {
    const s = new PriorityScheduler(WINDOW);
    for (const { id, weight } of streams) {
      s.open(id, WINDOW);
      s.queue(id, WINDOW);
      s.setWeight(id, weight);
    }
    const sent = new Map<number, number>();
    for (let i = 0; i < rounds; i++) {
      reweightAt?.(i, s);
      const p = s.pick(CHUNK);
      if (!p) throw new Error('scheduler stalled');
      const bytes = Math.min(CHUNK, p.budget);
      s.sent(p.streamId, bytes);
      sent.set(p.streamId, (sent.get(p.streamId) ?? 0) + bytes);
      // keep DATA replenished for long-run measurements
      s.queue(p.streamId, bytes);
    }
    return sent;
  }

  it('flat-tree long-term byte share approximates weights 1:2:5', () => {
    const weights = [
      { id: 1, weight: 1 },
      { id: 2, weight: 2 },
      { id: 5, weight: 5 },
    ];
    const sent = runShares(weights, 60_000);
    const total = [...sent.values()].reduce((a, b) => a + b, 0);
    for (const { id, weight } of weights) {
      const share = sent.get(id)! / total;
      const ideal = weight / 8;
      expect(Math.abs(share - ideal)).toBeLessThan(0.02);
    }
  });

  it('converges to new shares after dynamic weight changes', () => {
    const s = new PriorityScheduler(WINDOW);
    s.open(1, WINDOW).open(2, WINDOW).queue(1, WINDOW).queue(2, WINDOW);
    s.setWeight(1, 1);
    s.setWeight(2, 1);
    const sent = new Map<number, number>([
      [1, 0],
      [2, 0],
    ]);
    // phase 1: equal
    for (let i = 0; i < 20_000; i++) {
      const p = s.pick(CHUNK)!;
      s.sent(p.streamId, CHUNK);
      s.queue(p.streamId, CHUNK);
      sent.set(p.streamId, sent.get(p.streamId)! + CHUNK);
    }
    // phase 2: 1:3
    s.setWeight(1, 1);
    s.setWeight(2, 3);
    const phase2 = new Map<number, number>([
      [1, 0],
      [2, 0],
    ]);
    for (let i = 0; i < 40_000; i++) {
      const p = s.pick(CHUNK)!;
      s.sent(p.streamId, CHUNK);
      s.queue(p.streamId, CHUNK);
      phase2.set(p.streamId, phase2.get(p.streamId)! + CHUNK);
    }
    const total = phase2.get(1)! + phase2.get(2)!;
    expect(Math.abs(phase2.get(1)! / total - 0.25)).toBeLessThan(0.02);
    expect(Math.abs(phase2.get(2)! / total - 0.75)).toBeLessThan(0.02);
  });

  it('allocates hierarchical shares: parent weights partition, children split within', () => {
    // root: A(weight 3) vs B(weight 1) => A group 75%, B group 25%
    // A: a1 weight 1, a2 weight 3 => 25%/75% of A => 18.75% / 56.25% total
    // B: b1 weight 1 => 25% total
    const s = new PriorityScheduler(WINDOW);
    s.open(1, WINDOW).open(2, WINDOW).open(3, WINDOW).open(4, WINDOW);
    s.reprioritize({ streamId: 1, dependsOn: 0, weight: 3 }); // A
    s.reprioritize({ streamId: 2, dependsOn: 0, weight: 1 }); // B
    s.open(10, WINDOW).setWeight(10, 1);
    s.open(11, WINDOW).setWeight(11, 3);
    s.open(20, WINDOW).setWeight(20, 1);
    s.reprioritize({ streamId: 10, dependsOn: 1 });
    s.reprioritize({ streamId: 11, dependsOn: 1 });
    s.reprioritize({ streamId: 20, dependsOn: 2 });
    // 1 and 2 are open but blocked (no DATA queued): descendants are scheduled
    // through them and inherit the parent-group share.
    for (const id of [10, 11, 20]) s.queue(id, WINDOW);
    const sent = new Map<number, number>();
    for (let i = 0; i < 80_000; i++) {
      const p = s.pick(CHUNK)!;
      s.sent(p.streamId, CHUNK);
      s.queue(p.streamId, CHUNK);
      sent.set(p.streamId, (sent.get(p.streamId) ?? 0) + CHUNK);
    }
    const total = [...sent.values()].reduce((a, b) => a + b, 0);
    const share = (id: number) => sent.get(id)! / total;
    expect(Math.abs(share(10) - 0.1875)).toBeLessThan(0.02);
    expect(Math.abs(share(11) - 0.5625)).toBeLessThan(0.02);
    expect(Math.abs(share(20) - 0.25)).toBeLessThan(0.02);
  });

  it('closing a parent re-hangs children and their shares continue under grandparent', () => {
    const s = new PriorityScheduler(1_000_000);
    s.open(1, 14_000).setWeight(1, 1);
    s.open(2, 1_000_000).setWeight(2, 1);
    s.open(3, 1_000_000).setWeight(3, 1);
    s.reprioritize({ streamId: 2, dependsOn: 1 });
    s.reprioritize({ streamId: 3, dependsOn: 2 });
    s.queue(1, 14_000);
    s.queue(2, 50_000);
    s.queue(3, 50_000);
    // While 1 has DATA it strictly dominates its descendants; drain it.
    while (s.queuedOf(1) > 0) {
      const p = s.pick(CHUNK)!;
      expect(p.streamId).toBe(1);
      s.sent(1, Math.min(CHUNK, p.budget));
    }
    // 1 empty: the subtree below it becomes schedulable
    expect(s.pick()!.streamId).toBe(2);
    s.close(1); // closed intermediate disappears, subtree re-hung at root
    expect(s.tree.parentOf(2)).toBe(0);
    expect(s.pick()!.streamId).toBe(2);
    // drain 2, close it: 3 must survive attached to root
    while (s.queuedOf(2) > 0) {
      const p = s.pick(CHUNK)!;
      expect(p.streamId).toBe(2);
      s.sent(2, Math.min(CHUNK, p.budget));
    }
    s.close(2);
    expect(s.tree.parentOf(3)).toBe(0);
    expect(s.pick()!.streamId).toBe(3);
  });
});
