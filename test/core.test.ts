import { describe, expect, it } from 'vitest';
import {
  DEFAULT_WEIGHT,
  FlowWindow,
  MAX_WEIGHT,
  MIN_WEIGHT,
  PriorityScheduler,
  PriorityTree,
  Stream,
} from '../src/index.js';

it('tracks credit', () => {
  const x = new FlowWindow(10);
  x.consume(4);
  expect(x.value).toBe(6);
});

// 树不变量：根唯一可达，所有节点恰好出现一次，无环
function expectValidTree(tree: PriorityTree, size: number) {
  expect(tree.validate()).toBe(size);
}

describe('priority tree — dependency to missing stream', () => {
  it('creates idle placeholder nodes for unknown dependencies', () => {
    const t = new PriorityTree();
    t.reprioritize(3, 9, 16, false); // 9 从不存在
    expectValidTree(t, 3); // 根 + 3 + 占位 9
    expect(t.parentOf(9)).toBe(0);
    expect(t.parentOf(3)).toBe(9);
    // 占位节点后续也可以承载自己的优先级
    t.reprioritize(9, 0, 42, false);
    expectValidTree(t, 3);
    expect(t.parentOf(9)).toBe(0);
    expect(t.weightOf(9)).toBe(42);
  });

  it('scheduler exposes placeholders but never schedules them', () => {
    const s = new PriorityScheduler();
    s.priority(7, 5);
    expect(s.isIdlePlaceholder(5)).toBe(true);
    expect(s.isIdlePlaceholder(7)).toBe(true);
    expect(s.schedule()).toBeNull();
    expect(() => s.queueData(5, 100)).toThrow();
  });
});

describe('priority tree — self dependency', () => {
  it('rewrites a stream depending on itself to depend on root', () => {
    const t = new PriorityTree();
    t.reprioritize(3, 3, 20);
    expectValidTree(t, 2);
    expect(t.parentOf(3)).toBe(0);
    expect(t.weightOf(3)).toBe(20);
  });

  it('exclusive self dependency also falls back to root and moves siblings', () => {
    const t = new PriorityTree();
    t.reprioritize(1, 0);
    t.reprioritize(2, 0);
    t.reprioritize(3, 3, 16, true); // exclusive + 自依赖
    expectValidTree(t, 4);
    expect(t.parentOf(3)).toBe(0);
    expect(t.parentOf(1)).toBe(3);
    expect(t.parentOf(2)).toBe(3);
    expect(t.childrenOf(0)).toEqual([3]);
  });

  it('rejects reprioritization of root stream 0', () => {
    const t = new PriorityTree();
    expect(() => t.reprioritize(0, 0)).toThrow();
  });

  it('rejects out-of-range weights', () => {
    const t = new PriorityTree();
    expect(() => t.reprioritize(1, 0, 0)).toThrow();
    expect(() => t.reprioritize(1, 0, 257)).toThrow();
    expect(() => t.reprioritize(1, 0, 1.5)).toThrow();
  });
});

describe('priority tree — ancestor dependency (cycle breaking)', () => {
  it('restructures A->B->C then A depends on C without a cycle', () => {
    const t = new PriorityTree();
    t.reprioritize(1, 0);
    t.reprioritize(2, 1);
    t.reprioritize(3, 2); // 0->1->2->3
    expectValidTree(t, 4);

    t.reprioritize(1, 3); // 1 想依赖自己的后代 3
    expectValidTree(t, 4);
    // nghttp2 语义：只把 dep(3) 摘下移到 stream(1) 的原位，3 的子节点 2
    // 保持不动；1 再挂到 3 下 -> 2 因此成为 1 的子节点（环被解开）。
    expect(t.parentOf(1)).toBe(3);
    expect(t.parentOf(3)).toBe(0);
    expect(t.parentOf(2)).toBe(1);
  });
  it('keeps the moved subtree intact', () => {
    const t = new PriorityTree();
    // 0->[1,4]，1->2，2->3
    t.reprioritize(1, 0);
    t.reprioritize(2, 1);
    t.reprioritize(3, 2);
    t.reprioritize(4, 0);
    // 1 依赖自己的后代 3（成环）：3 上移到 1 的原位（0 下），
    // 3 自身的后代关系 2->3 保持不变；1 再挂到 3 下。
    t.reprioritize(1, 3);
    expectValidTree(t, 5);
    expect(t.parentOf(1)).toBe(3);
    expect(t.parentOf(3)).toBe(0);
    expect(t.parentOf(2)).toBe(1);
    expect(t.parentOf(4)).toBe(0);
  });

  it('handles exclusive flag after cycle restructure', () => {
    const t = new PriorityTree();
    // 0->[1,9]，1->2，2->3（3 是 1 的后代）
    t.reprioritize(1, 0);
    t.reprioritize(2, 1);
    t.reprioritize(3, 2);
    t.reprioritize(9, 0);
    // 1 exclusive 依赖后代 3：
    // 环重组先把 3 本身上移到 1 的原位（0 下），2 仍为 1 的子节点；
    // 1 再独占挂到 3 下——此时 3 下只有 1，无需收编；
    // 1 原本的兄弟 9 留在 0 下（重组后它已是 3 的兄弟，不受影响）。
    t.reprioritize(1, 3, 16, true);
    expectValidTree(t, 5);
    expect(t.parentOf(1)).toBe(3);
    expect(t.parentOf(3)).toBe(0);
    expect(t.parentOf(2)).toBe(1);
    expect(t.parentOf(9)).toBe(0);
    expect(t.childrenOf(3)).toEqual([1]);
  });

  it('exclusive after cycle restructure adopts children of the new parent', () => {
    const t = new PriorityTree();
    // 0->[1,7,9]，1->2，2->3
    t.reprioritize(1, 0);
    t.reprioritize(2, 1);
    t.reprioritize(3, 2);
    t.reprioritize(7, 0);
    t.reprioritize(9, 0);
    // 1 exclusive 依赖后代 3：环重组把 3 移到 0 下（3 此时无子节点），
    // 1 独占插入到 3 下；7、9 是 3 的兄弟而非子节点，故留在 0 下。
    t.reprioritize(1, 3, 16, true);
    expectValidTree(t, 6);
    expect(new Set(t.childrenOf(0))).toEqual(new Set([3, 7, 9]));
    expect(t.childrenOf(3)).toEqual([1]);
    expect(t.childrenOf(1)).toEqual([2]);
  });

  it('exclusive after cycle restructure moves existing children under the node', () => {
    const t = new PriorityTree();
    // 0->1->2->3；0->4；4->5（5 是 4 的子节点）
    t.reprioritize(1, 0);
    t.reprioritize(2, 1);
    t.reprioritize(3, 2);
    t.reprioritize(4, 0);
    t.reprioritize(5, 4);
    // 2 exclusive 依赖后代 3：环重组把 3 移到 2 的原位（1 下），
    // 随后 2 独占挂到 3 下；3 无子节点可收编，拓扑简单。
    t.reprioritize(2, 3, 16, true);
    expectValidTree(t, 6);
    expect(t.parentOf(2)).toBe(3);
    expect(t.parentOf(3)).toBe(1);
    expect(t.parentOf(1)).toBe(0);
    expect(t.parentOf(5)).toBe(4);
  });

  it('exclusive adopts children when new parent already has children', () => {
    const t = new PriorityTree();
    // 0->[1,2]，0->2->3（3 是 2 的后代），0->1->8
    t.reprioritize(1, 0);
    t.reprioritize(2, 0);
    t.reprioritize(3, 2);
    t.reprioritize(8, 1);
    // 2 exclusive 依赖 1（不成环，普通场景）：8 被收编到 2 下
    t.reprioritize(2, 1, 16, true);
    expectValidTree(t, 5);
    expect(t.childrenOf(1)).toEqual([2]);
    expect(new Set(t.childrenOf(2))).toEqual(new Set([3, 8]));
  });
});

describe('priority tree — exclusive reprioritization', () => {
  it('moves all prior siblings under the exclusive node (RFC 5.3.3)', () => {
    const t = new PriorityTree();
    t.reprioritize(1, 0);
    t.reprioritize(2, 0);
    t.reprioritize(4, 0);
    t.reprioritize(6, 2);
    // 0->[1,2,4], 2->6；让 4 exclusive 依赖 0
    t.reprioritize(4, 0, 16, true);
    expectValidTree(t, 5);
    expect(t.childrenOf(0)).toEqual([4]);
    expect(new Set(t.childrenOf(4))).toEqual(new Set([1, 2]));
    expect(t.parentOf(6)).toBe(2); // 兄弟自己的子树原样保留
  });

  it('exclusive move to a new parent preserves subtrees', () => {
    const t = new PriorityTree();
    t.reprioritize(1, 0);
    t.reprioritize(2, 1);
    t.reprioritize(3, 1);
    t.reprioritize(5, 0);
    t.reprioritize(5, 1, 16, true); // 5 独占 1：2、3 收编到 5 下
    expectValidTree(t, 5);
    expect(t.childrenOf(1)).toEqual([5]);
    expect(new Set(t.childrenOf(5))).toEqual(new Set([2, 3]));
    expectValidTree(t, 5);
  });

  it('non-exclusive reprioritize simply reattaches', () => {
    const t = new PriorityTree();
    t.reprioritize(1, 0);
    t.reprioritize(2, 1);
    t.reprioritize(3, 0);
    t.reprioritize(2, 3);
    expectValidTree(t, 4);
    expect(t.parentOf(2)).toBe(3);
    expect(t.childrenOf(1)).toEqual([]);
  });
});

describe('priority tree — closing intermediate nodes', () => {
  it('reparents descendants and redistributes weight proportionally', () => {
    const t = new PriorityTree();
    // 0->1(w16), 1->2(w8), 1->3(w24)
    t.reprioritize(1, 0, 16);
    t.reprioritize(2, 1, 8);
    t.reprioritize(3, 1, 24);
    t.close(1);
    expectValidTree(t, 3);
    expect(t.parentOf(2)).toBe(0);
    expect(t.parentOf(3)).toBe(0);
    // 权重整体替换：w2 = 16*8/32 = 4；w3 = 16*24/32 = 12（恰好整除）
    expect(t.weightOf(2)).toBe(4);
    expect(t.weightOf(3)).toBe(12);
    expect(t.has(1)).toBe(false);
  });

  it('floors proportional shares independently (nghttp2 integer rule)', () => {
    const t = new PriorityTree();
    // 三个等权子节点：floor(16*3/9)=floor(5.33)=5
    t.reprioritize(1, 0, 16);
    t.reprioritize(2, 1, 3);
    t.reprioritize(3, 1, 3);
    t.reprioritize(4, 1, 3);
    t.close(1);
    expectValidTree(t, 4);
    const weights = [t.weightOf(2), t.weightOf(3), t.weightOf(4)].sort();
    expect(weights).toEqual([5, 5, 5]);
  });

  it('keeps at least weight 1 and caps at 256', () => {
    const t = new PriorityTree();
    t.reprioritize(1, 0, 256);
    t.reprioritize(2, 1, 1);
    t.reprioritize(3, 1, 255);
    t.close(1);
    expectValidTree(t, 3);
    expect(t.weightOf(2)).toBe(1); // floor(1.0)=1
    expect(t.weightOf(3)).toBe(255); // floor(255.0)=255
  });

  it('leaf close just removes the node', () => {
    const t = new PriorityTree();
    t.reprioritize(1, 0);
    t.close(1);
    expectValidTree(t, 1);
    expect(t.has(1)).toBe(false);
  });

  it('closing a leaf stream via scheduler leaves descendants scheduled', () => {
    const s = new PriorityScheduler();
    s.open(1, { dependency: 0, weight: 16 });
    s.open(2, { dependency: 1, weight: 16 });
    s.open(3, { dependency: 1, weight: 16 });
    s.queueData(2, 1000);
    s.queueData(3, 1000);
    s.close(1); // 中间节点关闭
    expect(s.tree.parentOf(2)).toBe(0);
    expect(s.tree.parentOf(3)).toBe(0);
    const counts: Record<number, number> = { 2: 0, 3: 0 };
    for (let i = 0; i < 1000; i++) {
      const pick = s.schedule();
      expect(pick === 2 || pick === 3).toBe(true);
      if (pick) counts[pick]++;
    }
    expect(counts[2]).toBe(500);
    expect(counts[3]).toBe(500);
  });

  it('later priority updates to a closed stream are ignored', () => {
    const s = new PriorityScheduler();
    s.open(1);
    s.close(1);
    expect(() => s.priority(1, 0, 200)).not.toThrow();
    expect(s.tree.has(1)).toBe(false);
  });

  it('closing an idle placeholder prunes it and rewrites future dependencies', () => {
    const s = new PriorityScheduler();
    s.priority(5, 3); // 3、5 均为占位
    expect(s.isIdlePlaceholder(3)).toBe(true);
    expect(s.isIdlePlaceholder(5)).toBe(true);
    s.close(3);
    expect(s.tree.has(3)).toBe(false);
    // 5 仍在（它的父 3 关闭后被重挂到根）
    expect(s.tree.parentOf(5)).toBe(0);
    // 后续依赖已关闭占位 3 -> 回退到根
    s.priority(7, 3);
    expect(s.tree.parentOf(7)).toBe(0);
    s.tree.validate();
  });
});

describe('weight bounds', () => {
  it('exposes RFC constants and default weight', () => {
    expect(MIN_WEIGHT).toBe(1);
    expect(MAX_WEIGHT).toBe(256);
    expect(DEFAULT_WEIGHT).toBe(16);
  });
});

describe('scheduling — only ready streams (DATA + window)', () => {
  it('returns null when nothing is enqueued', () => {
    const s = new PriorityScheduler();
    s.open(1);
    expect(s.schedule()).toBeNull();
  });

  it('skips streams with no window and restores after WINDOW_UPDATE', () => {
    const s = new PriorityScheduler();
    s.open(1, { weight: 1 });
    s.open(2, { weight: 1 });
    s.queueData(1, 1000);
    s.queueData(2, 1000);
    const w1 = s.getStream(1)!.window;
    w1.consume(w1.value);
    expect(s.schedule()).toBe(2);
    expect(s.schedule()).toBe(2);
    s.grantWindow(1, 65535);
    // 恢复后两者重新均摊
    const counts = { 1: 0, 2: 0 };
    for (let i = 0; i < 100; i++) counts[s.schedule()!]++;
    expect(counts[1]).toBe(50);
    expect(counts[2]).toBe(50);
  });

  it('does not schedule idle placeholders or closed streams', () => {
    const s = new PriorityScheduler();
    s.priority(5, 3); // 3、5 均为占位
    expect(s.schedule()).toBeNull();
    s.open(1);
    s.queueData(1, 100);
    s.close(1);
    expect(s.schedule()).toBeNull();
  });

  it('half-closed streams remain schedulable until DATA drained', () => {
    const s = new PriorityScheduler();
    s.open(1);
    s.queueData(1, 100);
    const st = s.getStream(1)!;
    st.halfClose();
    expect(st.ready).toBe(true);
    expect(s.schedule()).toBe(1);
  });

  it('blocked ancestor forwards its share to ready descendants', () => {
    const s = new PriorityScheduler();
    // 1 阻塞（无数据），2/3 是它的后代；4 是 1 的活跃兄弟
    s.open(1, { dependency: 0, weight: 16 });
    s.open(2, { dependency: 1, weight: 8 });
    s.open(3, { dependency: 1, weight: 8 });
    s.open(4, { dependency: 0, weight: 16 });
    s.queueData(2, 1e6);
    s.queueData(3, 1e6);
    s.queueData(4, 1e6);
    const counts: Record<number, number> = { 2: 0, 3: 0, 4: 0 };
    for (let i = 0; i < 6400; i++) counts[s.schedule()!]++;
    // 顶层 [1(w16 但阻塞), 4(w16)]：1 的 1/2 份额下传，2、3 平分得 1/4
    // 4 应得 1/2；2、3 各 1/4。SWRR 为精确比例：
    expect(counts[4]).toBe(3200);
    expect(counts[2]).toBe(1600);
    expect(counts[3]).toBe(1600);
  });
});

describe('dynamic weight changes', () => {
  it('rebalances shares immediately after priority() with new weight', () => {
    const s = new PriorityScheduler();
    s.open(1, { weight: 16 });
    s.open(2, { weight: 16 });
    s.queueData(1, 1e7);
    s.queueData(2, 1e7);

    // 阶段一：1:2 = 1:3
    s.priority(2, 0, 48);
    const phase1 = { 1: 0, 2: 0 };
    for (let i = 0; i < 4000; i++) phase1[s.schedule()!]++;
    expect(phase1[1]).toBe(1000);
    expect(phase1[2]).toBe(3000);

    // 阶段二：改为 3:1
    s.priority(1, 0, 48);
    s.priority(2, 0, 16);
    const phase2 = { 1: 0, 2: 0 };
    for (let i = 0; i < 4000; i++) phase2[s.schedule()!]++;
    expect(phase2[1]).toBe(3000);
    expect(phase2[2]).toBe(1000);
  });
});

describe('long-running weighted load', () => {
  it('long-term shares match exact weight ratios at top level', () => {
    const s = new PriorityScheduler();
    const entries: [number, number][] = [
      [1, 1],
      [2, 4],
      [3, 16],
    ];
    for (const [id, w] of entries) {
      s.open(id, { dependency: 0, weight: w });
      s.queueData(id, 1e9);
    }
    const counts: Record<number, number> = { 1: 0, 2: 0, 3: 0 };
    const rounds = 21000;
    for (let i = 0; i < rounds; i++) counts[s.schedule()!]++;
    expect(counts[1]).toBe(1000);
    expect(counts[2]).toBe(4000);
    expect(counts[3]).toBe(16000);
  });

  it('nested tree long-term shares multiply along the path', () => {
    const s = new PriorityScheduler();
    // 顶层 a(w1)/b(w1)；a 下 x(w1)/y(w3)；b 下 z(w1)
    s.open(10, { dependency: 0, weight: 16 }); // a
    s.open(20, { dependency: 0, weight: 16 }); // b
    s.open(1, { dependency: 10, weight: 4 }); // x
    s.open(2, { dependency: 10, weight: 12 }); // y
    s.open(3, { dependency: 20, weight: 16 }); // z
    for (const id of [1, 2, 3]) s.queueData(id, 1e9);
    const counts: Record<number, number> = { 1: 0, 2: 0, 3: 0 };
    for (let i = 0; i < 3200; i++) counts[s.schedule()!]++;
    // x: 1/2 * 1/4 = 1/8；y: 1/2 * 3/4 = 3/8；z: 1/2
    expect(counts[1]).toBe(400);
    expect(counts[2]).toBe(1200);
    expect(counts[3]).toBe(1600);
  });

  it('transmit consumes DATA and flow window and stops when drained', () => {
    const s = new PriorityScheduler();
    s.open(1, { weight: 1 });
    s.open(2, { weight: 1 });
    s.queueData(1, 50);
    s.queueData(2, 1e6);
    let sent1 = 0;
    for (let i = 0; i < 10; i++) {
      const t = s.transmit(undefined, 10); // 每帧 10 字节
      expect(t).not.toBeNull();
      if (t!.id === 1) sent1 += t!.bytes;
    }
    expect(sent1).toBe(50); // 等权：10 次中恰好 5 次给流 1，恰好发完
    // 1 耗尽后只剩 2
    for (let i = 0; i < 50; i++) expect(s.schedule()).toBe(2);
  });

  it('appears fair over many random operations (invariant fuzz)', () => {
    const s = new PriorityScheduler();
    const streams = new Set<number>();
    let nextId = 1;
    for (let round = 0; round < 3000; round++) {
      const r = Math.random();
      if (r < 0.45 && streams.size < 40) {
        const id = nextId++;
        const dep = streams.size && Math.random() < 0.8 ? [...streams][(Math.random() * streams.size) | 0] : 0;
        s.open(id, { dependency: dep, weight: 1 + ((Math.random() * 256) | 0), exclusive: Math.random() < 0.2 });
        streams.add(id);
        if (Math.random() < 0.8) s.queueData(id, Math.random() < 0.5 ? 1000 : 0);
      } else if (r < 0.7 && streams.size > 2) {
        const id = [...streams][(Math.random() * streams.size) | 0];
        const dep = Math.random() < 0.3 ? id : streams.size ? [...streams][(Math.random() * streams.size) | 0] : 0;
        s.priority(id, dep, 1 + ((Math.random() * 256) | 0), Math.random() < 0.25);
      } else if (r < 0.85 && streams.size > 1) {
        const id = [...streams][(Math.random() * streams.size) | 0];
        s.close(id);
        streams.delete(id);
      } else {
        // 给随机流补/撤数据或窗口，再调度
        if (streams.size) {
          const id = [...streams][(Math.random() * streams.size) | 0];
          if (Math.random() < 0.5) s.queueData(id, 1000);
          else s.grantWindow(id, 1 + ((Math.random() * 1000) | 0));
        }
        const pick = s.schedule();
        if (pick !== null) {
          const st = s.getStream(pick)!;
          expect(st.ready).toBe(true);
          const t = s.transmit(pick, 1 + ((Math.random() * 100) | 0));
          expect(t).not.toBeNull();
        }
      }
      // 每轮后树必须保持：唯一可达、无环、无重复挂载
      s.tree.validate();
    }
  });
});

describe('Stream state machine', () => {
  it('guards invalid state transitions', () => {
    const s = new Stream();
    expect(() => s.halfClose()).toThrow();
    s.open();
    expect(() => s.open()).toThrow();
    s.halfClose();
    expect(s.state).toBe('half-closed');
    s.close();
    expect(s.state).toBe('closed');
  });
});
