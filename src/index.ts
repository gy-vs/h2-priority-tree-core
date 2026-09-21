/**
 * HTTP/2 connection core: flow control, streams and a RFC 7540 §5.3
 * dependency-tree based priority scheduler.
 */

export class FlowWindow {
  constructor(public value = 65535) {}
  consume(size: number) {
    if (size < 0 || size > this.value) throw new Error('flow control');
    this.value -= size;
  }
  update(delta: number) {
    const next = this.value + delta;
    if (delta <= 0 || next > 0x7fffffff) throw new Error('invalid update');
    this.value = next;
  }
}

export type StreamState = 'idle' | 'open' | 'half-closed' | 'closed';

export class Stream {
  state: StreamState = 'idle';
  open() {
    if (this.state !== 'idle') throw new Error('state');
    this.state = 'open';
  }
  halfClose() {
    if (this.state !== 'open') throw new Error('state');
    this.state = 'half-closed';
  }
  close() {
    this.state = 'closed';
  }
}

/** Stream id 0 is the virtual root of the priority tree (RFC 7540 §5.3.1). */
export const ROOT_STREAM = 0;
/** RFC weight value range is 1..256 (wire byte 0..255 + 1). */
export const MIN_WEIGHT = 1;
export const MAX_WEIGHT = 256;
export const DEFAULT_WEIGHT = 16;

export function clampWeight(w: number): number {
  if (!Number.isFinite(w)) return DEFAULT_WEIGHT;
  return Math.min(MAX_WEIGHT, Math.max(MIN_WEIGHT, Math.trunc(w)));
}
/** Convert an on-the-wire RFC 7540 weight byte (0..255) to 1..256. */
export function wireWeightToWeight(byte: number): number {
  return clampWeight(Math.trunc(byte) + 1);
}

type NodeState = StreamState;

interface PriorityNode {
  id: number;
  parent: PriorityNode | null;
  /** Children kept in tree insertion order so ties are deterministic. */
  children: PriorityNode[];
  weight: number;
  state: NodeState;
  /** Bytes queued in this stream (DATA frames waiting to be sent). */
  queued: number;
  /** Stream-level flow-control window (connection window is tracked separately). */
  window: number;
  /**
   * Deficit for Deficit Weighted Round Robin within the parent's sibling
   * group; it is an implementation bookkeeping value, not protocol state.
   */
  deficit: number;
}

export interface PriorityUpdate {
  streamId: number;
  dependsOn: number;
  weight?: number;
  exclusive?: boolean;
}

function assertValidStreamId(id: number) {
  if (!Number.isInteger(id) || id < 0 || id > 0x7fffffff) {
    throw new Error('invalid stream id');
  }
}

/**
 * Acyclic HTTP/2 priority tree.
 *
 * The tree owns stream lifecycle state relevant to prioritization. Streams
 * referenced by PRIORITY/PRIORITY_UPDATE frames that have never been seen are
 * created as `idle` placeholder nodes, as required by RFC 7540 §5.3.1.
 */
export class PriorityTree {
  readonly root: PriorityNode;
  private readonly nodes = new Map<number, PriorityNode>();

  constructor() {
    this.root = this.makeNode(ROOT_STREAM);
    this.root.state = 'open';
    this.nodes.set(ROOT_STREAM, this.root);
  }

  private makeNode(id: number): PriorityNode {
    return {
      id,
      parent: null,
      children: [],
      weight: DEFAULT_WEIGHT,
      state: 'idle',
      queued: 0,
      window: 0,
      deficit: 0,
    };
  }

  /** Existing node, or a new idle placeholder attached to the root. */
  ensure(id: number): PriorityNode {
    assertValidStreamId(id);
    let n = this.nodes.get(id);
    if (!n) {
      n = this.makeNode(id);
      this.attach(n, this.root);
      this.nodes.set(id, n);
    }
    return n;
  }

  get(id: number): PriorityNode | undefined {
    return this.nodes.get(id);
  }

  has(id: number): boolean {
    return this.nodes.has(id);
  }

  /** All reachable stream ids excluding root. */
  reachable(): number[] {
    const out: number[] = [];
    this.walk((n) => {
      if (n !== this.root) out.push(n.id);
    });
    return out;
  }

  /** Pre-order DFS from root (visits every node exactly once if acyclic). */
  walk(visit: (n: PriorityNode) => void) {
    const dfs = (n: PriorityNode) => {
      visit(n);
      for (const c of [...n.children]) dfs(c);
    };
    dfs(this.root);
  }

  private attach(child: PriorityNode, parent: PriorityNode) {
    parent.children.push(child);
    child.parent = parent;
  }

  private detach(child: PriorityNode) {
    const p = child.parent;
    if (!p) return;
    const i = p.children.indexOf(child);
    if (i >= 0) p.children.splice(i, 1);
    child.parent = null;
  }

  /** True if `maybeAncestor` is a strict ancestor of `n` (or the same node). */
  private isAncestor(maybeAncestor: PriorityNode, n: PriorityNode): boolean {
    let cur: PriorityNode | null = n;
    while (cur) {
      if (cur === maybeAncestor) return true;
      cur = cur.parent;
    }
    return false;
  }

  /**
   * Apply a PRIORITY / PRIORITY_UPDATE frame (RFC 7540 §5.3.1/§5.3.2).
   *
   * - A stream depending on itself is re-parented to the root.
   * - If depending on `dependsOn` would form a cycle (the dependency is a
   *   current descendant), the first node below this stream on the path to
   *   the dependency is lifted — together with its whole subtree — to this
   *   stream's parent (RFC 7540 §5.3.1 Figure 7). No node becomes
   *   unreachable and no parent-child relationship other than the lifted
   *   edge is lost.
   * - `exclusive` atomically moves every current child of the new parent
   *   underneath the stream.
   */
  reprioritize(u: PriorityUpdate) {
    const { streamId, dependsOn } = u;
    assertValidStreamId(streamId);
    assertValidStreamId(dependsOn);
    if (streamId === ROOT_STREAM) throw new Error('cannot reprioritize root');

    const node = this.ensure(streamId);
    const weight = u.weight === undefined ? node.weight : clampWeight(u.weight);

    // Self-dependency: treat the request as if depending on the root.
    if (streamId === dependsOn) {
      this.setParent(node, this.root, !!u.exclusive);
      node.weight = weight;
      return;
    }

    const dep = this.ensure(dependsOn);

    // Cycle: dep is a descendant of node. Per RFC 7540 §5.3.1 (Figure 7),
    // detach the ancestor of dep that is directly below node — taking its
    // whole subtree — and re-attach it to node's parent. Then node can safely
    // be moved under dep without forming a loop; all nodes stay reachable.
    if (dep !== node && this.isAncestor(node, dep)) {
      let lift = dep;
      while (lift.parent !== node) lift = lift.parent!;
      this.detach(lift);
      this.attach(lift, node.parent ?? this.root);
      lift.deficit = 0;
    }

    this.setParent(node, dep, !!u.exclusive);
    node.weight = weight;
  }

  private setParent(node: PriorityNode, newParent: PriorityNode, exclusive: boolean) {
    this.detach(node);
    if (exclusive) {
      // Atomic: every existing child of the new parent becomes a child of
      // the moved node, preserving relative order.
      const siblings = newParent.children.splice(0, newParent.children.length);
      for (const s of siblings) {
        s.parent = node;
        node.children.push(s);
        // The siblings join a new sibling group; forget old DWRR credit.
        s.deficit = 0;
      }
    }
    this.attach(node, newParent);
    // The moved node joins a new sibling group; forget old DWRR credit.
    node.deficit = 0;
  }

  open(id: number) {
    const n = this.ensure(id);
    if (n === this.root) return;
    if (n.state !== 'idle') throw new Error('state');
    n.state = 'open';
  }

  halfClose(id: number) {
    const n = this.get(id);
    if (!n || n.state !== 'open') throw new Error('state');
    n.state = 'half-closed';
  }

  /**
   * Close a stream. The node is removed but its subtree survives: every
   * child is re-attached to the closing node's parent (RFC 7540 §5.3.4,
   * dependency on a closed stream is retained by reparenting).
   */
  close(id: number) {
    assertValidStreamId(id);
    if (id === ROOT_STREAM) throw new Error('cannot close root');
    const n = this.nodes.get(id);
    if (!n) return;
    const parent = n.parent ?? this.root;
    const kids = n.children.splice(0, n.children.length);
    this.detach(n);
    for (const c of kids) {
      c.parent = null;
      this.attach(c, parent);
      // New sibling group after reattachment; forget old DWRR credit.
      c.deficit = 0;
    }
    this.nodes.delete(id);
  }

  parentOf(id: number): number {
    const n = this.nodes.get(id);
    return n && n.parent ? n.parent.id : ROOT_STREAM;
  }

  childrenOf(id: number): number[] {
    const n = this.nodes.get(id);
    return n ? n.children.map((c) => c.id) : [];
  }

  weightOf(id: number): number {
    return this.nodes.get(id)?.weight ?? DEFAULT_WEIGHT;
  }

  stateOf(id: number): NodeState | undefined {
    return this.nodes.get(id)?.state;
  }

  setWeight(id: number, weight: number) {
    const n = this.nodes.get(id);
    if (!n) throw new Error('unknown stream');
    n.weight = clampWeight(weight);
  }

  /**
   * Structural invariant: every node except root has exactly one parent, is
   * present in that parent's child list, is reachable from root exactly once,
   * and no parent cycle exists.
   */
  assertInvariant(): void {
    const seen = new Set<number>();
    const check = (n: PriorityNode, depth: number) => {
      if (depth > this.nodes.size + 1) throw new Error('parent cycle');
      if (seen.has(n.id)) throw new Error(`node ${n.id} reachable more than once`);
      seen.add(n.id);
      for (const c of n.children) {
        if (c.parent !== n) throw new Error(`child ${c.id} parent pointer mismatch`);
        check(c, depth + 1);
      }
    };
    check(this.root, 0);
    if (seen.size !== this.nodes.size) {
      const unreachable = [...this.nodes.keys()].filter((id) => !seen.has(id));
      throw new Error(`unreachable nodes: ${unreachable.join(',')}`);
    }
  }
}

export interface PickResult {
  streamId: number;
  /** Bytes the scheduler considers available for this stream right now. */
  budget: number;
}

/**
 * Weighted fair scheduler over a {@link PriorityTree}.
 *
 * Scheduling policy (RFC 7540 §5.3):
 * - only a stream with queued DATA and positive stream *and* connection
 *   window is eligible;
 * - traversal is recursive: a blocked (eligible-but-not-ready, or idle
 *   placeholder) node is skipped and its descendants are considered in its
 *   place — a stream cannot send while its parent is able to send;
 * - sibling groups are served with Deficit Weighted Round Robin so long-term
 *   shares approximate weights.
 */
export class PriorityScheduler {
  readonly tree = new PriorityTree();
  private connectionWindow: number;
  /**
   * DWRR quantum base (bytes of credit granted per unit of weight each round).
   * Actual per-round credit of a sibling is QUANTUM_UNIT * its weight.
   */
  static readonly QUANTUM_UNIT = MAX_WEIGHT;
  private lastSelection: { streamId: number; path: PriorityNode[] } | null = null;

  constructor(connectionWindow = 65535) {
    this.connectionWindow = connectionWindow;
  }

  private node(id: number): PriorityNode {
    const n = this.tree.get(id);
    if (!n || n === this.tree.root) throw new Error('unknown stream');
    return n;
  }

  /** Create a stream (possibly promoting an idle placeholder). */
  open(id: number, window = 65535): this {
    this.tree.open(id);
    const n = this.tree.get(id)!;
    n.window = window;
    return this;
  }

  halfClose(id: number): this {
    this.tree.halfClose(id);
    return this;
  }

  close(id: number): this {
    this.tree.close(id);
    return this;
  }

  reprioritize(u: PriorityUpdate): this {
    this.tree.reprioritize(u);
    return this;
  }

  setWeight(id: number, weight: number): this {
    this.tree.setWeight(id, weight);
    return this;
  }

  /** Queue DATA bytes for a stream (requires an existing, non-idle stream). */
  queue(id: number, bytes: number): this {
    if (!Number.isFinite(bytes) || bytes < 0) throw new Error('invalid bytes');
    const n = this.node(id);
    if (n.state === 'idle' || n.state === 'closed') throw new Error('state');
    n.queued += bytes;
    return this;
  }

  /** WINDOW_UPDATE for a stream. */
  grant(id: number, delta: number): this {
    if (delta <= 0 || delta > 0x7fffffff) throw new Error('invalid update');
    this.node(id).window += delta;
    return this;
  }

  grantConnection(delta: number): this {
    if (delta <= 0 || this.connectionWindow + delta > 0x7fffffff) {
      throw new Error('invalid update');
    }
    this.connectionWindow += delta;
    return this;
  }

  windowOf(id: number): number {
    return this.node(id).window;
  }

  get connectionWindowValue(): number {
    return this.connectionWindow;
  }

  queuedOf(id: number): number {
    return this.node(id).queued;
  }

  weightOf(id: number): number {
    return this.tree.weightOf(id);
  }

  stateOf(id: number): StreamState | undefined {
    return this.tree.stateOf(id);
  }

  /**
   * This stream itself is ready: it carries DATA, has positive stream and
   * connection windows, and is not idle/closed. Idle placeholders are pure
   * dependency shells and never send themselves.
   */
  private isReady(n: PriorityNode): boolean {
    return (
      n !== this.tree.root &&
      n.state !== 'idle' &&
      n.state !== 'closed' &&
      n.queued > 0 &&
      n.window > 0 &&
      this.connectionWindow > 0
    );
  }

  /** Whether n itself or any descendant contains a ready stream. */
  private subtreeHasReady(n: PriorityNode): boolean {
    if (this.isReady(n)) return true;
    for (const c of n.children) if (this.subtreeHasReady(c)) return true;
    return false;
  }

  private minReadyBudget(n: PriorityNode): number {
    return Math.min(n.queued, n.window, this.connectionWindow);
  }

  /**
   * Pick the next stream to send on, or null if nothing is eligible.
   *
   * `maxBytes` caps the chosen send budget. Calling {@link sent} after a
   * successful send deducts DATA, windows and DWRR deficits.
   */
  pick(maxBytes = Number.MAX_SAFE_INTEGER): PickResult | null {
    if (this.connectionWindow <= 0) return null;
    const path: PriorityNode[] = [];
    const found = this.schedule(this.tree.root, path);
    if (!found) return null;
    // Remember the sibling-group winners along the path so sent() can charge
    // the actual byte cost at every level (hierarchical DWRR).
    this.lastSelection = { streamId: found.id, path };
    return {
      streamId: found.id,
      budget: Math.min(maxBytes, this.minReadyBudget(found)),
    };
  }

  /**
   * Recursive weighted selection (RFC 7540 §5.3.2 traversal with DWRR):
   * - a ready node wins over its entire subtree;
   * - an idle/blocked node is invisible and the subtree is scheduled inline;
   * - among siblings, active (subtree-ready) groups accumulate deficit at
   *   weight per round; the highest deficit wins.
   *
   * `path` collects, for every nesting level, the child that won its sibling
   * group so the caller can charge the send's byte cost on each of them.
   */
  private schedule(n: PriorityNode, path: PriorityNode[]): PriorityNode | null {
    if (this.isReady(n)) return n;

    // Deficit is per active set: stale credit from a previous active period
    // must not let a returning stream burst or starve others.
    const active: PriorityNode[] = [];
    for (const c of n.children) {
      if (this.subtreeHasReady(c)) active.push(c);
      else c.deficit = 0;
    }
    if (active.length === 0) return null;

    const Q = PriorityScheduler.QUANTUM_UNIT;

    // DWRR: every active child is served in order; a child whose deficit is
    // exhausted waits for a new round in which every active group receives
    // Q*weight. An active child always resolves (it has a ready descendant),
    // so this loop terminates after at most a couple of rounds.
    // eslint-disable-next-line no-constant-condition
    while (true) {
      let best: PriorityNode | null = null;
      for (const c of active) {
        if (!best || c.deficit > best.deficit) best = c;
      }
      if (!best) return null;
      if (best.deficit <= 0) {
        // Nobody can be served this round: start a new round.
        for (const c of active) c.deficit += Q * c.weight;
        continue;
      }
      path.push(best);
      const picked = this.schedule(best, path);
      if (picked) return picked;
      path.pop();
      // Became inactive concurrently within this traversal; drop and retry.
      best.deficit = 0;
      active.splice(active.indexOf(best), 1);
      if (active.length === 0) return null;
    }
  }

  /**
   * Account for a send of `bytes`: deduct queued DATA, stream window and the
   * connection window, and charge DWRR deficit at every tree level the send
   * passed through. Pairs with {@link pick}.
   */
  sent(id: number, bytes: number): this {
    if (bytes < 0) throw new Error('invalid bytes');
    const n = this.node(id);
    if (bytes > n.queued || bytes > n.window || bytes > this.connectionWindow) {
      throw new Error('flow control');
    }
    n.queued -= bytes;
    n.window -= bytes;
    this.connectionWindow -= bytes;
    if (this.lastSelection && this.lastSelection.streamId === id) {
      for (const g of this.lastSelection.path) g.deficit -= bytes;
      this.lastSelection = null;
    }
    if (n.queued === 0) {
      // Leaving the active set resets DWRR state.
      n.deficit = 0;
    }
    return this;
  }
}
