// HTTP/2 流控窗口（RFC 7540 §6.9）
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

// HTTP/2 流：状态 + 待发送 DATA 字节 + 流级控流窗口
export class Stream {
  state: StreamState = 'idle';
  pendingData = 0;
  window = new FlowWindow();

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
  // 可调度：处于活跃状态、有 DATA、还有窗口
  get ready() {
    return (
      (this.state === 'open' || this.state === 'half-closed') &&
      this.pendingData > 0 &&
      this.window.value > 0
    );
  }
}

// RFC 7540 §5.3：权重取值 1..256，默认 16；0 为根流（虚拟节点）
export const ROOT_STREAM = 0;
export const MIN_WEIGHT = 1;
export const MAX_WEIGHT = 256;
export const DEFAULT_WEIGHT = 16;

export interface PriorityOptions {
  dependency?: number;
  weight?: number;
  exclusive?: boolean;
}

// 依赖树内部节点
class PNode {
  parent: PNode | null = null;
  readonly children = new Set<PNode>();
  constructor(
    readonly id: number,
    public weight: number,
  ) {}
}

/**
 * RFC 7540 §5.3 优先级依赖树（纯拓扑，不感知 Stream 状态）。
 *
 * 不变量：根 0 唯一；每个非根节点恰有一个父节点；从根沿 children
 * 可且仅可达每个节点一次（无环、无重复挂载）。
 */
export class PriorityTree {
  protected readonly nodes = new Map<number, PNode>();
  protected readonly root: PNode;

  constructor() {
    this.root = new PNode(ROOT_STREAM, DEFAULT_WEIGHT);
    this.nodes.set(ROOT_STREAM, this.root);
  }

  has(streamId: number): boolean {
    return this.nodes.has(streamId);
  }

  weightOf(streamId: number): number {
    return this.require(streamId).weight;
  }

  // 根返回 null；其余返回父 id（可能为 0）
  parentOf(streamId: number): number | null {
    const node = this.require(streamId);
    return node.parent ? node.parent.id : null;
  }

  childrenOf(streamId: number): number[] {
    return [...this.require(streamId).children].map((c) => c.id);
  }

  /**
   * PRIORITY / HEADERS 携带的优先级信息（§5.3.1 / §5.3.3）。
   *
   * - 依赖不存在的流：按需创建 idle 占位节点。
   * - 自依赖：改写为依赖根。
   * - 依赖自己的后代（会成环）：按 nghttp2 的权威重排，先把目标
   *   依赖节点本身摘下、上移到本节点当前的位置（它的子节点不动），
   *   本节点再挂到目标依赖下，环即解开，所有节点仍唯一可达。
   */
  reprioritize(
    streamId: number,
    dependencyId: number = ROOT_STREAM,
    weight: number = DEFAULT_WEIGHT,
    exclusive = false,
  ): void {
    if (streamId === ROOT_STREAM) throw new Error('cannot reprioritize root stream');
    if (!Number.isInteger(weight) || weight < MIN_WEIGHT || weight > MAX_WEIGHT) {
      throw new RangeError(`weight must be an integer in [${MIN_WEIGHT}, ${MAX_WEIGHT}]`);
    }
    // §5.3.1：依赖自己 -> 改为依赖根
    if (dependencyId === streamId) dependencyId = ROOT_STREAM;

    const node = this.ensure(streamId);
    const dep = dependencyId === ROOT_STREAM ? this.root : this.ensure(dependencyId);

    // §5.3.1：新父节点是自己的后代 -> 先把目标依赖节点摘下并移到
    // 本节点当前的位置（其后代不动），随后本节点再正常挂到它下面，
    // 环就此解开。与 nghttp2 的 reprioritize 行为一致。
    if (this.isAncestorNode(node, dep)) {
      this.detach(dep);
      this.attach(dep, node.parent ?? this.root);
    }

    if (node.parent !== dep) {
      this.detach(node);
      if (exclusive) {
        // §5.3.3：原兄弟全部变为本节点的子节点，本节点成为唯一依赖
        for (const sibling of [...dep.children]) {
          this.detach(sibling);
          this.attach(sibling, node);
        }
      }
      this.attach(node, dep);
    } else if (exclusive) {
      // 已在目标父节点下：仍需把其它兄弟收编进来
      for (const sibling of [...dep.children]) {
        if (sibling === node) continue;
        this.detach(sibling);
        this.attach(sibling, node);
      }
    }

    node.weight = weight;
  }

  /**
   * §5.3.4 关闭节点：子节点全部重挂到父节点（后代关系不丢失），
   * 关闭节点的权重按各子节点现有权重比例重新分配，随后摘除节点。
   *
   * 新权重 w'_i = max(1, floor(W_closed * w_i / sum(w)))（nghttp2
   * 同款整数算法：整体替换、各自取整，不超过权重上限）。
   */
  close(streamId: number): void {
    if (streamId === ROOT_STREAM) throw new Error('cannot close root stream');
    const node = this.nodes.get(streamId);
    if (!node || node.parent === null) return;

    const parent = node.parent;
    const kids = [...node.children];
    const total = kids.reduce((sum, k) => sum + k.weight, 0);

    for (const k of kids) {
      const next = total > 0 ? Math.floor((node.weight * k.weight) / total) : 0;
      this.detach(k);
      k.weight = Math.min(MAX_WEIGHT, Math.max(MIN_WEIGHT, next));
      this.attach(k, parent);
    }

    this.detach(node);
    this.nodes.delete(streamId);
  }

  isAncestor(maybeAncestorId: number, nodeId: number): boolean {
    return this.isAncestorNode(this.require(maybeAncestorId), this.require(nodeId));
  }

  /**
   * 校验树不变量：从根 DFS，每个节点唯一可达一次，parent/children
   * 双向一致，且 map 中不存在游离节点。返回可达节点数。
   */
  validate(): number {
    const seen = new Set<PNode>();
    const visit = (n: PNode) => {
      if (seen.has(n)) throw new Error(`node ${n.id} reachable more than once (cycle/duplicate mount)`);
      seen.add(n);
      for (const c of n.children) {
        if (c.parent !== n) throw new Error(`node ${c.id} parent/children mismatch`);
        visit(c);
      }
    };
    visit(this.root);
    if (seen.size !== this.nodes.size) {
      throw new Error(`unreachable nodes: ${this.nodes.size - seen.size}`);
    }
    for (const n of this.nodes.values()) {
      if (n !== this.root && n.parent === null) throw new Error(`node ${n.id} detached`);
    }
    return seen.size;
  }

  protected ensure(streamId: number): PNode {
    let n = this.nodes.get(streamId);
    if (!n) {
      n = new PNode(streamId, DEFAULT_WEIGHT);
      this.nodes.set(streamId, n);
      this.attach(n, this.root); // 先挂根，reprioritize 再移动
    }
    return n;
  }

  protected isAncestorNode(maybeAncestor: PNode, node: PNode): boolean {
    let cur: PNode | null = node;
    while (cur) {
      if (cur === maybeAncestor) return true;
      cur = cur.parent;
    }
    return false;
  }

  protected detach(n: PNode) {
    n.parent?.children.delete(n);
    n.parent = null;
  }

  protected attach(n: PNode, parent: PNode) {
    n.parent = parent;
    parent.children.add(n);
  }

  protected require(streamId: number): PNode {
    const n = this.nodes.get(streamId);
    if (!n) throw new Error(`stream ${streamId} not in priority tree`);
    return n;
  }
}

/**
 * 加权调度器：在 PriorityTree 之上做基于权重的近似公平调度。
 *
 * 每层兄弟之间使用确定性的 smooth weighted round-robin（nginx 风格）：
 * 固定成员下，每 sum(weight) 次选择每个节点恰好被选中 weight 次，
 * 长期份额即权重比例；多层树的份额沿路径连乘（§5.3.2 的递归分配）。
 *
 * 仅选择真正有 DATA 且有窗口的活跃流（Stream.ready）。
 * 阻塞节点（无 DATA / 无窗口 / idle 占位）被选中时，其份额按
 * §5.3.2 下传给后代；整棵子树都不可调度时，其权重从本轮剔除，
 * 份额由可调度的兄弟吸收。
 */
export class PriorityScheduler {
  readonly tree = new PriorityTree();
  protected readonly streams = new Map<number, Stream>();
  protected readonly closed = new Set<number>();
  // parentId -> (childId -> smooth round-robin 当前权重)
  private rr = new Map<number, Map<number, number>>();

  /**
   * 处理 PRIORITY 帧。引用不存在的流会创建 idle 占位节点；
   * 已关闭流的优先级更新被忽略，依赖已关闭流时回退到根。
   */
  priority(
    streamId: number,
    dependencyId: number = ROOT_STREAM,
    weight: number = DEFAULT_WEIGHT,
    exclusive = false,
  ): void {
    if (this.closed.has(streamId)) return;
    if (dependencyId !== ROOT_STREAM && this.closed.has(dependencyId)) dependencyId = ROOT_STREAM;
    this.tree.reprioritize(streamId, dependencyId, weight, exclusive);
    this.resetRR();
  }

  /** 流激活（HEADERS）。此前可已有 PRIORITY 建好的占位节点。 */
  open(streamId: number, init?: PriorityOptions): Stream {
    if (streamId === ROOT_STREAM) throw new Error('cannot open root stream');
    if (this.closed.has(streamId)) throw new Error(`stream ${streamId} is closed`);
    let s = this.streams.get(streamId);
    if (!s) {
      s = new Stream();
      this.streams.set(streamId, s);
    }
    if (init || !this.tree.has(streamId)) {
      this.tree.reprioritize(
        streamId,
        init?.dependency ?? ROOT_STREAM,
        init?.weight ?? DEFAULT_WEIGHT,
        init?.exclusive ?? false,
      );
    }
    s.open();
    this.resetRR();
    return s;
  }

  getStream(streamId: number): Stream | undefined {
    return this.streams.get(streamId);
  }

  isIdlePlaceholder(streamId: number): boolean {
    return this.tree.has(streamId) && !this.streams.has(streamId);
  }

  queueData(streamId: number, bytes: number): void {
    if (bytes < 0) throw new Error('negative data');
    this.requireStream(streamId).pendingData += bytes;
  }

  /** WINDOW_UPDATE，解除流的窗口阻塞。 */
  grantWindow(streamId: number, delta: number): void {
    this.requireStream(streamId).window.update(delta);
  }

  /**
   * 关闭流：状态置 closed，树节点摘除并把整棵子树重挂到父节点。
   * 对仅由 PRIORITY 建出的 idle 占位节点同样摘除，避免游离占位。
   */
  close(streamId: number): void {
    this.streams.get(streamId)?.close();
    if (this.tree.has(streamId)) this.tree.close(streamId);
    this.closed.add(streamId);
    this.resetRR();
  }

  /** 选出下一个应发送 DATA 的流；没有可调度流时返回 null。 */
  schedule(): number | null {
    const ready = new Map<number, boolean>();
    this.markReady(ROOT_STREAM, ready);
    if (!ready.get(ROOT_STREAM)) return null;
    return this.pick(ROOT_STREAM, ready);
  }

  /**
   * 选出流并消费其 DATA/窗口。maxBytes 缺省取 min(待发, 窗口)。
   * 显式传入的流若不可调度则返回 null。
   */
  transmit(
    streamId?: number,
    maxBytes: number = Number.POSITIVE_INFINITY,
  ): { id: number; bytes: number } | null {
    const id = streamId ?? this.schedule();
    if (id === null) return null;
    const s = this.streams.get(id);
    if (!s || !s.ready) return null;
    const bytes = Math.min(s.pendingData, s.window.value, maxBytes);
    if (!Number.isFinite(bytes) || bytes <= 0) return null;
    s.window.consume(bytes);
    s.pendingData -= bytes;
    return { id, bytes };
  }

  // 子树内是否存在可调度流（阻塞节点的后代仍可让分支保持合格）
  private markReady(id: number, memo: Map<number, boolean>): boolean {
    let ok = this.streams.get(id)?.ready === true;
    for (const child of this.tree.childrenOf(id)) {
      if (this.markReady(child, memo)) ok = true;
    }
    memo.set(id, ok);
    return ok;
  }

  private pick(parentId: number, ready: Map<number, boolean>): number | null {
    const eligible = this.tree.childrenOf(parentId).filter((id) => ready.get(id));
    if (eligible.length === 0) return null;

    let state = this.rr.get(parentId);
    if (!state) {
      state = new Map();
      this.rr.set(parentId, state);
    }
    const total = eligible.reduce((sum, id) => sum + this.tree.weightOf(id), 0);
    for (const id of eligible) state.set(id, (state.get(id) ?? 0) + this.tree.weightOf(id));

    let chosen = eligible[0];
    for (const id of eligible) {
      if ((state.get(id) ?? 0) > (state.get(chosen) ?? 0)) chosen = id;
    }
    state.set(chosen, (state.get(chosen) ?? 0) - total);

    // 自身可调度即选中；阻塞节点的份额下传给后代（§5.3.2）
    return this.streams.get(chosen)?.ready === true ? chosen : this.pick(chosen, ready);
  }

  private resetRR() {
    this.rr.clear();
  }

  private requireStream(streamId: number): Stream {
    const s = this.streams.get(streamId);
    if (!s) throw new Error(`stream ${streamId} does not exist`);
    return s;
  }
}
