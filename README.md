# HTTP/2 connection core

TypeScript library implementing HTTP/2 stream state, flow control and the
RFC 7540 §5.3 dependency/weight prioritization model.

Run `npm install`, then `npm test` and `npm run build`.

## Features

- `FlowWindow` / `Stream` — flow-control credit and the
  idle → open → half-closed → closed state machine.
- `PriorityTree` — acyclic parent/children dependency tree rooted at stream 0:
  - references to missing streams create idle placeholder nodes;
  - self-dependency is rewritten to depend on the root;
  - depending on an ancestor (which would form a cycle) is broken by lifting
    the target dependency node into the stream's old position first
    (nghttp2-compatible restructuring), then reattaching normally;
  - `exclusive` reprioritization makes the stream the sole child and moves
    the parent's former children under it (RFC 5.3.3);
  - closing a node reparents its children to its parent and redistributes the
    closed weight proportionally with integer (floor) math, clamped to 1..256;
  - `validate()` checks the root is unique and every node is reachable
    exactly once (no cycle, no duplicate mount, no detached node).
- `PriorityScheduler` — weighted scheduling on top of the tree:
  - only streams with pending DATA and a positive flow window are eligible;
  - a blocked node forwards its share to ready descendants; a subtree with no
    ready stream is skipped entirely;
  - per-level smooth weighted round-robin gives exact weight proportions over
    a cycle (nested shares multiply along the path);
  - dynamic weight changes take effect immediately;
  - `WINDOW_UPDATE` unblocks streams; idle placeholders are never scheduled.

## Quick start

```ts
import { PriorityScheduler } from './src/index.js';

const s = new PriorityScheduler();
s.open(1, { dependency: 0, weight: 16 });
s.open(2, { dependency: 1, weight: 32, exclusive: true });
s.queueData(1, 1000);
s.grantWindow(1, 1000);

s.schedule();        // 2 — eligible stream id, or null
s.transmit();        // { id, bytes }, consuming DATA + window
s.close(1);          // descendants reparented, weights redistributed
s.tree.validate();   // invariant: every node reachable exactly once
```
