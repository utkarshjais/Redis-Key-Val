# Distributed Key-Value Store

A leaderless, fault-tolerant key-value store running across 3 nodes with quorum-based replication, write-ahead logging, and live backup support.

---

## Design Decisions

### 1. Leaderless Architecture
Any node can accept reads and writes. There is no single "master" node.

- **Why:** A leader-based design has a single point of failure. If the leader crashes, the system needs an election before writes resume. Leaderless means any surviving node keeps the system running with no downtime.
- **Tradeoff:** Harder to reason about consistency — two nodes can accept writes for the same key at the same time. Resolved via versioning (see below).

---

### 2. Quorum Writes (W=2) and Reads (R=2)
A write is only confirmed to the client once **2 out of 3** nodes have durably stored it. A strong read contacts all nodes and returns the highest-versioned value.

```
W + R > N
2 + 2 > 3  ✅  — guarantees a read always overlaps with the last write
```

- **Why:** If we required all 3 nodes (W=3), one slow or crashed node would block every write. W=1 is fast but risks data loss if that one node dies before replicating. W=2 is the safe middle ground.
- **Tradeoff:** One node can be temporarily behind (stale). The `get(key, strong=false)` fast path accepts this tradeoff for speed.

---

### 3. Write-Ahead Log (WAL)
Every write is appended to an on-disk log file **before** updating in-memory state.

```
Client PUT
    │
    ▼
Write to WAL (disk) ◄── crash here = safe, replayed on restart
    │
    ▼
Update memory
    │
    ▼
Replicate to peers
```

- **Why:** If a node crashes mid-write and only memory was updated, the data is gone on restart. The WAL survives crashes. On restart, entries are replayed in order to rebuild exact memory state.
- **Tradeoff:** Every write pays a disk I/O cost. Acceptable because disk writes are sequential (fast) and the WAL is append-only.

---

### 4. Snapshots + WAL Delta Recovery
Periodically, the entire in-memory store is saved to disk as a snapshot. On restart, instead of replaying the full WAL from Day 1, the node loads the snapshot and replays only the WAL entries written after it.

```
Recovery = Snapshot (fast baseline) + WAL delta (small)
```

- **Why:** Without snapshots, a node that has been running for months would need to replay millions of WAL entries on restart — unacceptably slow.
- **Tradeoff:** Snapshot writes are heavier than WAL appends. Done on a background thread every 60 seconds to avoid blocking writes.

---

### 5. Hinted Handoff (Retry Queue)
When a peer node is unreachable during replication, the write is not dropped. Instead, it is saved to a **disk-backed retry queue**. A background thread retries delivery every 5 seconds until the peer ACKs.

- **Why:** Without this, a node that was down for 10 minutes would miss all writes during that window and permanently diverge from the cluster. Hinted handoff is how eventual consistency is actually achieved.
- **Tradeoff:** The retry queue node must stay up for hints to be delivered. If the coordinator also crashes, hints on disk survive and are retried on its restart too.

---

### 6. Versioning + Last-Write-Wins
Every value is wrapped with a version number (monotonically incrementing) and a timestamp (tiebreaker). When two nodes receive conflicting values for the same key, the higher version wins.

- **Why:** In a leaderless system, two nodes can accept writes for the same key simultaneously. Without versioning, the outcome would be random depending on replication order. Versioning makes conflict resolution deterministic.
- **Tradeoff:** Last-write-wins means one concurrent write is silently discarded. Acceptable for most use cases. Applications that cannot tolerate this would need vector clocks or application-level merge logic.

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                        CLIENT                               │
│                  put("user:1", "Alice")                     │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                     NODE 1 (coordinator)                    │
│                                                             │
│  1. version = counter++                                     │
│  2. WAL.append(key, value)  ──► disk: wal_node1.log        │
│  3. store.put(key, value)   ──► memory                     │
│  4. replicate concurrently ─────────────────────┐          │
│                                                 │          │
│  5. wait for 2 ACKs (self + 1 peer)             │          │
│  6. return true to client ✅                    │          │
└─────────────────────────────────────────────────┼──────────┘
                                                  │
                    ┌─────────────────────────────┘
                    │                             │
                    ▼                             ▼
     ┌──────────────────────┐      ┌──────────────────────────┐
     │       NODE 2         │      │         NODE 3           │
     │                      │      │                          │
     │  WAL.append() ✅     │      │  unreachable?            │
     │  store.put()  ✅     │      │       │                  │
     │  ACK ──────────────► │      │       ▼                  │
     │                      │      │  RetryQueue.add()        │
     └──────────────────────┘      │  (retried every 5s) 🔄  │
                                   └──────────────────────────┘
```

---

## Node Recovery Flow

```
Node restarts
      │
      ▼
Load Snapshot from disk  ──► fast baseline (e.g. state from 60s ago)
      │
      ▼
Replay WAL entries after snapshot index  ──► fills the delta
      │
      ▼
Ask peers: "what did I miss?"  ──► catchUpFromPeers()
      │
      ▼
Apply missing entries (version-guarded, idempotent)
      │
      ▼
Node is fully caught up ✅
```

---

## Live Backup Flow

```
backup() called
      │
      ▼
Acquire READ lock (microseconds)
      │
      ├── copy in-memory store  ──► snapshot
      └── note current WAL index
      │
Release lock  ──► writes continue normally
      │
      ▼
Read WAL entries written AFTER noted index  ──► WAL tail
      │
      ▼
Bundle = { snapshot + WAL tail }

To restore: apply snapshot, replay WAL tail in order.
Every acknowledged write is in one of the two. ✅
```

---

## File Structure

```
kvstore/
├── VersionedValue.java    # Data model — value + version + timestamp
├── WALEntry.java          # Single WAL record — serialize/deserialize
├── WAL.java               # Append-only on-disk log
├── SnapshotBundle.java    # Snapshot data holder
├── SnapshotManager.java   # Take and load periodic snapshots
├── RetryQueue.java        # Hinted handoff — disk-backed retry queue
├── KVStore.java           # Core store — put/get/replicate/backup/recover
└── Main.java              # TestableKVStore + 8 test cases
```

---

## Running the Tests

```bash
# Compile all files
javac *.java

# Run
java Main
```

Expected output: all 8 tests pass, ending with `=== ALL TESTS PASSED ===`

---

## Test Coverage

| Test | What it verifies |
|------|-----------------|
| 1. Basic put/get | Data is stored and retrievable |
| 2. Replication | Write on Node1 appears on Node2 and Node3 |
| 3. Node crash | System accepts writes with only 2/3 nodes up |
| 4. Node recovery | Crashed node catches up missed writes on restart |
| 5. Concurrent writes | Higher version wins, all nodes converge to same value |
| 6. Strong vs stale read | `strong=false` is fast but may be stale; `strong=true` is always fresh |
| 7. Duplicate writes | Writing same key twice does not corrupt data |
| 8. Live backup | Backup captures all keys with no writes lost |
