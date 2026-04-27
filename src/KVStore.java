import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

// ============================================================
// MAIN KVSTORE CLASS
// Leaderless distributed key-value store.
// Any node accepts reads and writes.
// W=2 quorum: write confirmed on 2/3 nodes before ACK to client.
// R=2 quorum: strong read checks 2 nodes, takes highest version.
// ============================================================

public class KVStore {

    // ---- Constants ----
    private static final int QUORUM            = 2;   // W=2, R=2 out of 3 nodes
    private static final int SNAPSHOT_INTERVAL = 60;  // seconds between snapshots
    private static final int RETRY_INTERVAL    = 5;   // seconds between retry attempts

    // ---- Identity ----
    private final String       nodeId;
    private final List<String> peerNodes;

    // ---- Storage ----
    private final ConcurrentHashMap<String, VersionedValue> store = new ConcurrentHashMap<>();
    private final ReadWriteLock storeLock = new ReentrantReadWriteLock();

    // Version counter — increments on every write this node coordinates
    private final AtomicLong versionCounter = new AtomicLong(0);

    // ---- Persistence ----
    private final WAL             wal;
    private final SnapshotManager snapshotManager;
    private final RetryQueue      retryQueue;

    // ---- Background threads ----
    private final ScheduledExecutorService scheduler =
            Executors.newScheduledThreadPool(2);

    // ============================================================
    // CONSTRUCTOR
    // On startup: load snapshot → replay WAL delta → start background tasks
    // ============================================================
    public KVStore(String nodeId, List<String> peerNodes) {
        this.nodeId    = nodeId;
        this.peerNodes = peerNodes;

        this.wal             = new WAL(nodeId);
        this.snapshotManager = new SnapshotManager(nodeId);
        this.retryQueue      = new RetryQueue(nodeId);

        recoverState();
        startBackgroundTasks();
    }

    // ============================================================
    // RECOVERY
    // Load snapshot first (fast baseline), then replay only the
    // WAL entries written after that snapshot (the delta).
    // ============================================================
    private void recoverState() {
        try {
            long walReplayFrom = -1;

            SnapshotBundle bundle = snapshotManager.loadSnapshot();
            if (bundle != null) {
                store.putAll(bundle.store);
                walReplayFrom = bundle.walIndexAtSnapshot;
                System.out.println("[" + nodeId + "] Loaded snapshot at WAL index " + walReplayFrom);
            }

            List<WALEntry> entries = wal.entriesAfter(walReplayFrom);
            for (WALEntry entry : entries) {
                applyEntry(entry.key, entry.vv);
            }

            long maxVersion = store.values().stream()
                    .mapToLong(v -> v.version).max().orElse(0);
            versionCounter.set(maxVersion + 1);

            System.out.println("[" + nodeId + "] Recovery complete. Keys: " +
                    store.size() + ", WAL entries replayed: " + entries.size());

        } catch (Exception e) {
            System.err.println("[" + nodeId + "] Recovery failed: " + e.getMessage());
        }
    }

    // ============================================================
    // GET
    //   strong=true  → ask all peers, take highest version (linearizable)
    //   strong=false → return local value immediately (fast, possibly stale)
    // ============================================================
    public Object get(String key) {
        return get(key, true);
    }

    public Object get(String key, boolean strong) {
        if (!strong) {
            VersionedValue local = store.get(key);
            return local == null ? null : local.value;
        }

        // Strong read: ask all nodes, return highest version value
        VersionedValue best = store.get(key);

        for (String peer : peerNodes) {
            Map<String, Object> request = new HashMap<>();
            request.put("action", "get");
            request.put("key", key);

            Map<String, Object> response = sendToNode(peer, request);
            if (response != null && response.containsKey("vv")) {
                VersionedValue peerVV = (VersionedValue) response.get("vv");
                if (peerVV != null && peerVV.isNewerThan(best)) {
                    best = peerVV;
                }
            }
        }

        return best == null ? null : best.value;
    }

    // ============================================================
    // PUT
    // 1. Assign version number
    // 2. Write to own WAL (durable before proceeding)
    // 3. Apply to own memory
    // 4. Replicate to all peers concurrently
    // 5. Wait for W=2 total ACKs (self + at least 1 peer)
    // 6. Unacked peers → retry queue
    // ============================================================
    public boolean put(String key, Object value) {
        try {
            long version   = versionCounter.getAndIncrement();
            long timestamp = System.currentTimeMillis();
            VersionedValue vv = new VersionedValue(value, version, timestamp);

            WALEntry entry = wal.append(key, vv);
            applyEntry(key, vv);

            int acks = 1; // self
            List<Future<Boolean>> futures = new ArrayList<>();
            ExecutorService replicationPool = Executors.newFixedThreadPool(peerNodes.size());

            for (String peer : peerNodes) {
                final String p = peer;
                futures.add(replicationPool.submit(() -> replicateToPeer(p, key, vv, entry)));
            }

            for (Future<Boolean> future : futures) {
                try {
                    if (future.get(2, TimeUnit.SECONDS)) acks++;
                } catch (TimeoutException | ExecutionException e) {
                    // handled by retry queue inside replicateToPeer
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            replicationPool.shutdown();

            if (acks >= QUORUM) {
                return true;
            } else {
                System.err.println("[" + nodeId + "] Warning: quorum not reached for key=" + key);
                return false;
            }

        } catch (IOException e) {
            System.err.println("[" + nodeId + "] PUT failed: " + e.getMessage());
            return false;
        }
    }

    // ============================================================
    // REPLICATE TO PEER
    // If peer is down → add to retry queue (stored on disk).
    // ============================================================
    private boolean replicateToPeer(String peer, String key,
                                    VersionedValue vv, WALEntry entry) {
        Map<String, Object> message = new HashMap<>();
        message.put("action", "replicate");
        message.put("key", key);
        message.put("vv", vv);
        message.put("walIndex", entry.index);

        Map<String, Object> response = sendToNode(peer, message);

        if (response != null && Boolean.TRUE.equals(response.get("ack"))) {
            return true;
        } else {
            retryQueue.add(peer, entry);
            System.out.println("[" + nodeId + "] Peer " + peer +
                    " unreachable. Added to retry queue.");
            return false;
        }
    }

    // ============================================================
    // ON MESSAGE — routes incoming peer messages by "action"
    // ============================================================
    public Map<String, Object> onMessage(String fromNode, Map<String, Object> message) {
        Map<String, Object> response = new HashMap<>();
        String action = (String) message.get("action");

        switch (action) {

            case "replicate": {
                String key = (String) message.get("key");
                VersionedValue vv = (VersionedValue) message.get("vv");
                try {
                    wal.append(key, vv);
                    applyEntry(key, vv);
                    response.put("ack", true);
                } catch (IOException e) {
                    response.put("ack", false);
                    response.put("error", e.getMessage());
                }
                break;
            }

            case "get": {
                String key = (String) message.get("key");
                response.put("vv", store.get(key));
                break;
            }

            case "catch_up": {
                long fromIndex = (long) message.get("fromIndex");
                try {
                    List<WALEntry> missing = wal.entriesAfter(fromIndex);
                    response.put("entries", missing);
                    response.put("latestIndex", wal.getLatestIndex());
                } catch (IOException e) {
                    response.put("entries", new ArrayList<>());
                }
                break;
            }

            case "status": {
                response.put("nodeId", nodeId);
                response.put("keys", store.size());
                response.put("latestWalIndex", wal.getLatestIndex());
                response.put("version", versionCounter.get());
                break;
            }

            default:
                response.put("error", "unknown action: " + action);
        }

        return response;
    }

    // ============================================================
    // CATCH UP FROM PEERS
    // Called after restart to sync writes missed while down.
    // ============================================================
    public void catchUpFromPeers() {
        long ourLatestIndex = wal.getLatestIndex();

        for (String peer : peerNodes) {
            Map<String, Object> request = new HashMap<>();
            request.put("action", "catch_up");
            request.put("fromIndex", ourLatestIndex);

            Map<String, Object> response = sendToNode(peer, request);
            if (response == null) continue;

            @SuppressWarnings("unchecked")
            List<WALEntry> missing = (List<WALEntry>) response.get("entries");
            if (missing == null) continue;

            for (WALEntry entry : missing) {
                try {
                    wal.append(entry.key, entry.vv);
                    applyEntry(entry.key, entry.vv);
                } catch (IOException e) {
                    System.err.println("[" + nodeId + "] Catch-up apply failed: " + e.getMessage());
                }
            }

            System.out.println("[" + nodeId + "] Caught up " + missing.size() +
                    " entries from " + peer);
            break;
        }
    }

    // ============================================================
    // APPLY ENTRY — idempotent, version-guarded
    // ============================================================
    private void applyEntry(String key, VersionedValue incoming) {
        store.merge(key, incoming, (existing, newVal) ->
                newVal.isNewerThan(existing) ? newVal : existing
        );
    }

    // ============================================================
    // BACKUP / RESTORE
    // Live backup: snapshot + WAL tail captured under read lock.
    // Restore: apply snapshot then replay WAL tail.
    // ============================================================
    public Map<String, Object> backup() {
        Map<String, VersionedValue> snapshot;
        long snapshotWalIndex;

        storeLock.readLock().lock();
        try {
            snapshot         = new HashMap<>(store);
            snapshotWalIndex = wal.getLatestIndex();
        } finally {
            storeLock.readLock().unlock();
        }

        List<WALEntry> walTail;
        try {
            walTail = wal.entriesAfter(snapshotWalIndex);
        } catch (IOException e) {
            walTail = new ArrayList<>();
        }

        Map<String, Object> bundle = new HashMap<>();
        bundle.put("snapshot", snapshot);
        bundle.put("walTail", walTail);
        bundle.put("snapshotWalIndex", snapshotWalIndex);
        bundle.put("nodeId", nodeId);
        bundle.put("timestamp", System.currentTimeMillis());

        System.out.println("[" + nodeId + "] Backup created. Keys: " +
                snapshot.size() + ", WAL tail: " + walTail.size());
        return bundle;
    }

    @SuppressWarnings("unchecked")
    public void restoreFromBackup(Map<String, Object> bundle) {
        Map<String, VersionedValue> snapshot =
                (Map<String, VersionedValue>) bundle.get("snapshot");
        List<WALEntry> walTail = (List<WALEntry>) bundle.get("walTail");

        storeLock.writeLock().lock();
        try {
            store.clear();
            store.putAll(snapshot);
        } finally {
            storeLock.writeLock().unlock();
        }

        for (WALEntry entry : walTail) {
            applyEntry(entry.key, entry.vv);
        }

        System.out.println("[" + nodeId + "] Restored from backup. Keys: " + store.size());
    }

    // ============================================================
    // BACKGROUND TASKS
    // 1. Periodic snapshots — bounds recovery time
    // 2. Retry queue processor — achieves eventual consistency
    // ============================================================
    private void startBackgroundTasks() {

        scheduler.scheduleAtFixedRate(() -> {
            try {
                snapshotManager.takeSnapshot(store, wal.getLatestIndex(), storeLock);
                System.out.println("[" + nodeId + "] Snapshot taken at WAL index " +
                        wal.getLatestIndex());
            } catch (IOException e) {
                System.err.println("[" + nodeId + "] Snapshot failed: " + e.getMessage());
            }
        }, SNAPSHOT_INTERVAL, SNAPSHOT_INTERVAL, TimeUnit.SECONDS);

        scheduler.scheduleAtFixedRate(() -> {
            for (RetryEntry retryEntry : retryQueue.getAll()) {
                Map<String, Object> message = new HashMap<>();
                message.put("action", "replicate");
                message.put("key", retryEntry.walEntry.key);
                message.put("vv", retryEntry.walEntry.vv);
                message.put("walIndex", retryEntry.walEntry.index);

                Map<String, Object> response =
                        sendToNode(retryEntry.peerId, message);

                if (response != null && Boolean.TRUE.equals(response.get("ack"))) {
                    retryQueue.remove(retryEntry.peerId, retryEntry.walEntry.index);
                    System.out.println("[" + nodeId + "] Retry succeeded for peer " +
                            retryEntry.peerId);
                }
            }
        }, RETRY_INTERVAL, RETRY_INTERVAL, TimeUnit.SECONDS);
    }

    // ============================================================
    // STATUS — diagnostic snapshot
    // ============================================================
    public Map<String, Object> status() {
        Map<String, Object> s = new LinkedHashMap<>();
        s.put("nodeId",         nodeId);
        s.put("keys",           store.size());
        s.put("latestWalIndex", wal.getLatestIndex());
        s.put("version",        versionCounter.get());
        s.put("retryQueueSize", retryQueue.getAll().size());
        return s;
    }

    public void shutdown() {
        scheduler.shutdown();
    }

    // ============================================================
    // SEND TO NODE — black-box RPC (implemented by subclass/framework)
    // Returns null if peer is unreachable
    // ============================================================
    protected Map<String, Object> sendToNode(String targetNode, Map<String, Object> message) {
        return null; // placeholder
    }
}
