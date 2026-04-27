import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

// ============================================================
// RETRY ENTRY
// Holds a single pending replication hint: target peer + WAL entry.
// ============================================================

class RetryEntry implements Serializable {
    public final String   peerId;
    public final WALEntry walEntry;

    public RetryEntry(String peerId, WALEntry walEntry) {
        this.peerId   = peerId;
        this.walEntry = walEntry;
    }
}

// ============================================================
// RETRY QUEUE (Hinted Handoff)
// If a peer is down during replication → store hint on DISK.
// Background thread retries until peer ACKs.
// Disk storage ensures hints survive a crash of this node.
// ============================================================

public class RetryQueue {
    private final ConcurrentHashMap<String, RetryEntry> queue = new ConcurrentHashMap<>();
    private final String queueFilePath;

    public RetryQueue(String nodeId) {
        this.queueFilePath = System.getProperty("java.io.tmpdir") + "/kvstore_retry_" + nodeId + ".dat";
        loadFromDisk();
    }

    public void add(String peerId, WALEntry entry) {
        String key = peerId + ":" + entry.index;
        queue.put(key, new RetryEntry(peerId, entry));
        persistToDisk();
    }

    public void remove(String peerId, long walIndex) {
        queue.remove(peerId + ":" + walIndex);
        persistToDisk();
    }

    public Collection<RetryEntry> getAll() {
        return Collections.unmodifiableCollection(queue.values());
    }

    private void persistToDisk() {
        try (ObjectOutputStream oos =
                     new ObjectOutputStream(new FileOutputStream(queueFilePath))) {
            oos.writeObject(new HashMap<>(queue));
        } catch (IOException e) {
            System.err.println("Warning: could not persist retry queue: " + e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    private void loadFromDisk() {
        File file = new File(queueFilePath);
        if (!file.exists()) return;
        try (ObjectInputStream ois =
                     new ObjectInputStream(new FileInputStream(file))) {
            Map<String, RetryEntry> loaded = (Map<String, RetryEntry>) ois.readObject();
            queue.putAll(loaded);
        } catch (Exception e) {
            System.err.println("Warning: could not load retry queue: " + e.getMessage());
        }
    }
}