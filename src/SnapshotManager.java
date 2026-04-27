import java.io.*;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;

// ============================================================
// SNAPSHOT MANAGER
// Periodic point-in-time copy of entire store.
// Recovery = load snapshot + replay only WAL delta since snapshot.
// Lock held only for copy (microseconds), not disk write.
// ============================================================

public class SnapshotManager {
    private final String snapshotPath;

    public SnapshotManager(String nodeId) {
        this.snapshotPath = "/tmp/kvstore_snapshot_" + nodeId + ".dat";
    }

    // Copy-on-write: lock held only while copying map reference
    public void takeSnapshot(Map<String, VersionedValue> store,
                             long walIndexAtSnapshot,
                             ReadWriteLock storeLock) throws IOException {

        Map<String, VersionedValue> copy;
        storeLock.readLock().lock();
        try {
            copy = new HashMap<>(store);
        } finally {
            storeLock.readLock().unlock();
        }

        // Disk write outside the lock — writes continue normally
        try (ObjectOutputStream oos =
                     new ObjectOutputStream(new FileOutputStream(snapshotPath))) {
            oos.writeObject(copy);
            oos.writeLong(walIndexAtSnapshot);
        }
    }

    @SuppressWarnings("unchecked")
    public SnapshotBundle loadSnapshot() throws IOException, ClassNotFoundException {
        File file = new File(snapshotPath);
        if (!file.exists()) return null;
        try (ObjectInputStream ois =
                     new ObjectInputStream(new FileInputStream(snapshotPath))) {
            Map<String, VersionedValue> store = (Map<String, VersionedValue>) ois.readObject();
            long walIndex = ois.readLong();
            return new SnapshotBundle(store, walIndex);
        }
    }
}
