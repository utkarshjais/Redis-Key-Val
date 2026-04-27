import java.util.Map;

// ============================================================
// SNAPSHOT BUNDLE
// Holds a point-in-time store snapshot plus the WAL index
// at which it was taken, so recovery can replay only the delta.
// ============================================================

public class SnapshotBundle {
    public final Map<String, VersionedValue> store;
    public final long walIndexAtSnapshot;  // replay WAL entries after this index

    public SnapshotBundle(Map<String, VersionedValue> store, long walIndexAtSnapshot) {
        this.store              = store;
        this.walIndexAtSnapshot = walIndexAtSnapshot;
    }
}
