import java.io.Serializable;

// ============================================================
// DATA MODEL
// Every stored value carries a timestamp for conflict resolution.
// Higher timestamp wins — last write wins.
//
// Assumption: clock skew between servers is 10-100ms.
// Writes happening seconds apart will always resolve correctly.
// For millisecond-level concurrent writes in production,
// a Lamport clock would be more accurate.
// ============================================================

public class VersionedValue implements Serializable {
    public final Object value;
    public final long timestamp;  // wall clock time of write — used for conflict resolution

    public VersionedValue(Object value, long timestamp) {
        this.value     = value;
        this.timestamp = timestamp;
    }

    // Higher timestamp wins — written later = more recent = wins.
    public boolean isNewerThan(VersionedValue other) {
        if (other == null) return true;
        return this.timestamp > other.timestamp;
    }

    @Override
    public String toString() {
        return String.format("VersionedValue{value=%s, timestamp=%d}", value, timestamp);
    }
}