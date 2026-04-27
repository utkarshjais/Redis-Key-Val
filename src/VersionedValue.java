import java.io.Serializable;

// ============================================================
// DATA MODEL
// Every stored value carries metadata for conflict resolution
// ============================================================

public class VersionedValue implements Serializable {
    public final Object value;
    public final long version;    // monotonically increasing, used for conflict resolution
    public final long timestamp;  // tiebreaker if versions are equal (concurrent writes)

    public VersionedValue(Object value, long version, long timestamp) {
        this.value     = value;
        this.version   = version;
        this.timestamp = timestamp;
    }

    // Higher version wins. If tie → higher timestamp wins.
    public boolean isNewerThan(VersionedValue other) {
        if (other == null) return true;
        if (this.version != other.version) return this.version > other.version;
        return this.timestamp > other.timestamp;
    }

    @Override
    public String toString() {
        return String.format("VersionedValue{value=%s, version=%d}", value, version);
    }
}
