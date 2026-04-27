import java.io.Serializable;

// ============================================================
// WAL ENTRY
// One line per committed write on disk
// index + key + value + version → enough to fully rebuild state
// ============================================================

public class WALEntry implements Serializable {
    public final long   index;      // sequential, used to find missing entries on catch-up
    public final String key;
    public final VersionedValue vv;

    public WALEntry(long index, String key, VersionedValue vv) {
        this.index = index;
        this.key   = key;
        this.vv    = vv;
    }

    // Serialize to one JSON-like line for easy append
    public String serialize() {
        return String.format("{\"index\":%d,\"key\":\"%s\",\"version\":%d," +
                        "\"timestamp\":%d,\"value\":\"%s\"}",
                index, key, vv.version, vv.timestamp, vv.value);
    }

    public static WALEntry deserialize(String line) {
        long   index     = Long.parseLong(extractField(line, "index"));
        String key       = extractField(line, "key");
        long   version   = Long.parseLong(extractField(line, "version"));
        long   timestamp = Long.parseLong(extractField(line, "timestamp"));
        String value     = extractField(line, "value");
        return new WALEntry(index, key, new VersionedValue(value, version, timestamp));
    }

    private static String extractField(String json, String field) {
        String pattern = "\"" + field + "\":";
        int start = json.indexOf(pattern) + pattern.length();
        if (json.charAt(start) == '"') {
            start++;
            int end = json.indexOf('"', start);
            return json.substring(start, end);
        } else {
            int end = json.indexOf(',', start);
            if (end == -1) end = json.indexOf('}', start);
            return json.substring(start, end);
        }
    }
}
