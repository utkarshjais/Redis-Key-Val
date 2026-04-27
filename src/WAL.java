import java.io.*;
import java.util.*;

// ============================================================
// WRITE-AHEAD LOG
// Append-only file on disk. Written BEFORE memory update.
// On restart → replay to rebuild in-memory state.
// ============================================================

public class WAL {
    private final String   filePath;
    private final Object   writeLock = new Object();
    private       long     nextIndex = 0;

    public WAL(String nodeId) {
        this.filePath  = "/tmp/kvstore_wal_" + nodeId + ".log";
        this.nextIndex = countExistingEntries();
    }

    // Append entry to disk and fsync before returning
    public WALEntry append(String key, VersionedValue vv) throws IOException {
        synchronized (writeLock) {
            WALEntry entry = new WALEntry(nextIndex++, key, vv);
            try (FileWriter fw = new FileWriter(filePath, true);
                 BufferedWriter bw = new BufferedWriter(fw)) {
                bw.write(entry.serialize());
                bw.newLine();
                bw.flush();
                // In production: FileDescriptor.sync() for true fsync
            }
            return entry;
        }
    }

    // Read all entries from disk — used on node restart
    public List<WALEntry> readAll() throws IOException {
        List<WALEntry> entries = new ArrayList<>();
        File file = new File(filePath);
        if (!file.exists()) return entries;
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (!line.trim().isEmpty()) {
                    entries.add(WALEntry.deserialize(line));
                }
            }
        }
        return entries;
    }

    // Return all entries after a given index — used for catch-up replication
    public List<WALEntry> entriesAfter(long index) throws IOException {
        List<WALEntry> all    = readAll();
        List<WALEntry> result = new ArrayList<>();
        for (WALEntry e : all) {
            if (e.index > index) result.add(e);
        }
        return result;
    }

    public long getLatestIndex() {
        return nextIndex - 1;
    }

    private long countExistingEntries() {
        try {
            List<WALEntry> entries = readAll();
            return entries.isEmpty() ? 0 : entries.get(entries.size() - 1).index + 1;
        } catch (IOException e) {
            return 0;
        }
    }
}
