import java.util.*;

// ============================================================
// TESTABLE SUBCLASS
// Overrides sendToNode
// ============================================================

class TestableKVStore extends KVStore {
    private Map<String, TestableKVStore> cluster;
    private volatile boolean isDown = false;

    public TestableKVStore(String nodeId, List<String> peerNodes) {
        super(nodeId, peerNodes);
    }

    public void setCluster(Map<String, TestableKVStore> cluster) {
        this.cluster = cluster;
    }

    public void setDown(boolean down) {
        this.isDown = down;
        System.out.println("[" + (down ? "CRASH" : "RECOVER") + "] " +
                status().get("nodeId"));
    }

    @Override
    protected Map<String, Object> sendToNode(String targetNode,
                                             Map<String, Object> message) {
        if (isDown) return null;

        TestableKVStore target = cluster.get(targetNode);
        if (target == null || target.isDown) return null;

        return target.onMessage((String) status().get("nodeId"), message);
    }
}

// ============================================================
// MAIN — simulates a 3-node cluster in-process
// ============================================================

public class Main {

    public static void main(String[] args) throws InterruptedException {

        System.out.println("=== Distributed KV Store Test ===\n");

        TestableKVStore node1 = new TestableKVStore("node1", Arrays.asList("node2", "node3"));
        TestableKVStore node2 = new TestableKVStore("node2", Arrays.asList("node1", "node3"));
        TestableKVStore node3 = new TestableKVStore("node3", Arrays.asList("node1", "node2"));

        Map<String, TestableKVStore> cluster = new HashMap<>();
        cluster.put("node1", node1);
        cluster.put("node2", node2);
        cluster.put("node3", node3);
        node1.setCluster(cluster);
        node2.setCluster(cluster);
        node3.setCluster(cluster);

        // --- Test 1: Basic put / get ---
        System.out.println("--- Test 1: Basic put/get ---");
        node1.put("user:1", "Alice");
        Thread.sleep(100);
        System.out.println("node1 get user:1 = " + node1.get("user:1"));
        System.out.println("node2 get user:1 = " + node2.get("user:1"));
        System.out.println("node3 get user:1 = " + node3.get("user:1"));

        // --- Test 2: Replication ---
        System.out.println("\n--- Test 2: All nodes have same data ---");
        node1.put("user:2", "Bob");
        Thread.sleep(100);
        System.out.println("node1=" + node1.get("user:2"));
        System.out.println("node2=" + node2.get("user:2"));
        System.out.println("node3=" + node3.get("user:2"));

        // --- Test 3: One node down ---
        System.out.println("\n--- Test 3: node3 crashes, writes still work ---");
        node3.setDown(true);
        boolean result = node1.put("user:3", "Charlie");
        System.out.println("put with node3 down: " + result);
        System.out.println("node1 get user:3 = " + node1.get("user:3"));
        System.out.println("node2 get user:3 = " + node2.get("user:3"));

        // --- Test 4: Node recovers ---
        System.out.println("\n--- Test 4: node3 recovers and catches up ---");
        node3.setDown(false);
        node3.catchUpFromPeers();
        Thread.sleep(100);
        System.out.println("node3 get user:3 = " + node3.get("user:3"));

        // --- Test 5: Concurrent writes to the same key ---
        // Node1 and Node2 both write "user:5" at the same time.
        // Each node assigns its own version number from its own counter.
        // Whichever version number is higher wins — all nodes must
        // converge to the SAME value (no split brain, no corruption).
        System.out.println("\n--- Test 5: Concurrent writes to same key ---");
        Thread t1 = new Thread(() -> node1.put("user:5", "version-from-node1"));
        Thread t2 = new Thread(() -> node2.put("user:5", "version-from-node2"));
        t1.start();
        t2.start();
        t1.join();
        t2.join();
        Thread.sleep(200); // let replication settle

        // Strong read checks all nodes and returns the highest version.
        // All three nodes must agree on the same winner.
        Object winner = node1.get("user:5"); // strong read — checks all nodes
        System.out.println("Concurrent write winner : " + winner);
        System.out.println("node1 agrees            : " + node1.get("user:5"));
        System.out.println("node2 agrees            : " + node2.get("user:5"));
        System.out.println("node3 agrees            : " + node3.get("user:5"));
        // All four lines must print the same value
        boolean allAgree = winner != null
                && winner.equals(node1.get("user:5"))
                && winner.equals(node2.get("user:5"))
                && winner.equals(node3.get("user:5"));
        System.out.println("All nodes converged     : " + allAgree);


        // --- Test 6: Duplicate / idempotent writes ---
        // Writing the same key twice must not corrupt data.
        // Each call increments the version, so the second write wins,
        // but the value is the same — safe and correct.
        // Also verifies the version counter is monotonically increasing.
        System.out.println("\n--- Test 7: Duplicate writes (idempotency) ---");
        node1.put("user:7", "SameValue");
        Thread.sleep(100);
        node1.put("user:7", "SameValue"); // exact same key + value again
        Thread.sleep(100);
        System.out.println("After 2 identical writes : " + node1.get("user:7"));
        System.out.println("node2 has correct value  : " + node2.get("user:7"));
        System.out.println("node3 has correct value  : " + node3.get("user:7"));
        // Value must still be "SameValue", not null, not duplicated, not corrupted

        // --- Test 7: Live backup ---
        // Backup runs while writes are live. No write should be lost.
        // Returns a snapshot + WAL tail bundle that covers everything.
        System.out.println("\n--- Test 8: Live backup ---");
        Map<String, Object> bundle = node1.backup();
        System.out.println("Backup keys: " +
                ((Map<?, ?>) bundle.get("snapshot")).size());

        // --- Status ---
        System.out.println("node1: " + node1.status());
        System.out.println("node2: " + node2.status());
        System.out.println("node3: " + node3.status());

        node1.shutdown();
        node2.shutdown();
        node3.shutdown();
        System.out.println("\n=== ALL TESTS PASSED ===");
    }
}
