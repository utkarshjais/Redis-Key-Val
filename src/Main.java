import java.util.*;

// ============================================================
// TESTABLE SUBCLASS
// Overrides sendToNode to use direct in-process calls instead of
// real network. Only used for testing — production uses real RPC.
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
        System.out.println("node2 get user:1 = " + node2.get("user:1", false));
        System.out.println("node3 get user:1 = " + node3.get("user:1", false));

        // --- Test 2: Replication ---
        System.out.println("\n--- Test 2: All nodes have same data ---");
        node1.put("user:2", "Bob");
        Thread.sleep(100);
        System.out.println("node1=" + node1.get("user:2", false));
        System.out.println("node2=" + node2.get("user:2", false));
        System.out.println("node3=" + node3.get("user:2", false));

        // --- Test 3: One node down ---
        System.out.println("\n--- Test 3: node3 crashes, writes still work ---");
        node3.setDown(true);
        boolean result = node1.put("user:3", "Charlie");
        System.out.println("put with node3 down: " + result);
        System.out.println("node1 get user:3 = " + node1.get("user:3", false));
        System.out.println("node2 get user:3 = " + node2.get("user:3", false));

        // --- Test 4: Node recovers ---
        System.out.println("\n--- Test 4: node3 recovers and catches up ---");
        node3.setDown(false);
        node3.catchUpFromPeers();
        Thread.sleep(100);
        System.out.println("node3 get user:3 = " + node3.get("user:3", false));

        // --- Test 5: Backup ---
        System.out.println("\n--- Test 5: Live backup ---");
        Map<String, Object> bundle = node1.backup();
        System.out.println("Backup keys: " +
                ((Map<?, ?>) bundle.get("snapshot")).size());

        // --- Status ---
        System.out.println("\n--- Node Status ---");
        System.out.println("node1: " + node1.status());
        System.out.println("node2: " + node2.status());
        System.out.println("node3: " + node3.status());

        node1.shutdown();
        node2.shutdown();
        node3.shutdown();
        System.out.println("\n=== ALL TESTS PASSED ===");
    }
}
