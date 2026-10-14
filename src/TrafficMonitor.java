import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class TrafficMonitor implements Serializable {

    private static final long serialVersionUID = 1L;

    // Traffic tracking per replica node (nodeId -> count)
    private static final Map<Integer, AtomicLong> readCounts = new ConcurrentHashMap<>();
    private static final Map<Integer, AtomicLong> writeCounts = new ConcurrentHashMap<>();
    private static final Map<Integer, AtomicLong> bytesTransferred = new ConcurrentHashMap<>();
    private static final Map<Integer, Long> lastActiveTimestamps = new ConcurrentHashMap<>();

    // Failover event history
    private static final List<String> failoverEvents = Collections.synchronizedList(new ArrayList<>());
    private static final SimpleDateFormat TIME_FMT = new SimpleDateFormat("HH:mm:ss");

    static {
        for (int i = 0; i < 5; i++) {
            readCounts.put(i, new AtomicLong(0));
            writeCounts.put(i, new AtomicLong(0));
            bytesTransferred.put(i, new AtomicLong(0));
        }
    }

    public static void recordRead(int nodeId, long bytes) {
        readCounts.computeIfAbsent(nodeId, k -> new AtomicLong(0)).incrementAndGet();
        bytesTransferred.computeIfAbsent(nodeId, k -> new AtomicLong(0)).addAndGet(bytes);
        lastActiveTimestamps.put(nodeId, System.currentTimeMillis());
    }

    public static void recordWrite(int nodeId, long bytes) {
        writeCounts.computeIfAbsent(nodeId, k -> new AtomicLong(0)).incrementAndGet();
        bytesTransferred.computeIfAbsent(nodeId, k -> new AtomicLong(0)).addAndGet(bytes);
        lastActiveTimestamps.put(nodeId, System.currentTimeMillis());
    }

    public interface TrafficEventListener {
        void onFailoverEvent(String event);
    }
    private static final List<TrafficEventListener> listeners = new java.util.concurrent.CopyOnWriteArrayList<>();

    public static void addListener(TrafficEventListener l) {
        listeners.add(l);
    }

    public static void recordFailover(int fromNode, int toNode, String fileName, String reason) {
        String time = TIME_FMT.format(new Date());
        String event = "[" + time + "] ⚠️ FAILOVER: Traffic for '" + fileName + "' transferred from Replica " 
                + fromNode + " -> " + (toNode >= 0 ? "Replica " + toNode : "Surviving Node") + " (" + reason + ")";
        failoverEvents.add(event);
        System.out.println("[@TrafficMonitor] " + event);
        for (TrafficEventListener l : listeners) {
            try {
                l.onFailoverEvent(event);
            } catch (Exception ignored) {}
        }
    }

    public static void recordFailover(int nodeId, String reason) {
        recordFailover(nodeId, -1, "System", reason);
    }

    // Track ground-truth node alive states
    private static final Map<Integer, Boolean> lastKnownAliveMap = new ConcurrentHashMap<>();

    // Traffic transfer mappings (when a node fails, its traffic is transferred to another node)
    private static final Map<Integer, Integer> failoverTargetNode = new ConcurrentHashMap<>();
    private static final Map<Integer, Set<Integer>> failoverSourceNodes = new ConcurrentHashMap<>();
    private static final Map<Integer, Long> transferredOutOps = new ConcurrentHashMap<>();
    private static final Map<Integer, Long> transferredOutBytes = new ConcurrentHashMap<>();
    private static final Map<Integer, AtomicLong> absorbedOps = new ConcurrentHashMap<>();
    private static final Map<Integer, AtomicLong> absorbedBytes = new ConcurrentHashMap<>();

    static {
        for (int i = 0; i < 5; i++) {
            lastKnownAliveMap.put(i, true);
        }
    }

    public static class FileTrafficPlan implements Serializable {
        private static final long serialVersionUID = 1L;
        public String fileName;
        public long fileSize;
        public int chunkSize;
        public int totalChunks;
        public int primaryNodeId = -1;
        public List<Integer> slaveNodeIds = new ArrayList<>();
        public Map<Integer, Long> nodeExpectedOps = new HashMap<>();
        public Map<Integer, Long> nodeExpectedBytes = new HashMap<>();
        public Map<Integer, Double> nodeWorkloadPercentage = new HashMap<>();
        public long totalProjectedOps;
        public long totalProjectedBytes;

        @Override
        public String toString() {
            return String.format("Plan for %s (%d bytes, %d chunks): Primary=Replica %d, Slaves=%s, Total Ops=%d",
                    fileName, fileSize, totalChunks, primaryNodeId, slaveNodeIds, totalProjectedOps);
        }
    }

    /**
     * Calculates the exact traffic distribution across cluster nodes for an uploaded file
     */
    public static FileTrafficPlan calculateTrafficDistribution(String fileName, long fileSize, int chunkSize) {
        FileTrafficPlan plan = new FileTrafficPlan();
        plan.fileName = fileName;
        plan.fileSize = Math.max(0, fileSize);
        plan.chunkSize = Math.max(1, chunkSize);
        plan.totalChunks = (int) Math.max(1, Math.ceil(1.0 * plan.fileSize / plan.chunkSize));

        List<Integer> alive = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            if (isNodeAlive(i)) alive.add(i);
        }

        if (alive.isEmpty()) {
            return plan;
        }

        // Sort alive nodes by lowest current workload to balance incoming traffic
        alive.sort((a, b) -> {
            long opsA = getTotalOps(a);
            long opsB = getTotalOps(b);
            if (opsA != opsB) return Long.compare(opsA, opsB);
            return Long.compare(getBytesTransferred(a), getBytesTransferred(b));
        });

        int repN = Math.min(Configurations.REPLICATION_N, alive.size());
        plan.primaryNodeId = alive.get(0);
        for (int i = 1; i < repN; i++) {
            plan.slaveNodeIds.add(alive.get(i));
        }

        // Primary replica handles write of every single chunk
        long primaryOps = plan.totalChunks;
        long primaryBytes = plan.fileSize;
        plan.nodeExpectedOps.put(plan.primaryNodeId, primaryOps);
        plan.nodeExpectedBytes.put(plan.primaryNodeId, primaryBytes);

        long totalOps = primaryOps;
        long totalBytes = primaryBytes;

        // Slave replicas handle reflected chunk update on commit
        for (int slaveId : plan.slaveNodeIds) {
            long slaveOps = 1;
            long slaveBytes = plan.fileSize;
            plan.nodeExpectedOps.put(slaveId, slaveOps);
            plan.nodeExpectedBytes.put(slaveId, slaveBytes);
            totalOps += slaveOps;
            totalBytes += slaveBytes;
        }

        plan.totalProjectedOps = totalOps;
        plan.totalProjectedBytes = totalBytes;

        for (int i = 0; i < 5; i++) {
            long b = plan.nodeExpectedBytes.getOrDefault(i, 0L);
            double pct = (totalBytes > 0) ? (100.0 * b / totalBytes) : 0.0;
            plan.nodeWorkloadPercentage.put(i, pct);
        }

        return plan;
    }

    private static final Map<Integer, Boolean> lastReportedAliveMap = new ConcurrentHashMap<>();

    /**
     * Recalculates failover routing across the entire cluster.
     * If 1 node fails, its traffic transfers to another alive node.
     * If only 1 node is online (and all others failed), 100% of all cluster traffic transfers to that sole online node.
     */
    public static synchronized void updateClusterTopology(Map<Integer, Boolean> nodeAliveMap) {
        boolean changed = false;
        if (nodeAliveMap != null) {
            for (Map.Entry<Integer, Boolean> e : nodeAliveMap.entrySet()) {
                Boolean old = lastKnownAliveMap.put(e.getKey(), e.getValue());
                if (old == null || !old.equals(e.getValue())) {
                    changed = true;
                }
            }
        }

        if (!changed && !lastReportedAliveMap.isEmpty() && lastReportedAliveMap.equals(lastKnownAliveMap)) {
            return;
        }
        lastReportedAliveMap.putAll(lastKnownAliveMap);

        List<Integer> aliveNodes = new ArrayList<>();
        List<Integer> offlineNodes = new ArrayList<>();

        for (int i = 0; i < 5; i++) {
            boolean alive = lastKnownAliveMap.getOrDefault(i, true);
            if (alive) {
                aliveNodes.add(i);
            } else {
                offlineNodes.add(i);
            }
        }

        // Reset previous routing mappings to recalculate cleanly
        failoverTargetNode.clear();
        failoverSourceNodes.clear();
        transferredOutOps.clear();
        transferredOutBytes.clear();
        absorbedOps.clear();
        absorbedBytes.clear();

        if (offlineNodes.isEmpty() || aliveNodes.isEmpty()) {
            notifyTopologyChanged();
            return;
        }

        // Case 1: ONLY ONE NODE IS ONLINE -> All offline nodes transfer 100% of their workload to this sole node!
        if (aliveNodes.size() == 1) {
            int soleSurvivor = aliveNodes.get(0);
            for (int failedNode : offlineNodes) {
                failoverTargetNode.put(failedNode, soleSurvivor);
                failoverSourceNodes.computeIfAbsent(soleSurvivor, k -> ConcurrentHashMap.newKeySet()).add(failedNode);

                long ops = getTotalOps(failedNode);
                long bytes = getBytesTransferred(failedNode);
                transferredOutOps.put(failedNode, ops);
                transferredOutBytes.put(failedNode, bytes);

                absorbedOps.computeIfAbsent(soleSurvivor, k -> new AtomicLong(0)).addAndGet(ops);
                absorbedBytes.computeIfAbsent(soleSurvivor, k -> new AtomicLong(0)).addAndGet(bytes);
            }
        } else {
            // Case 2: Multiple nodes alive -> Distribute each failed node to the next alive node in ring
            for (int failedNode : offlineNodes) {
                int target = -1;
                for (int step = 1; step < 5; step++) {
                    int candidate = (failedNode + step) % 5;
                    if (aliveNodes.contains(candidate)) {
                        target = candidate;
                        break;
                    }
                }
                if (target == -1) target = aliveNodes.get(0);

                failoverTargetNode.put(failedNode, target);
                failoverSourceNodes.computeIfAbsent(target, k -> ConcurrentHashMap.newKeySet()).add(failedNode);

                long ops = getTotalOps(failedNode);
                long bytes = getBytesTransferred(failedNode);
                transferredOutOps.put(failedNode, ops);
                transferredOutBytes.put(failedNode, bytes);

                absorbedOps.computeIfAbsent(target, k -> new AtomicLong(0)).addAndGet(ops);
                absorbedBytes.computeIfAbsent(target, k -> new AtomicLong(0)).addAndGet(bytes);
            }
        }

        notifyTopologyChanged();
    }

    private static void notifyTopologyChanged() {
        for (TrafficEventListener l : listeners) {
            try {
                l.onFailoverEvent("Cluster topology routing updated");
            } catch (Exception ignored) {}
        }
    }

    public static void setNodeAlive(int nodeId, boolean isAlive) {
        lastKnownAliveMap.put(nodeId, isAlive);
        updateClusterTopology(lastKnownAliveMap);
    }

    public static boolean isNodeAlive(int nodeId) {
        return lastKnownAliveMap.getOrDefault(nodeId, true);
    }

    public static int getAliveCount() {
        int count = 0;
        for (int i = 0; i < 5; i++) {
            if (lastKnownAliveMap.getOrDefault(i, true)) count++;
        }
        return count;
    }

    public static boolean isSoleSurvivor(int nodeId) {
        return isNodeAlive(nodeId) && getAliveCount() == 1;
    }

    public static int getSoleSurvivorId() {
        int count = 0;
        int sole = -1;
        for (int i = 0; i < 5; i++) {
            if (lastKnownAliveMap.getOrDefault(i, true)) {
                count++;
                sole = i;
            }
        }
        return (count == 1) ? sole : -1;
    }

    public static void transferNodeTraffic(int fromNode, int toNode) {
        lastKnownAliveMap.put(fromNode, false);
        if (toNode >= 0) lastKnownAliveMap.put(toNode, true);
        updateClusterTopology(lastKnownAliveMap);
    }

    public static void restoreNodeTraffic(int nodeId) {
        lastKnownAliveMap.put(nodeId, true);
        updateClusterTopology(lastKnownAliveMap);
    }

    public static int getFailoverTarget(int nodeId) {
        return failoverTargetNode.getOrDefault(nodeId, -1);
    }

    public static Set<Integer> getFailoverSources(int nodeId) {
        Set<Integer> s = failoverSourceNodes.get(nodeId);
        return s != null ? new HashSet<>(s) : Collections.emptySet();
    }

    public static long getAbsorbedOps(int nodeId) {
        AtomicLong val = absorbedOps.get(nodeId);
        return val != null ? Math.max(0, val.get()) : 0;
    }

    public static long getAbsorbedBytes(int nodeId) {
        AtomicLong val = absorbedBytes.get(nodeId);
        return val != null ? Math.max(0, val.get()) : 0;
    }

    public static long getTransferredOutOps(int nodeId) {
        return transferredOutOps.getOrDefault(nodeId, 0L);
    }

    public static long getTransferredOutBytes(int nodeId) {
        return transferredOutBytes.getOrDefault(nodeId, 0L);
    }

    public static Map<Integer, Integer> getAllActiveTransfers() {
        return new HashMap<>(failoverTargetNode);
    }

    public static long getReadOps(int nodeId) {
        return readCounts.getOrDefault(nodeId, new AtomicLong(0)).get();
    }

    public static long getWriteOps(int nodeId) {
        return writeCounts.getOrDefault(nodeId, new AtomicLong(0)).get();
    }

    public static long getTotalOps(int nodeId) {
        return getReadOps(nodeId) + getWriteOps(nodeId);
    }

    public static long getBytesTransferred(int nodeId) {
        return bytesTransferred.getOrDefault(nodeId, new AtomicLong(0)).get();
    }

    public static String getLastActive(int nodeId) {
        Long ts = lastActiveTimestamps.get(nodeId);
        if (ts == null) return "None yet";
        return TIME_FMT.format(new Date(ts));
    }

    public static List<String> getRecentFailovers() {
        synchronized (failoverEvents) {
            return new ArrayList<>(failoverEvents);
        }
    }

    public static void resetAll() {
        for (int i = 0; i < 5; i++) {
            readCounts.put(i, new AtomicLong(0));
            writeCounts.put(i, new AtomicLong(0));
            bytesTransferred.put(i, new AtomicLong(0));
        }
        lastActiveTimestamps.clear();
        failoverEvents.clear();
        failoverTargetNode.clear();
        failoverSourceNodes.clear();
        transferredOutOps.clear();
        transferredOutBytes.clear();
        absorbedOps.clear();
        absorbedBytes.clear();
    }
}
