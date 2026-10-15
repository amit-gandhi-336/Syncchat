package server;

import common.ClockService;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Periodically synchronizes clock times across cluster nodes using the Berkeley Algorithm.
 * Runs immediately at startup and repeats every configured interval (e.g., 60s / 1 min or 120s / 2 min).
 */
public class BerkeleyCoordinator {

    private static final int DEFAULT_INTERVAL_SECONDS = 60;
    private static final DateTimeFormatter TIME_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static class NodeTarget {
        private final int nodeId;
        private final String host;
        private final int port;

        public NodeTarget(int nodeId, String host, int port) {
            this.nodeId = nodeId;
            this.host = host;
            this.port = port;
        }

        public int getNodeId() {
            return nodeId;
        }

        public String getHost() {
            return host;
        }

        public int getPort() {
            return port;
        }

        @Override
        public String toString() {
            return "Node " + nodeId + " (" + host + ":" + port + ")";
        }
    }

    private static class NodeReading {
        final NodeTarget target;
        final ClockService service;
        final long time;

        NodeReading(NodeTarget target, ClockService service, long time) {
            this.target = target;
            this.service = service;
            this.time = time;
        }
    }

    private final java.util.function.Supplier<List<NodeTarget>> targetsSupplier;
    private final int initialDelaySeconds;
    private final int intervalSeconds;
    private final ScheduledExecutorService scheduler;
    private final AtomicInteger roundNumber = new AtomicInteger(1);
    private volatile boolean running = false;

    public BerkeleyCoordinator(List<NodeTarget> targets, int intervalSeconds) {
        this(() -> targets, 0, intervalSeconds, false);
    }

    public BerkeleyCoordinator(List<NodeTarget> targets, int initialDelaySeconds, int intervalSeconds, boolean isDaemon) {
        this(() -> targets, initialDelaySeconds, intervalSeconds, isDaemon);
    }

    public BerkeleyCoordinator(java.util.function.Supplier<List<NodeTarget>> targetsSupplier,
                               int initialDelaySeconds,
                               int intervalSeconds,
                               boolean isDaemon) {
        this.targetsSupplier = targetsSupplier;
        this.initialDelaySeconds = initialDelaySeconds;
        this.intervalSeconds = intervalSeconds;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "berkeley-coordinator-thread");
            thread.setDaemon(isDaemon);
            return thread;
        });
    }

    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;

        System.out.println("=================================================");
        System.out.println("        BERKELEY CLOCK COORDINATOR");
        System.out.println("=================================================");
        System.out.println("Configured target nodes:");
        for (NodeTarget target : targetsSupplier.get()) {
            System.out.println("  - " + target);
        }
        System.out.println("Sync interval: " + intervalSeconds + " seconds ("
                + (intervalSeconds / 60.0) + " minutes)");
        if (initialDelaySeconds > 0) {
            System.out.println("Initial delay: " + initialDelaySeconds + " seconds ("
                    + (initialDelaySeconds / 60.0) + " minutes)");
        }
        System.out.println("=================================================");
        System.out.println("Periodic clock synchronization active.\n");

        scheduler.scheduleWithFixedDelay(
                this::runSynchronizationSafe,
                initialDelaySeconds,
                intervalSeconds,
                TimeUnit.SECONDS
        );
    }

    public synchronized void stop() {
        if (!running) {
            return;
        }
        running = false;
        scheduler.shutdownNow();
        System.out.println("Berkeley clock coordinator stopped.");
    }

    public boolean isRunning() {
        return running;
    }

    private void runSynchronizationSafe() {
        try {
            synchronizeClocks();
        } catch (Throwable t) {
            System.err.println("Unexpected error during clock synchronization round: " + t.getMessage());
            t.printStackTrace(System.err);
        } finally {
            System.out.println("\nNext synchronization scheduled in " + intervalSeconds + "s ("
                    + (intervalSeconds / 60.0) + " min)...\n");
        }
    }

    public synchronized void synchronizeClocks() {
        int round = roundNumber.getAndIncrement();
        String timestamp = LocalDateTime.now().format(TIME_FORMAT);

        System.out.println("-------------------------------------------------");
        System.out.println(" [Round #" + round + " - " + timestamp + "] Synchronizing Clocks");
        System.out.println("-------------------------------------------------");

        List<NodeReading> reachableReadings = new ArrayList<>();

        System.out.println("\n[1] Querying node clocks (Before Synchronization):");
        for (NodeTarget target : targetsSupplier.get()) {
            try {
                Registry registry = LocateRegistry.getRegistry(target.getHost(), target.getPort());
                ClockService service = (ClockService) registry.lookup("ClockService");
                long time = service.getTime();
                reachableReadings.add(new NodeReading(target, service, time));
                System.out.println("  " + target + ": " + time + " ms");
            } catch (Exception e) {
                System.out.println("  " + target + ": [OFFLINE/UNREACHABLE - " + e.getMessage() + "]");
            }
        }

        if (reachableReadings.isEmpty()) {
            System.out.println("\nNo nodes are currently reachable. Skipping synchronization.");
            return;
        }

        if (reachableReadings.size() == 1) {
            System.out.println("\nOnly 1 node is reachable ("
                    + reachableReadings.get(0).target
                    + "). No adjustment needed.");
            return;
        }

        // Calculate average time across reachable nodes
        long sum = 0;
        for (NodeReading reading : reachableReadings) {
            sum += reading.time;
        }
        long average = sum / reachableReadings.size();
        System.out.println("\n[2] Calculated Cluster Average Time: " + average + " ms (across "
                + reachableReadings.size() + " reachable nodes)");

        // Calculate and apply adjustments
        System.out.println("\n[3] Adjustments:");
        for (NodeReading reading : reachableReadings) {
            long adjustment = average - reading.time;
            System.out.println("  " + reading.target + " adjustment: "
                    + (adjustment >= 0 ? "+" : "") + adjustment + " ms");
            try {
                reading.service.adjustClock(adjustment);
            } catch (Exception e) {
                System.err.println("  Failed to apply adjustment to " + reading.target + ": " + e.getMessage());
            }
        }

        // Confirm adjusted times
        System.out.println("\n[4] Verified Clocks (After Synchronization):");
        for (NodeReading reading : reachableReadings) {
            try {
                long verifiedTime = reading.service.getTime();
                System.out.println("  " + reading.target + ": " + verifiedTime + " ms");
            } catch (Exception e) {
                System.err.println("  Could not verify time on " + reading.target + ": " + e.getMessage());
            }
        }
    }

    public static void main(String[] args) {
        try {
            ProcessLog.redirect("berkeley-coordinator");
        } catch (Exception ignored) {
            // Optional file logging; continue if unavailable
        }

        int intervalSeconds = Integer.getInteger(
                "syncchat.berkeley.intervalSeconds",
                DEFAULT_INTERVAL_SECONDS
        );

        List<NodeTarget> targets = new ArrayList<>();

        String clusterConfig = System.getProperty("syncchat.nodes");
        if (clusterConfig != null && !clusterConfig.isBlank()) {
            for (String entry : clusterConfig.split(",")) {
                String[] fields = entry.trim().split("@", 3);
                if (fields.length == 3) {
                    targets.add(new NodeTarget(
                            Integer.parseInt(fields[0]),
                            fields[1],
                            Integer.parseInt(fields[2])
                    ));
                }
            }
        }

        // Parse command line arguments:
        // Case 1: single numeric arg -> interval in seconds
        // Case 2: 3 host arguments -> host1 host2 host3
        // Case 3: 4 arguments -> host1 host2 host3 intervalSeconds
        if (args.length == 1) {
            try {
                intervalSeconds = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                System.err.println("Invalid interval argument: " + args[0]);
                printUsageAndExit();
                return;
            }
        } else if (args.length >= 3) {
            targets.clear();
            targets.add(new NodeTarget(1, args[0], 2001));
            targets.add(new NodeTarget(2, args[1], 2002));
            targets.add(new NodeTarget(3, args[2], 2003));

            if (args.length >= 4) {
                try {
                    intervalSeconds = Integer.parseInt(args[3]);
                } catch (NumberFormatException e) {
                    System.err.println("Invalid interval argument: " + args[3]);
                    printUsageAndExit();
                    return;
                }
            }
        }

        // Default to localhost nodes if none configured yet
        if (targets.isEmpty()) {
            targets.add(new NodeTarget(1, "localhost", 2001));
            targets.add(new NodeTarget(2, "localhost", 2002));
            targets.add(new NodeTarget(3, "localhost", 2003));
        }

        if (intervalSeconds <= 0) {
            System.err.println("Interval must be a positive integer in seconds.");
            return;
        }

        BerkeleyCoordinator coordinator = new BerkeleyCoordinator(targets, intervalSeconds);
        Runtime.getRuntime().addShutdownHook(new Thread(
                coordinator::stop,
                "berkeley-coordinator-shutdown"
        ));
        coordinator.start();
    }

    private static void printUsageAndExit() {
        System.out.println("Usage:");
        System.out.println("  java -cp out server.BerkeleyCoordinator");
        System.out.println("  java -cp out server.BerkeleyCoordinator <intervalSeconds>");
        System.out.println("  java -cp out server.BerkeleyCoordinator <host1> <host2> <host3>");
        System.out.println("  java -cp out server.BerkeleyCoordinator <host1> <host2> <host3> <intervalSeconds>");
        System.out.println("Or via JVM properties:");
        System.out.println("  -Dsyncchat.berkeley.intervalSeconds=<seconds> -Dsyncchat.nodes=<id@host@port,...>");
    }
}