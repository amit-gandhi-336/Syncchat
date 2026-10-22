package server;

import common.*;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import java.util.concurrent.*;

public class ChatServer
        extends UnicastRemoteObject
        implements ChatService,
                   ClockService,
                   NodeService,
                   ReplicationService,
                   MapReduceService,
                   MPICollectiveService {

    private final List<String> mpiLocalBuffer =
            Collections.synchronizedList(new ArrayList<>());

    private volatile int[][] localMatrixB;
    private volatile int[][] localRowsA;

    private final Set<String> users =
            ConcurrentHashMap.newKeySet();

    private final Map<String, List<Message>> messages =
            new ConcurrentHashMap<>();

    private final ExecutorService pool =
            Executors.newFixedThreadPool(5);

    private final LogicalClock clock;

    private final int nodeId;

    private volatile boolean primary;

    private volatile int currentPrimary;

    private final Map<Integer, NodeInfo> nodes =
            new ConcurrentHashMap<>();

    private final ConsistencyMode consistencyMode;

    private BerkeleyCoordinator berkeleyCoordinator;

    private static final int MAP_BATCH_SIZE = 100;

    public ChatServer(
            int nodeId,
            long offset,
            boolean primary,
            ConsistencyMode consistencyMode,
            int exportPort)
            throws RemoteException {

        super(exportPort);

        this.nodeId = nodeId;
        this.primary = primary;

        this.currentPrimary =
                primary ? nodeId : 1;

        this.consistencyMode =
                consistencyMode;

        clock =
                new LogicalClock(offset);
    }

    // =====================================================
    // CHAT METHODS
    // =====================================================

    private void replicateToBackups(
            String sender,
            String receiver,
            String content) {

        for (NodeInfo node : nodes.values()) {

            if (node.getNodeId() == nodeId) {
                continue;
            }

            try {

                Registry registry =
                        LocateRegistry.getRegistry(
                                node.getHost(),
                                node.getPort()
                        );

                NodeService nodeService =
                        (NodeService)
                                registry.lookup(
                                        "NodeService"
                                );

                if (nodeService.isPrimary()) {
                    continue;
                }

                ReplicationService service =
                        (ReplicationService)
                                registry.lookup(
                                        "ReplicationService"
                                );

                service.replicateMessage(
                        sender,
                        receiver,
                        content
                );

                System.out.println(
                        "Replication ACK from Node "
                                + node.getNodeId()
                );

            } catch (Exception e) {

                boolean nodeAvailable =
                        isNodeAvailable(node);

                if (consistencyMode ==
                        ConsistencyMode.STRONG
                        && nodeAvailable) {

                    throw new RuntimeException(
                            "Strong replication failed on reachable backup Node "
                                    + node.getNodeId()
                    );
                }

                System.out.println(
                        nodeAvailable
                                ? "Replication failed for reachable Node "
                                + node.getNodeId()
                                : "Backup Node "
                                + node.getNodeId()
                                + " is unavailable; skipping replication for this operation."
                );
            }
        }
    }

    private boolean isNodeAvailable(
            NodeInfo node) {

        try {

            Registry registry =
                    LocateRegistry.getRegistry(
                            node.getHost(),
                            node.getPort()
                    );

            NodeService service =
                    (NodeService)
                            registry.lookup(
                                    "NodeService"
                            );

            return service.isAlive();

        } catch (Exception e) {

            return false;
        }
    }

    @Override
    public boolean registerUser(
            String username)
            throws RemoteException {

        if (!primary) {

            throw new RemoteException(
                    "This node is a BACKUP. " +
                    "Register user through PRIMARY."
            );
        }

        if (users.contains(username)) {
            return false;
        }

        users.add(username);

        messages.put(
                username,
                Collections.synchronizedList(
                        new ArrayList<>()
                )
        );

        System.out.println(
                "Node " + nodeId +
                " registered user: " +
                username
        );

        if (consistencyMode ==
                ConsistencyMode.STRONG) {

            replicateUserToBackups(username);

        } else {

            pool.submit(() ->
                    replicateUserToBackups(
                            username
                    )
            );
        }

        return true;
    }

    @Override
    public List<String> getRegisteredUsers()
            throws RemoteException {

        return users.stream()
                .sorted()
                .toList();
    }

    private void replicateUserToBackups(
            String username) {

        for (NodeInfo node : nodes.values()) {

            if (node.getNodeId() == nodeId) {
                continue;
            }

            try {

                Registry registry =
                        LocateRegistry.getRegistry(
                                node.getHost(),
                                node.getPort()
                        );

                NodeService nodeService =
                        (NodeService)
                                registry.lookup(
                                        "NodeService"
                                );

                if (nodeService.isPrimary()) {
                    continue;
                }

                ReplicationService service =
                        (ReplicationService)
                                registry.lookup(
                                        "ReplicationService"
                                );

                service.replicateUser(username);

                System.out.println(
                        "User replication ACK from Node "
                                + node.getNodeId()
                );

            } catch (Exception e) {

                boolean nodeAvailable =
                        isNodeAvailable(node);

                if (consistencyMode ==
                        ConsistencyMode.STRONG
                        && nodeAvailable) {

                    throw new RuntimeException(
                            "User replication failed on reachable backup Node "
                                    + node.getNodeId()
                    );
                }

                System.out.println(
                        nodeAvailable
                                ? "User replication failed for reachable Node "
                                + node.getNodeId()
                                : "Backup Node "
                                + node.getNodeId()
                                + " is unavailable; skipping user replication."
                );
            }
        }
    }

    @Override
    public boolean replicateUser(
            String username)
            throws RemoteException {

        if (users.contains(username)) {
            return true;
        }

        users.add(username);

        messages.put(
                username,
                Collections.synchronizedList(
                        new ArrayList<>()
                )
        );

        System.out.println(
                "Node " + nodeId +
                " replicated user: " +
                username
        );

        return true;
    }

    @Override
    public void sendMessage(
            String sender,
            String receiver,
            String content)
            throws RemoteException {

        if (!primary) {

            throw new RemoteException(
                    "This node is a BACKUP. " +
                    "Send write request to the PRIMARY."
            );
        }

        if (!users.contains(receiver)) {

            throw new RemoteException(
                    "Receiver does not exist."
            );
        }

        Message message =
                new Message(
                        sender,
                        receiver,
                        content
                );

        messages
                .get(receiver)
                .add(message);

        System.out.println(
                "Node " + nodeId +
                " stored message locally."
        );

        if (consistencyMode ==
                ConsistencyMode.STRONG) {

            System.out.println(
                    "STRONG CONSISTENCY: " +
                    "Waiting for backup acknowledgements..."
            );

            replicateToBackups(
                    sender,
                    receiver,
                    content
            );

            System.out.println(
                    "All available backups acknowledged."
            );

        } else {

            System.out.println(
                    "EVENTUAL CONSISTENCY: " +
                    "Replicating in background..."
            );

            pool.submit(() -> {

                try {

                    replicateToBackups(
                            sender,
                            receiver,
                            content
                    );

                } catch (Exception e) {

                    System.out.println(
                            "Background replication failed: "
                                    + e.getMessage()
                    );
                }
            });
        }
    }

    @Override
    public List<Message> getMessages(
            String username)
            throws RemoteException {

        List<Message> list =
                messages.get(username);

        if (list == null) {
            return new ArrayList<>();
        }

        synchronized (list) {
            return new ArrayList<>(list);
        }
    }

    // =====================================================
    // PRIMARY-BACKUP STATE REPLICATION
    // =====================================================

    @Override
    public Map<String, List<Message>> getState()
            throws RemoteException {

        Map<String, List<Message>> state =
                new HashMap<>();

        for (String username : users) {

            List<Message> userMessages =
                    messages.get(username);

            if (userMessages == null) {

                state.put(
                        username,
                        new ArrayList<>()
                );

            } else {

                synchronized (userMessages) {

                    state.put(
                            username,
                            new ArrayList<>(
                                    userMessages
                            )
                    );
                }
            }
        }

        return state;
    }

    @Override
    public void installState(
            Map<String, List<Message>> state)
            throws RemoteException {

        if (state == null) {
            return;
        }

        users.clear();
        messages.clear();

        for (Map.Entry<String, List<Message>> entry :
                state.entrySet()) {

            String username =
                    entry.getKey();

            List<Message> userMessages =
                    entry.getValue();

            users.add(username);

            messages.put(
                    username,
                    Collections.synchronizedList(
                            new ArrayList<>(
                                    userMessages == null
                                            ? new ArrayList<>()
                                            : userMessages
                            )
                    )
            );
        }

        System.out.println(
                "Node " + nodeId +
                " restored replicated state."
        );
    }

    private void synchronizeStateFromBackup() {

        System.out.println(
                "Starting state recovery..."
        );

        for (NodeInfo node : nodes.values()) {

            if (node.getNodeId() == nodeId) {
                continue;
            }

            try {

                Registry registry =
                        LocateRegistry.getRegistry(
                                node.getHost(),
                                node.getPort()
                        );

                NodeService nodeService =
                        (NodeService)
                                registry.lookup(
                                        "NodeService"
                                );

                if (!nodeService.isAlive()) {
                    continue;
                }

                ReplicationService replicationService =
                        (ReplicationService)
                                registry.lookup(
                                        "ReplicationService"
                                );

                Map<String, List<Message>> state =
                        replicationService.getState();

                if (state == null) {
                    continue;
                }

                installState(state);

                int totalMessages = 0;

                for (List<Message> list :
                        messages.values()) {

                    totalMessages += list.size();
                }

                System.out.println();
                System.out.println(
                        "================================="
                );
                System.out.println(
                        "STATE RECOVERY SUCCESSFUL"
                );
                System.out.println(
                        "Recovered from Node "
                                + node.getNodeId()
                );
                System.out.println(
                        "Users recovered: "
                                + users.size()
                );
                System.out.println(
                        "Messages recovered: "
                                + totalMessages
                );
                System.out.println(
                        "================================="
                );

                return;

            } catch (Exception e) {

                System.out.println(
                        "Unable to recover state from Node "
                                + node.getNodeId()
                );
            }
        }

        System.out.println(
                "No backup was available for state recovery."
        );
    }

    // =====================================================
    // MAP REDUCE
    // =====================================================

    @Override
    public Map<String, Long> mapMessageCounts(
            List<Message> batch)
            throws RemoteException {

        Map<String, Long> partialCounts =
                new HashMap<>();

        for (Message message : batch) {

            partialCounts.merge(
                    message.getSender(),
                    1L,
                    Long::sum
            );
        }

        return partialCounts;
    }

    @Override
    public Map<String, Long> getMessageCountsBySender()
            throws RemoteException {

        if (!primary) {

            throw new RemoteException(
                    "Run chat analytics through the PRIMARY node."
            );
        }

        List<Message> snapshot =
                new ArrayList<>();

        for (List<Message> inbox :
                messages.values()) {

            synchronized (inbox) {
                snapshot.addAll(inbox);
            }
        }

        List<NodeInfo> workers =
                new ArrayList<>(
                        nodes.values()
                );

        workers.sort(
                Comparator.comparingInt(
                        NodeInfo::getNodeId
                )
        );

        if (workers.isEmpty()) {

            workers.add(
                    new NodeInfo(
                            nodeId,
                            "localhost",
                            0
                    )
            );
        }

        List<CompletableFuture<Map<String, Long>>>
                mapResults =
                new ArrayList<>();

        int taskNumber = 0;

        for (
                int start = 0;
                start < snapshot.size();
                start += MAP_BATCH_SIZE
        ) {

            int end =
                    Math.min(
                            start + MAP_BATCH_SIZE,
                            snapshot.size()
                    );

            List<Message> batch =
                    new ArrayList<>(
                            snapshot.subList(
                                    start,
                                    end
                            )
                    );

            NodeInfo worker =
                    workers.get(
                            taskNumber++
                                    % workers.size()
                    );

            mapResults.add(
                    CompletableFuture.supplyAsync(
                            () -> {

                                try {

                                    if (worker.getNodeId()
                                            == nodeId) {

                                        return mapMessageCounts(
                                                batch
                                        );
                                    }

                                    Registry registry =
                                            LocateRegistry.getRegistry(
                                                    worker.getHost(),
                                                    worker.getPort()
                                            );

                                    MapReduceService service =
                                            (MapReduceService)
                                                    registry.lookup(
                                                            "MapReduceService"
                                                    );

                                    return service.mapMessageCounts(
                                            batch
                                    );

                                } catch (Exception e) {

                                    System.err.println(
                                            "Map task failed on Node "
                                                    + worker.getNodeId()
                                                    + "; mapping batch locally: "
                                                    + e.getMessage()
                                    );

                                    try {

                                        return mapMessageCounts(
                                                batch
                                        );

                                    } catch (
                                            RemoteException impossible) {

                                        throw new CompletionException(
                                                impossible
                                        );
                                    }
                                }

                            },
                            pool
                    )
            );
        }

        Map<String, Long> totals =
                new TreeMap<>();

        for (
                CompletableFuture<Map<String, Long>>
                        result : mapResults
        ) {

            result.join().forEach(
                    (sender, count) ->
                            totals.merge(
                                    sender,
                                    count,
                                    Long::sum
                            )
            );
        }

        return totals;
    }

    // =====================================================
    // SERVER METHODS
    // =====================================================

    @Override
    public String getServerStatus()
            throws RemoteException {

        return "SyncChat Server is running";
    }

    // =====================================================
    // CLOCK METHODS
    // =====================================================

    @Override
    public long getTime()
            throws RemoteException {

        return clock.getTime();
    }

    @Override
    public void adjustClock(
            long adjustment)
            throws RemoteException {

        clock.adjust(adjustment);
    }

    // =====================================================
    // NODE METHODS
    // =====================================================

    public void addNode(NodeInfo node) {

        nodes.put(
                node.getNodeId(),
                node
        );
    }

    public Map<Integer, NodeInfo> getNodes() {

        return nodes;
    }

    public int getCurrentPrimary() {

        return currentPrimary;
    }

    @Override
    public int getNodeId()
            throws RemoteException {

        return nodeId;
    }

    @Override
    public boolean isAlive()
            throws RemoteException {

        return true;
    }

    @Override
    public boolean isPrimary()
            throws RemoteException {

        return primary;
    }

    @Override
    public void setPrimary(
            boolean primary)
            throws RemoteException {

        this.primary = primary;
        if (primary) {
            startBerkeleyCoordinator();
        } else {
            stopBerkeleyCoordinator();
        }
    }

    // =====================================================
    // BERKELEY COORDINATOR MANAGEMENT
    // =====================================================

    public synchronized void startBerkeleyCoordinator() {
        boolean enabled = Boolean.parseBoolean(
                System.getProperty("syncchat.berkeley.enabled", "true")
        );
        if (!enabled) {
            return;
        }
        if (berkeleyCoordinator != null && berkeleyCoordinator.isRunning()) {
            return;
        }
        int intervalSeconds = Integer.getInteger(
                "syncchat.berkeley.intervalSeconds",
                60
        );
        int initialDelaySeconds = Integer.getInteger(
                "syncchat.berkeley.initialDelaySeconds",
                intervalSeconds
        );
        berkeleyCoordinator = new BerkeleyCoordinator(
                this::getCoordinatorTargets,
                initialDelaySeconds,
                intervalSeconds,
                true
        );
        berkeleyCoordinator.start();
        System.out.println(
                "Berkeley clock coordinator started on Primary (Node " + nodeId
                        + ", initial delay: " + initialDelaySeconds + "s, interval: "
                        + intervalSeconds + "s)."
        );
    }

    public synchronized void stopBerkeleyCoordinator() {
        if (berkeleyCoordinator != null) {
            berkeleyCoordinator.stop();
            berkeleyCoordinator = null;
            System.out.println(
                    "Berkeley clock coordinator stopped on Node " + nodeId + "."
            );
        }
    }

    private List<BerkeleyCoordinator.NodeTarget> getCoordinatorTargets() {
        List<BerkeleyCoordinator.NodeTarget> list = new ArrayList<>();
        for (NodeInfo info : nodes.values()) {
            list.add(new BerkeleyCoordinator.NodeTarget(
                    info.getNodeId(),
                    info.getHost(),
                    info.getPort()
            ));
        }
        return list;
    }

    // =====================================================
    // RING ELECTION
    // =====================================================

    @Override
    public void startRingElection()
            throws RemoteException {

        RingElection election =
                new RingElection(
                        nodeId,
                        nodes
                );

        election.startElection();
    }

    @Override
    public void forwardElectionMessage(
            int candidateId)
            throws RemoteException {

        RingElection election =
                new RingElection(
                        nodeId,
                        nodes
                );

        election.receiveElectionMessage(candidateId);
    }

    // =====================================================
    // PRIMARY ANNOUNCEMENT
    // =====================================================

    @Override
    public void announcePrimary(
            int winnerId)
            throws RemoteException {

        currentPrimary = winnerId;

        if (nodeId == winnerId) {

            primary = true;

            System.out.println();
            System.out.println(
                    "================================="
            );

            System.out.println(
                    "Node " + nodeId +
                    " IS NOW PRIMARY"
            );

            System.out.println(
                    "Starting state recovery..."
            );

            System.out.println(
                    "================================="
            );

            pool.submit(
                    this::synchronizeStateFromBackup
            );

            startBerkeleyCoordinator();

        } else {

            primary = false;

            stopBerkeleyCoordinator();

            System.out.println(
                    "Node " + nodeId +
                    " is BACKUP"
            );
        }
    }

    // =====================================================
    // BACKUP REPLICATION
    // =====================================================

    @Override
    public void replicateMessage(
            String sender,
            String receiver,
            String content)
            throws RemoteException {

        if (!users.contains(receiver)) {

            throw new RemoteException(
                    "Receiver does not exist on Node "
                            + nodeId
            );
        }

        messages
                .get(receiver)
                .add(
                        new Message(
                                sender,
                                receiver,
                                content
                        )
                );

        System.out.println(
                "Node " + nodeId +
                " replicated message: " +
                sender +
                " -> " +
                receiver
        );
    }

    // =====================================================
    // MPI COLLECTIVE COMMUNICATION (BROADCAST, SCATTER, GATHER)
    // =====================================================

    private List<NodeInfo> getOrderedRanks() {
        Map<Integer, NodeInfo> allRanks = new HashMap<>(nodes);
        if (!allRanks.containsKey(nodeId)) {
            allRanks.put(nodeId, new NodeInfo(nodeId, "localhost", 2000 + nodeId));
        }
        List<NodeInfo> rankList = new ArrayList<>();
        for (NodeInfo info : allRanks.values()) {
            if (info.getNodeId() == nodeId || isNodeAvailable(info)) {
                rankList.add(info);
            }
        }
        if (rankList.isEmpty()) {
            rankList.add(new NodeInfo(nodeId, "localhost", 2000 + nodeId));
        }
        rankList.sort(Comparator.comparingInt(NodeInfo::getNodeId));
        return rankList;
    }

    @Override
    public void receiveBroadcast(String rootRankInfo, String message) throws RemoteException {
        System.out.println();
        System.out.println("-------------------------------------------------");
        System.out.println("[MPI BROADCAST RECEIVED] on Node " + nodeId);
        System.out.println("  From Root : " + rootRankInfo);
        System.out.println("  Data      : \"" + message + "\"");
        System.out.println("-------------------------------------------------");
    }

    @Override
    public void receiveScatterChunk(int rank, List<String> chunk) throws RemoteException {
        mpiLocalBuffer.clear();
        mpiLocalBuffer.addAll(chunk);

        System.out.println();
        System.out.println("-------------------------------------------------");
        System.out.println("[MPI SCATTER RECEIVED] on Rank " + rank + " (Node " + nodeId + ")");
        System.out.println("  Received Chunk (" + chunk.size() + " items): " + chunk);

        // Process the chunk locally (e.g., uppercase transformation for gather)
        for (int i = 0; i < mpiLocalBuffer.size(); i++) {
            mpiLocalBuffer.set(i, "[Rank " + rank + " PROCESSED]: " + mpiLocalBuffer.get(i).toUpperCase());
        }
        System.out.println("  Processed Chunk: " + mpiLocalBuffer);
        System.out.println("-------------------------------------------------");
    }

    @Override
    public List<String> getGatherChunk(int rank) throws RemoteException {
        System.out.println("[MPI GATHER] Node " + nodeId + " (Rank " + rank + ") transmitting " + mpiLocalBuffer.size() + " items back to Root.");
        return new ArrayList<>(mpiLocalBuffer);
    }

    @Override
    public void mpiBroadcast(String message) throws RemoteException {
        List<NodeInfo> rankList = getOrderedRanks();
        System.out.println();
        System.out.println("=================================================");
        System.out.println(" [MPI BROADCAST] Root (Node " + nodeId + ") broadcasting to " + rankList.size() + " processes");
        System.out.println(" Broadcast Message: \"" + message + "\"");
        System.out.println("=================================================");

        for (int rank = 0; rank < rankList.size(); rank++) {
            NodeInfo node = rankList.get(rank);
            try {
                if (node.getNodeId() == this.nodeId) {
                    receiveBroadcast("Root Rank " + rank + " (Node " + nodeId + ")", message);
                } else {
                    Registry registry = LocateRegistry.getRegistry(node.getHost(), node.getPort());
                    MPICollectiveService service = (MPICollectiveService) registry.lookup("MPICollectiveService");
                    service.receiveBroadcast("Root Node " + nodeId, message);
                }
                System.out.println("  [ACK] Broadcast successfully sent to Rank " + rank + " (Node " + node.getNodeId() + ")");
            } catch (Exception e) {
                System.err.println("  [FAIL] Failed to broadcast to Rank " + rank + " (Node " + node.getNodeId() + "): " + e.getMessage());
            }
        }
        System.out.println("=================================================");
    }

    @Override
    public List<String> mpiScatterGather(List<String> items) throws RemoteException {
        List<NodeInfo> rankList = getOrderedRanks();
        int P = rankList.size();
        int totalItems = items.size();
        int chunkSize = (int) Math.ceil((double) totalItems / P);

        System.out.println();
        System.out.println("=================================================");
        System.out.println(" [MPI SCATTER] Root (Node " + nodeId + ") scattering " + totalItems + " items across " + P + " processes");
        System.out.println(" Input Buffer: " + items);
        System.out.println("=================================================");

        // 1. SCATTER PHASE
        for (int rank = 0; rank < P; rank++) {
            int start = rank * chunkSize;
            int end = Math.min(start + chunkSize, totalItems);
            List<String> chunk = (start < totalItems) ? new ArrayList<>(items.subList(start, end)) : new ArrayList<>();

            NodeInfo node = rankList.get(rank);
            try {
                if (node.getNodeId() == this.nodeId) {
                    receiveScatterChunk(rank, chunk);
                } else {
                    Registry registry = LocateRegistry.getRegistry(node.getHost(), node.getPort());
                    MPICollectiveService service = (MPICollectiveService) registry.lookup("MPICollectiveService");
                    service.receiveScatterChunk(rank, chunk);
                }
                System.out.println("  -> Dispatched chunk " + chunk + " to Rank " + rank + " (Node " + node.getNodeId() + ")");
            } catch (Exception e) {
                System.err.println("  [FAIL] Scatter to Rank " + rank + " failed: " + e.getMessage());
            }
        }

        // 2. GATHER PHASE
        System.out.println();
        System.out.println("=================================================");
        System.out.println(" [MPI GATHER] Root (Node " + nodeId + ") gathering processed chunks in rank order");
        System.out.println("=================================================");

        List<String> assembledBuffer = new ArrayList<>();
        for (int rank = 0; rank < P; rank++) {
            NodeInfo node = rankList.get(rank);
            try {
                List<String> chunk;
                if (node.getNodeId() == this.nodeId) {
                    chunk = getGatherChunk(rank);
                } else {
                    Registry registry = LocateRegistry.getRegistry(node.getHost(), node.getPort());
                    MPICollectiveService service = (MPICollectiveService) registry.lookup("MPICollectiveService");
                    chunk = service.getGatherChunk(rank);
                }
                assembledBuffer.addAll(chunk);
                System.out.println("  <- Gathered " + chunk.size() + " items from Rank " + rank + " (Node " + node.getNodeId() + "): " + chunk);
            } catch (Exception e) {
                System.err.println("  [FAIL] Gather from Rank " + rank + " failed: " + e.getMessage());
            }
        }

        System.out.println("=================================================");
        System.out.println(" [MPI GATHER COMPLETE] Assembled " + assembledBuffer.size() + " items:");
        System.out.println(" Result: " + assembledBuffer);
        System.out.println("=================================================");
        return assembledBuffer;
    }

    // =====================================================
    // MPI PARALLEL MATRIX MULTIPLICATION (EXP 10)
    // =====================================================

    @Override
    public void mpiBcastMatrixB(int[][] matrixB) throws RemoteException {
        this.localMatrixB = matrixB;
        System.out.println("[MPI_Bcast] Node " + nodeId + " received full Matrix B (" + matrixB.length + "x" + matrixB[0].length + ")");
    }

    @Override
    public void mpiScatterRowsA(int rank, int[][] rowsA) throws RemoteException {
        this.localRowsA = rowsA;
        System.out.println("[MPI_Scatter] Rank " + rank + " (Node " + nodeId + ") received " + rowsA.length + " rows of Matrix A");
    }

    @Override
    public int[][] mpiComputeAndGatherRowsC(int rank) throws RemoteException {
        if (localRowsA == null || localMatrixB == null) {
            throw new RemoteException("Node " + nodeId + " missing Matrix A or B for computation.");
        }
        int numRows = localRowsA.length;
        int kLen = localMatrixB.length;
        int numCols = localMatrixB[0].length;
        int[][] rowsC = new int[numRows][numCols];

        for (int i = 0; i < numRows; i++) {
            for (int j = 0; j < numCols; j++) {
                int sum = 0;
                for (int k = 0; k < kLen; k++) {
                    sum += localRowsA[i][k] * localMatrixB[k][j];
                }
                rowsC[i][j] = sum;
            }
        }
        System.out.println("[MPI_Compute & Gather] Rank " + rank + " (Node " + nodeId + ") computed " + numRows + " rows of Matrix C");
        return rowsC;
    }

    @Override
    public String runParallelMatrixMultiplication(int matrixSize) throws RemoteException {
        if (!primary) {
            throw new RemoteException("Run parallel matrix multiplication through the PRIMARY node.");
        }
        int N = matrixSize <= 0 ? 150 : matrixSize;
        Random rand = new Random(42);

        int[][] A = new int[N][N];
        int[][] B = new int[N][N];
        for (int i = 0; i < N; i++) {
            for (int j = 0; j < N; j++) {
                A[i][j] = rand.nextInt(10);
                B[i][j] = rand.nextInt(10);
            }
        }

        // 1. SEQUENTIAL MULTIPLICATION (BASELINE)
        long seqStart = System.nanoTime();
        int[][] C_seq = new int[N][N];
        for (int i = 0; i < N; i++) {
            for (int j = 0; j < N; j++) {
                int sum = 0;
                for (int k = 0; k < N; k++) {
                    sum += A[i][k] * B[k][j];
                }
                C_seq[i][j] = sum;
            }
        }
        long seqEnd = System.nanoTime();
        double seqDurationMs = (seqEnd - seqStart) / 1_000_000.0;

        // 2. PARALLEL MPI MULTIPLICATION
        List<NodeInfo> rankList = getOrderedRanks();
        int P = rankList.size();
        int rowsPerProcess = (int) Math.ceil((double) N / P);

        long parStart = System.nanoTime();

        // Step 2.1: MPI_Bcast - Broadcast full Matrix B to all processes
        System.out.println("\n[MPI MATRIX MULT] 1. MPI_Bcast: Broadcasting Matrix B to " + P + " processes...");
        for (int rank = 0; rank < P; rank++) {
            NodeInfo node = rankList.get(rank);
            try {
                if (node.getNodeId() == this.nodeId) {
                    mpiBcastMatrixB(B);
                } else {
                    Registry registry = LocateRegistry.getRegistry(node.getHost(), node.getPort());
                    MPICollectiveService service = (MPICollectiveService) registry.lookup("MPICollectiveService");
                    service.mpiBcastMatrixB(B);
                }
            } catch (Exception e) {
                System.err.println("Failed to broadcast Matrix B to Rank " + rank + ": " + e.getMessage());
            }
        }

        // Step 2.2: MPI_Scatter - Scatter rows of Matrix A
        System.out.println("[MPI MATRIX MULT] 2. MPI_Scatter: Distributing rows of Matrix A among " + P + " processes...");
        for (int rank = 0; rank < P; rank++) {
            int startRow = rank * rowsPerProcess;
            int endRow = Math.min(startRow + rowsPerProcess, N);
            int chunkRowCount = Math.max(0, endRow - startRow);
            int[][] sliceA = new int[chunkRowCount][N];
            for (int r = 0; r < chunkRowCount; r++) {
                System.arraycopy(A[startRow + r], 0, sliceA[r], 0, N);
            }

            NodeInfo node = rankList.get(rank);
            try {
                if (node.getNodeId() == this.nodeId) {
                    mpiScatterRowsA(rank, sliceA);
                } else {
                    Registry registry = LocateRegistry.getRegistry(node.getHost(), node.getPort());
                    MPICollectiveService service = (MPICollectiveService) registry.lookup("MPICollectiveService");
                    service.mpiScatterRowsA(rank, sliceA);
                }
            } catch (Exception e) {
                System.err.println("Failed to scatter rows of A to Rank " + rank + ": " + e.getMessage());
            }
        }

        // Step 2.3 & 2.4: Parallel Compute & MPI_Gather
        System.out.println("[MPI MATRIX MULT] 3 & 4. Parallel Computing & MPI_Gather: Collecting rows into Matrix C...");
        int[][] C_par = new int[N][N];
        List<CompletableFuture<int[][]>> futures = new ArrayList<>();

        for (int rank = 0; rank < P; rank++) {
            final int r = rank;
            NodeInfo node = rankList.get(rank);
            futures.add(CompletableFuture.supplyAsync(() -> {
                try {
                    if (node.getNodeId() == this.nodeId) {
                        return mpiComputeAndGatherRowsC(r);
                    } else {
                        Registry registry = LocateRegistry.getRegistry(node.getHost(), node.getPort());
                        MPICollectiveService service = (MPICollectiveService) registry.lookup("MPICollectiveService");
                        return service.mpiComputeAndGatherRowsC(r);
                    }
                } catch (Exception e) {
                    System.err.println("Compute/Gather failed on Rank " + r + ": " + e.getMessage());
                    return new int[0][0];
                }
            }, pool));
        }

        int currentRow = 0;
        for (int rank = 0; rank < P; rank++) {
            try {
                int[][] partialC = futures.get(rank).join();
                for (int r = 0; r < partialC.length && currentRow < N; r++) {
                    System.arraycopy(partialC[r], 0, C_par[currentRow++], 0, N);
                }
            } catch (Exception e) {
                System.err.println("Failed assembling partial C from rank " + rank + ": " + e.getMessage());
            }
        }

        long parEnd = System.nanoTime();
        double parDurationMs = (parEnd - parStart) / 1_000_000.0;

        // 3. CORRECTNESS VERIFICATION
        boolean correct = true;
        for (int i = 0; i < N && correct; i++) {
            for (int j = 0; j < N; j++) {
                if (C_seq[i][j] != C_par[i][j]) {
                    correct = false;
                    break;
                }
            }
        }

        double speedup = seqDurationMs > 0 && parDurationMs > 0 ? (seqDurationMs / parDurationMs) : 1.0;
        double efficiency = speedup / P;

        StringBuilder sb = new StringBuilder();
        sb.append("=================================================\n");
        sb.append("      PARALLEL MATRIX MULTIPLICATION (MPI)\n");
        sb.append("=================================================\n");
        sb.append(String.format("Matrix Dimension (N x N)    : %d x %d\n", N, N));
        sb.append(String.format("Total Processes (Communicator): %d\n", P));
        sb.append(String.format("Sequential Execution Time   : %.3f ms\n", seqDurationMs));
        sb.append(String.format("Parallel MPI Execution Time : %.3f ms\n", parDurationMs));
        sb.append(String.format("Calculated Speedup (S)      : %.2fx\n", speedup));
        sb.append(String.format("Parallel Efficiency (E)     : %.2f%%\n", efficiency * 100));
        sb.append(String.format("Correctness Verification    : %s\n", correct ? "PASSED (Results match sequentially)" : "FAILED"));
        sb.append("=================================================");

        String report = sb.toString();
        System.out.println("\n" + report);
        return report;
    }
}