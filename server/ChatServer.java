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
                   MapReduceService {


    private final Set<String> users =
            ConcurrentHashMap.newKeySet();

    private final Map<String, List<Message>> messages =
            new ConcurrentHashMap<>();

    private final ExecutorService pool =
            Executors.newFixedThreadPool(5);

    private final LogicalClock clock;

    /*
     * Distributed node information
     */
    private final int nodeId;

    private volatile boolean primary;

    private volatile int currentPrimary;

    private final Map<Integer, NodeInfo> nodes =
            new ConcurrentHashMap<>();

    private final ConsistencyMode consistencyMode;
        private static final int MAP_BATCH_SIZE = 100;
    /*
     * Constructor
     */
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

        /*
         * Don't replicate to ourselves.
         */
        if (node.getNodeId() == nodeId) {
            continue;
        }

        try {

            Registry registry =
                    LocateRegistry.getRegistry(
                            node.getHost(),
                            node.getPort()
                    );

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

            System.out.println(
                    "Replication failed for Node "
                            + node.getNodeId()
            );

            if (consistencyMode ==
                    ConsistencyMode.STRONG) {

                throw new RuntimeException(
                        "Strong consistency failed. " +
                        "Backup Node " +
                        node.getNodeId() +
                        " did not acknowledge."
                );
            }
        }
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

    /*
     * Replicate user to backups.
     */
    if (consistencyMode ==
            ConsistencyMode.STRONG) {

        replicateUserToBackups(username);

    } else {

        pool.submit(() -> {

            replicateUserToBackups(username);

        });
    }

    return true;
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

            System.out.println(
                    "User replication failed for Node "
                            + node.getNodeId()
            );

            if (consistencyMode ==
                    ConsistencyMode.STRONG) {

                throw new RuntimeException(
                        "Strong consistency failed."
                );
            }
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

    /*
     * Store message locally on primary.
     */
    messages.get(receiver).add(message);

    System.out.println(
            "Node " + nodeId +
            " stored message locally."
    );

    /*
     * STRONG CONSISTENCY
     */
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
    }

    /*
     * EVENTUAL CONSISTENCY
     */
    else {

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

        return new ArrayList<>(list);
    }

        /** Map phase: count messages by sender within one batch. */
        @Override
        public Map<String, Long> mapMessageCounts(List<Message> batch)
                        throws RemoteException {

                Map<String, Long> partialCounts = new HashMap<>();
                for (Message message : batch) {
                        partialCounts.merge(message.getSender(), 1L, Long::sum);
                }
                return partialCounts;
        }

        /**
         * Coordinator: snapshot the primary's data, distribute disjoint map
         * batches to cluster nodes, then reduce their partial counts.
         */
        @Override
        public Map<String, Long> getMessageCountsBySender()
                        throws RemoteException {

                if (!primary) {
                        throw new RemoteException(
                                        "Run chat analytics through the PRIMARY node."
                        );
                }

                List<Message> snapshot = new ArrayList<>();
                for (List<Message> inbox : messages.values()) {
                        synchronized (inbox) {
                                snapshot.addAll(inbox);
                        }
                }

                List<NodeInfo> workers = new ArrayList<>(nodes.values());
                workers.sort(Comparator.comparingInt(NodeInfo::getNodeId));
                if (workers.isEmpty()) {
                        workers.add(new NodeInfo(nodeId, "localhost", 0));
                }

                List<CompletableFuture<Map<String, Long>>> mapResults =
                                new ArrayList<>();
                int taskNumber = 0;
                for (int start = 0; start < snapshot.size(); start += MAP_BATCH_SIZE) {
                        int end = Math.min(start + MAP_BATCH_SIZE, snapshot.size());
                        List<Message> batch = new ArrayList<>(snapshot.subList(start, end));
                        NodeInfo worker = workers.get(taskNumber++ % workers.size());

                        mapResults.add(CompletableFuture.supplyAsync(() -> {
                                try {
                                        if (worker.getNodeId() == nodeId) {
                                                return mapMessageCounts(batch);
                                        }

                                        Registry registry = LocateRegistry.getRegistry(
                                                        worker.getHost(), worker.getPort()
                                        );
                                        MapReduceService service = (MapReduceService)
                                                        registry.lookup("MapReduceService");
                                        return service.mapMessageCounts(batch);
                                } catch (Exception e) {
                                        System.err.println(
                                                        "Map task failed on Node " + worker.getNodeId()
                                                                        + "; mapping batch locally: " + e.getMessage()
                                        );
                                        try {
                                                return mapMessageCounts(batch);
                                        } catch (RemoteException impossible) {
                                                throw new CompletionException(impossible);
                                        }
                                }
                        }, pool));
                }

                // Reduce phase: sum each worker's partial count for each sender.
                Map<String, Long> totals = new TreeMap<>();
                for (CompletableFuture<Map<String, Long>> result : mapResults) {
                        result.join().forEach(
                                        (sender, count) -> totals.merge(sender, count, Long::sum)
                        );
                }
                return totals;
        }

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
public void setPrimary(boolean primary)
        throws RemoteException {

    this.primary = primary;
}

    // =====================================================
    // BULLY ELECTION
    // =====================================================

    @Override
    public void startBullyElection()
            throws RemoteException {

        BullyElection election =
                new BullyElection(
                        nodeId,
                        nodes
                );

        election.startElection();
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
                    "================================="
            );

        } else {

            primary = false;

            System.out.println(
                    "Node " + nodeId +
                    " is BACKUP"
            );
        }
    }

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

    messages.get(receiver).add(
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
}