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
                   ReplicationService {


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
    /*
     * Constructor
     */
    public ChatServer(
        int nodeId,
        long offset,
        boolean primary,
        ConsistencyMode consistencyMode)
        throws RemoteException {

    super();

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