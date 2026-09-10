package server;

import common.*;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import java.util.concurrent.*;

public class ChatServer
        extends UnicastRemoteObject
        implements ChatService,
                   ClockService,
                   NodeService {

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

    /*
     * Constructor
     */
    public ChatServer(
            int nodeId,
            long offset,
            boolean primary)
            throws RemoteException {

        super();

        this.nodeId = nodeId;
        this.primary = primary;

        this.currentPrimary =
                primary ? nodeId : 1;

        clock =
                new LogicalClock(offset);
    }

    // =====================================================
    // CHAT METHODS
    // =====================================================

    @Override
    public boolean registerUser(
            String username)
            throws RemoteException {

        try {

            return pool.submit(() -> {

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
                        Thread.currentThread()
                                .getName()
                                + " registered "
                                + username
                );

                return true;

            }).get();

        } catch (Exception e) {

            throw new RemoteException(
                    e.getMessage()
            );
        }
    }

    @Override
    public void sendMessage(
            String sender,
            String receiver,
            String content)
            throws RemoteException {

        pool.submit(() -> {

            if (!users.contains(receiver)) {
                return;
            }

            messages.get(receiver).add(
                    new Message(
                            sender,
                            receiver,
                            content
                    )
            );

            System.out.println(
                    Thread.currentThread()
                            .getName()
                            + " processed: "
                            + sender
                            + " -> "
                            + receiver
            );
        });
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
    // RING ELECTION
    // =====================================================

    @Override
    public void startRingElection(
            int candidateId)
            throws RemoteException {

        RingElection election =
                new RingElection(
                        nodeId,
                        nodes
                );

        election.forwardElection(
                candidateId
        );
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
}