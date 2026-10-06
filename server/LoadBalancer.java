package server;

import common.ChatService;
import common.Message;
import common.NodeInfo;
import common.NodeService;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * RMI gateway for client requests. Writes and analytics go to the current
 * primary; safe reads use round-robin selection among reachable nodes.
 */
public class LoadBalancer extends UnicastRemoteObject implements ChatService {

    private static final long serialVersionUID = 1L;
    private static final long HEALTH_CHECK_SECONDS = 2;

    private final Map<Integer, NodeState> nodes = new TreeMap<>();
    private final AtomicInteger readCursor = new AtomicInteger();
    private final ScheduledExecutorService healthMonitor =
            Executors.newSingleThreadScheduledExecutor(runnable -> {
                Thread thread = new Thread(runnable, "syncchat-lb-health-check");
                thread.setDaemon(true);
                return thread;
            });

    /**
     * @param exportPort fixed TCP port for this gateway's RMI object
     * @param nodeConfig comma-separated id@host@registryPort entries
     */
    public LoadBalancer(int exportPort, String nodeConfig)
            throws RemoteException {
        super(exportPort);
        parseNodes(nodeConfig);
    }

    private void parseNodes(String nodeConfig) {
        if (nodeConfig == null || nodeConfig.trim().isEmpty()) {
            throw new IllegalArgumentException("At least one backend node is required.");
        }

        for (String entry : nodeConfig.split(",")) {
            String[] fields = entry.trim().split("@", 3);
            if (fields.length != 3) {
                throw new IllegalArgumentException(
                        "Invalid node entry '" + entry
                                + "'; expected id@host@registryPort"
                );
            }
            int nodeId = Integer.parseInt(fields[0]);
            NodeInfo node = new NodeInfo(
                    nodeId,
                    fields[1],
                    Integer.parseInt(fields[2])
            );
            if (nodes.putIfAbsent(nodeId, new NodeState(node)) != null) {
                throw new IllegalArgumentException("Duplicate node ID: " + nodeId);
            }
        }
    }

    /** Start repeated probes so failed nodes are excluded and can rejoin. */
    public void startHealthMonitoring() {
        refreshHealth();
        healthMonitor.scheduleWithFixedDelay(
                this::refreshHealth,
                HEALTH_CHECK_SECONDS,
                HEALTH_CHECK_SECONDS,
                TimeUnit.SECONDS
        );
    }

    /** Probe through the existing NodeService API; successful RMI is the health signal. */
    private synchronized void refreshHealth() {
        for (NodeState state : nodes.values()) {
            try {
                Registry registry = LocateRegistry.getRegistry(
                        state.node.getHost(), state.node.getPort()
                );
                NodeService nodeService = (NodeService)
                        registry.lookup("NodeService");

                if (!nodeService.isAlive()) {
                    state.setUnavailable();
                    continue;
                }

                ChatService chatService = (ChatService)
                        registry.lookup("ChatService");
                boolean primary = nodeService.isPrimary();

                state.chatService = chatService;
                state.primary = primary;
                state.healthy = true;
            } catch (Exception e) {
                state.setUnavailable();
                System.err.println(
                        "Load balancer: Node " + state.node.getNodeId()
                                + " is unavailable: " + e.getMessage()
                );
            }
        }
    }

    private NodeState currentPrimary() throws RemoteException {
        // Refresh election state on demand instead of waiting for the next poll.
        refreshHealth();
        NodeState primary = null;
        for (NodeState state : nodes.values()) {
            if (state.healthy && state.primary) {
                if (primary != null) {
                    throw new RemoteException(
                            "Multiple healthy nodes report PRIMARY; refusing to route a write."
                    );
                }
                primary = state;
            }
        }
        if (primary == null) {
            throw new RemoteException(
                    "No healthy PRIMARY is currently available. Try again after election completes."
            );
        }
        return primary;
    }

    private List<NodeState> healthyNodes() {
        List<NodeState> healthy = new ArrayList<>();
        for (NodeState state : nodes.values()) {
            if (state.healthy && state.chatService != null) {
                healthy.add(state);
            }
        }
        return healthy;
    }

    private <T> T readFromHealthyNode(ReadOperation<T> operation)
            throws RemoteException {
        List<NodeState> healthy = healthyNodes();
        if (healthy.isEmpty()) {
            throw new RemoteException("No healthy SyncChat nodes are available.");
        }

        int start = Math.floorMod(readCursor.getAndIncrement(), healthy.size());
        RemoteException lastFailure = null;
        for (int offset = 0; offset < healthy.size(); offset++) {
            NodeState state = healthy.get((start + offset) % healthy.size());
            try {
            System.out.println(
                "Load balancer: routing read to Node "
                    + state.node.getNodeId()
            );
                return operation.call(state.chatService);
            } catch (RemoteException e) {
                state.setUnavailable();
                lastFailure = e;
                System.err.println(
                        "Load balancer: read from Node " + state.node.getNodeId()
                                + " failed; trying another healthy node."
                );
            }
        }
        throw new RemoteException("All read targets failed.", lastFailure);
    }

    private ChatService primaryService() throws RemoteException {
        NodeState primary = currentPrimary();
        if (primary.chatService == null) {
            throw new RemoteException("The current PRIMARY has no available ChatService.");
        }
        System.out.println(
            "Load balancer: routing primary-only request to Node "
                + primary.node.getNodeId()
        );
        return primary.chatService;
    }

    @Override
    public boolean registerUser(String username) throws RemoteException {
        // Do not retry writes: a lost response may mean the primary committed it.
        return primaryService().registerUser(username);
    }

    @Override
    public void sendMessage(String sender, String receiver, String content)
            throws RemoteException {
        // Writes always go to the discovered primary, never a random backup.
        primaryService().sendMessage(sender, receiver, content);
    }

    @Override
    public List<Message> getMessages(String username) throws RemoteException {
        return readFromHealthyNode(service -> service.getMessages(username));
    }

    @Override
    public Map<String, Long> getMessageCountsBySender() throws RemoteException {
        // The existing MapReduce coordinator requires the primary's snapshot.
        return primaryService().getMessageCountsBySender();
    }

    @Override
    public String getServerStatus() throws RemoteException {
        return readFromHealthyNode(service -> service.getServerStatus());
    }

    private interface ReadOperation<T> {
        T call(ChatService service) throws RemoteException;
    }

    private static final class NodeState {
        private final NodeInfo node;
        private volatile ChatService chatService;
        private volatile boolean healthy;
        private volatile boolean primary;

        private NodeState(NodeInfo node) {
            this.node = node;
        }

        private void setUnavailable() {
            healthy = false;
            primary = false;
            chatService = null;
        }
    }

    /**
     * Usage: java -Djava.rmi.server.hostname=<gatewayHost> -cp out
     * server.LoadBalancer <registryPort> <exportPort> <id@host@port,...>
     */
    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.println(
                    "Usage: java -Djava.rmi.server.hostname=<gatewayHost> "
                            + "server.LoadBalancer <registryPort> <exportPort> "
                            + "<id@host@registryPort,...>"
            );
            return;
        }

        try {
            int registryPort = Integer.parseInt(args[0]);
            int exportPort = Integer.parseInt(args[1]);
            Registry registry = LocateRegistry.createRegistry(registryPort);
            LoadBalancer loadBalancer = new LoadBalancer(exportPort, args[2]);
            registry.rebind("ChatService", loadBalancer);
            loadBalancer.startHealthMonitoring();

            System.out.println("SyncChat load balancer is running.");
            System.out.println("Registry port: " + registryPort);
            System.out.println("RMI object port: " + exportPort);
            System.out.println("Configured nodes: " + args[2]);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
