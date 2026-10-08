package server;

import common.NodeInfo;
import common.NodeService;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Map;

public class HeartbeatManager
        implements Runnable {

    private final int nodeId;

    private final Map<Integer, NodeInfo> nodes;

    private final ChatServer server;

    private volatile boolean electionInProgress =
            false;

    public HeartbeatManager(
            int nodeId,
            Map<Integer, NodeInfo> nodes,
            ChatServer server) {

        this.nodeId = nodeId;
        this.nodes = nodes;
        this.server = server;
    }

    @Override
    public void run() {

        while (true) {

            try {

                checkPrimary();

                Thread.sleep(3000);

            } catch (InterruptedException e) {

                Thread.currentThread()
                        .interrupt();

                return;
            }
        }
    }

    private void checkPrimary() {

        int primaryId =
                server.getCurrentPrimary();

        if (primaryId == nodeId) {

            electionInProgress = false;

            return;
        }

        NodeInfo primary =
                nodes.get(primaryId);

        if (primary == null) {
            return;
        }

        try {

            Registry registry =
                    LocateRegistry.getRegistry(
                            primary.getHost(),
                            primary.getPort()
                    );

            NodeService service =
                    (NodeService)
                            registry.lookup(
                                    "NodeService"
                            );

            if (service.isAlive()) {

                electionInProgress = false;
            }

        } catch (Exception e) {

            if (electionInProgress) {
                return;
            }

            electionInProgress = true;

            System.out.println();
            System.out.println(
                    "================================="
            );

            System.out.println(
                    "PRIMARY FAILURE DETECTED"
            );

            System.out.println(
                    "Node " + primaryId +
                    " is not responding."
            );

            System.out.println(
                    "Starting Bully Election..."
            );

            System.out.println(
                    "================================="
            );

            try {

                server.startBullyElection();

            } catch (Exception ex) {

                electionInProgress = false;

                ex.printStackTrace();
            }
        }
    }
}