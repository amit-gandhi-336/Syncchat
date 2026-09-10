package server;

import common.NodeInfo;
import common.NodeService;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Map;

public class BullyElection {

    private final int nodeId;
    private final Map<Integer, NodeInfo> nodes;

    public BullyElection(
            int nodeId,
            Map<Integer, NodeInfo> nodes) {

        this.nodeId = nodeId;
        this.nodes = nodes;
    }

    public void startElection() {

        System.out.println();
        System.out.println("=================================");
        System.out.println("       BULLY ELECTION");
        System.out.println("       Initiated by Node " + nodeId);
        System.out.println("=================================");

        boolean higherNodeAlive = false;

        for (NodeInfo node : nodes.values()) {

            if (node.getNodeId() <= nodeId) {
                continue;
            }

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

                if (service.isAlive()) {

                    higherNodeAlive = true;

                    System.out.println(
                            "Node " + nodeId +
                            " contacted Node " +
                            node.getNodeId()
                    );

                    service.startBullyElection();
                }

            } catch (Exception e) {

                System.out.println(
                        "Node " +
                        node.getNodeId() +
                        " is unavailable."
                );
            }
        }

        if (!higherNodeAlive) {
            becomeCoordinator();
        }
    }

    private void becomeCoordinator() {

        System.out.println();
        System.out.println(
                "*********************************"
        );

        System.out.println(
                "Node " + nodeId +
                " WINS THE BULLY ELECTION"
        );

        System.out.println(
                "Node " + nodeId +
                " is now PRIMARY"
        );

        System.out.println(
                "*********************************"
        );

        for (NodeInfo node : nodes.values()) {

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

                service.announcePrimary(nodeId);

            } catch (Exception e) {

                // Offline node
            }
        }
    }
}