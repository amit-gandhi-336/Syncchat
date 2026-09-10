package server;

import common.NodeInfo;
import common.NodeService;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class RingElection {

    private final int nodeId;
    private final Map<Integer, NodeInfo> nodes;

    public RingElection(
            int nodeId,
            Map<Integer, NodeInfo> nodes) {

        this.nodeId = nodeId;
        this.nodes = nodes;
    }

    public void startElection() {

        System.out.println();
        System.out.println("=================================");
        System.out.println("       RING ELECTION");
        System.out.println("       Initiated by Node " + nodeId);
        System.out.println("=================================");

        forwardElection(nodeId);
    }

    public void forwardElection(int candidateId) {

        List<Integer> ids =
                new ArrayList<>(nodes.keySet());

        Collections.sort(ids);

        int currentIndex =
                ids.indexOf(nodeId);

        if (currentIndex == -1) {
            return;
        }

        /*
         * Find next active node in the ring.
         */
        for (int i = 1; i < ids.size(); i++) {

            int nextIndex =
                    (currentIndex + i) % ids.size();

            int nextNodeId =
                    ids.get(nextIndex);

            if (!isAlive(nextNodeId)) {
                continue;
            }

            /*
             * If the message has returned to the
             * initiating node, election is complete.
             */
            if (nextNodeId == nodeId) {

                announceWinner(candidateId);
                return;
            }

            int highest =
                    Math.max(
                            candidateId,
                            nodeId
                    );

            System.out.println(
                    "Node " + nodeId +
                    " -> Node " + nextNodeId +
                    " | Candidate = " + highest
            );

            sendElection(
                    nextNodeId,
                    highest
            );

            return;
        }

        /*
         * Only this node is alive.
         */
        announceWinner(
                Math.max(candidateId, nodeId)
        );
    }

    private void sendElection(
            int targetId,
            int candidateId) {

        NodeInfo target =
                nodes.get(targetId);

        if (target == null) {
            return;
        }

        try {

            Registry registry =
                    LocateRegistry.getRegistry(
                            target.getHost(),
                            target.getPort()
                    );

            NodeService service =
                    (NodeService)
                            registry.lookup(
                                    "NodeService"
                            );

            service.startRingElection(
                    candidateId
            );

        } catch (Exception e) {

            System.out.println(
                    "Node " + targetId +
                    " is unavailable."
            );
        }
    }

    private boolean isAlive(int id) {

        if (id == nodeId) {
            return true;
        }

        NodeInfo node = nodes.get(id);

        if (node == null) {
            return false;
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

            return service.isAlive();

        } catch (Exception e) {

            return false;
        }
    }

    private void announceWinner(int winnerId) {

        System.out.println();
        System.out.println(
                "Node " + winnerId +
                " WINS THE RING ELECTION"
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

                service.announcePrimary(
                        winnerId
                );

            } catch (Exception e) {

                // Offline node
            }
        }
    }
}