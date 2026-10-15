package server;

import common.NodeInfo;
import common.NodeService;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Chang-Roberts Ring Election Algorithm.
 *
 * Nodes are arranged in a logical ring ordered by nodeId (ascending).
 * When an election starts, this node sends its own ID to its successor.
 * Each node forwards the message if the candidate ID is greater than its own;
 * if it equals the node's own ID the node has received its own message back
 * around the ring and declares itself the coordinator.
 */
public class RingElection {

    private final int nodeId;
    private final Map<Integer, NodeInfo> nodes;

    public RingElection(
            int nodeId,
            Map<Integer, NodeInfo> nodes) {

        this.nodeId = nodeId;
        this.nodes = nodes;
    }

    // ------------------------------------------------------------------
    // Public entry-point: initiate a new election from this node.
    // ------------------------------------------------------------------

    public void startElection() {

        System.out.println();
        System.out.println("=================================");
        System.out.println("       RING ELECTION");
        System.out.println("       Initiated by Node " + nodeId);
        System.out.println("=================================");

        // Send this node's own ID around the ring.
        forwardToSuccessor(nodeId);
    }

    // ------------------------------------------------------------------
    // Called when this node receives an election token from its
    // predecessor.  Implements the Chang-Roberts forwarding logic.
    // ------------------------------------------------------------------

    public void receiveElectionMessage(int candidateId) {

        System.out.println(
                "Node " + nodeId +
                " received election token with candidate " + candidateId
        );

        if (candidateId > nodeId) {

            // Candidate is larger — forward it.
            System.out.println(
                    "Node " + nodeId +
                    " forwarding candidate " + candidateId +
                    " (candidate > self)"
            );

            forwardToSuccessor(candidateId);

        } else if (candidateId == nodeId) {

            // The token has gone all the way around — this node wins.
            becomeCoordinator();

        } else {

            // candidateId < nodeId — discard (do not forward).
            System.out.println(
                    "Node " + nodeId +
                    " discarding candidate " + candidateId +
                    " (candidate < self)"
            );
        }
    }

    // ------------------------------------------------------------------
    // Send the token to the immediate successor in the ring.
    // If the successor is unreachable, try the next one, and so on.
    // ------------------------------------------------------------------

    private void forwardToSuccessor(int candidateId) {

        List<Integer> ring = buildRing();

        int myIndex = ring.indexOf(nodeId);

        if (myIndex < 0) {
            // Should not happen, but fall back to becoming coordinator.
            becomeCoordinator();
            return;
        }

        int size = ring.size();

        for (int offset = 1; offset < size; offset++) {

            int successorId = ring.get((myIndex + offset) % size);

            NodeInfo successor = nodes.get(successorId);

            if (successor == null) {
                continue;
            }

            try {

                Registry registry =
                        LocateRegistry.getRegistry(
                                successor.getHost(),
                                successor.getPort()
                        );

                NodeService service =
                        (NodeService)
                                registry.lookup("NodeService");

                if (!service.isAlive()) {
                    System.out.println(
                            "Node " + successorId +
                            " is not alive, trying next successor."
                    );
                    continue;
                }

                System.out.println(
                        "Node " + nodeId +
                        " forwarding candidate " + candidateId +
                        " to successor Node " + successorId
                );

                service.forwardElectionMessage(candidateId);

                return; // Successfully forwarded.

            } catch (Exception e) {

                System.out.println(
                        "Node " + successorId +
                        " is unreachable, trying next successor."
                );
            }
        }

        // All other nodes are unreachable — this node is the only survivor.
        System.out.println(
                "Node " + nodeId +
                " is the only reachable node. Winning by default."
        );

        becomeCoordinator();
    }

    // ------------------------------------------------------------------
    // Announce victory to every reachable node.
    // ------------------------------------------------------------------

    private void becomeCoordinator() {

        System.out.println();
        System.out.println("*********************************");
        System.out.println("Node " + nodeId + " WINS THE RING ELECTION");
        System.out.println("Node " + nodeId + " is now PRIMARY");
        System.out.println("*********************************");

        for (NodeInfo node : nodes.values()) {

            try {

                Registry registry =
                        LocateRegistry.getRegistry(
                                node.getHost(),
                                node.getPort()
                        );

                NodeService service =
                        (NodeService)
                                registry.lookup("NodeService");

                service.announcePrimary(nodeId);

            } catch (Exception e) {

                // Offline node — skip.
            }
        }
    }

    // ------------------------------------------------------------------
    // Build a stable, sorted ring of all known node IDs.
    // ------------------------------------------------------------------

    private List<Integer> buildRing() {

        List<Integer> ring = new ArrayList<>(nodes.keySet());

        ring.sort(Integer::compareTo);

        return ring;
    }
}
