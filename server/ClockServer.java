package server;

import common.*;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class ClockServer {

    public static void main(String[] args) {

        if (args.length != 5 && args.length != 8) {

            System.out.println(
                    "Usage:"
            );

            System.out.println(
                    "java server.ClockServer " +
                    "<nodeId> " +
                    "<registryPort> " +
                    "<offset> " +
                    "<primary> " +
                    "<STRONG|EVENTUAL> " +
                    "[<node1IP> <node2IP> <node3IP>]"
            );

            return;
        }

        try {

            int nodeId =
                    Integer.parseInt(
                            args[0]
                    );

            int registryPort =
                    Integer.parseInt(
                            args[1]
                    );

            long offset =
                    Long.parseLong(
                            args[2]
                    );

            boolean primary =
                    Boolean.parseBoolean(
                            args[3]
                    );

            ConsistencyMode consistencyMode =
                    ConsistencyMode.valueOf(
                            args[4].toUpperCase()
                    );

            if (nodeId < 1 || nodeId > 3) {
                throw new IllegalArgumentException("Node ID must be 1, 2, or 3.");
            }

            String[] nodeHosts = args.length == 8
                    ? new String[] {args[5], args[6], args[7]}
                    : null;
            String localIP = System.getProperty("java.rmi.server.hostname");
            if (localIP == null || localIP.isBlank()) {
                localIP = nodeHosts == null ? "localhost" : nodeHosts[nodeId - 1];
                System.setProperty("java.rmi.server.hostname", localIP);
            }

            int rmiObjectPort = Integer.getInteger(
                    "syncchat.rmi.exportPort",
                    registryPort + 1000
            );

            String clusterConfig = System.getProperty("syncchat.nodes");
            if (clusterConfig == null || clusterConfig.isBlank()) {
                clusterConfig = nodeHosts == null
                        ? "1@localhost@2001,2@localhost@2002,3@localhost@2003"
                        : "1@" + nodeHosts[0] + "@2001,2@" + nodeHosts[1]
                                + "@2002,3@" + nodeHosts[2] + "@2003";
            }

                        try {
                                ProcessLog.redirect("node" + nodeId);
                        } catch (java.io.IOException e) {
                                System.err.println("Could not enable dashboard log capture: " + e.getMessage());
                        }

            ChatServer server =
                    new ChatServer(
                            nodeId,
                            offset,
                            primary,
                            consistencyMode,
                            rmiObjectPort
                    );

            for (String entry : clusterConfig.split(",")) {
                String[] fields = entry.trim().split("@", 3);
                if (fields.length != 3) {
                    throw new IllegalArgumentException(
                            "Invalid node entry '" + entry
                                    + "'; expected id@host@registryPort"
                    );
                }
                server.addNode(new NodeInfo(
                        Integer.parseInt(fields[0]),
                        fields[1],
                        Integer.parseInt(fields[2])
                ));
            }

            Registry registry =
                    LocateRegistry.createRegistry(
                            registryPort
                    );

            registry.rebind(
                    "ChatService",
                    server
            );

            registry.rebind(
                    "ClockService",
                    server
            );

            registry.rebind(
                    "NodeService",
                    server
            );

            registry.rebind(
                    "ReplicationService",
                    server
            );

            registry.rebind(
                    "MapReduceService",
                    server
            );

            System.out.println();
            System.out.println(
                    "======================================"
            );

            System.out.println(
                    "       SYNCCHAT NODE STARTED"
            );

            System.out.println(
                    "======================================"
            );

            System.out.println(
                    "Node ID       : " + nodeId
            );

            System.out.println(
                    "Advertised host: " + localIP
            );

            System.out.println(
                    "Registry Port : " + registryPort
            );

            System.out.println(
                    "RMI object port: " + rmiObjectPort
            );

            System.out.println(
                    "Primary       : " + primary
            );

            System.out.println(
                    "Consistency   : " + consistencyMode
            );

            System.out.println(
                    "======================================"
            );

            Thread heartbeatThread = new Thread(
                    new HeartbeatManager(nodeId, server.getNodes(), server),
                    "HeartbeatManager"
            );

            heartbeatThread.setDaemon(true);

            heartbeatThread.start();

            System.out.println(
                    "Heartbeat manager started."
            );

            System.out.println(
                    "Node is ready."
            );

        } catch (Exception e) {

            e.printStackTrace();
        }
    }
}