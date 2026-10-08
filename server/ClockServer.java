package server;

import common.*;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class ClockServer {

    public static void main(String[] args) {

        if (args.length < 8) {

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
                    "<node1IP> " +
                    "<node2IP> " +
                    "<node3IP>"
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

            String node1IP =
                    args[5];

            String node2IP =
                    args[6];

            String node3IP =
                    args[7];

            String localIP;

            if (nodeId == 1) {

                localIP = node1IP;

            } else if (nodeId == 2) {

                localIP = node2IP;

            } else {

                localIP = node3IP;
            }

            System.setProperty(
                    "java.rmi.server.hostname",
                    localIP
            );

            int rmiObjectPort =
                    3000 + nodeId;

            ChatServer server =
                    new ChatServer(
                            nodeId,
                            offset,
                            primary,
                            consistencyMode,
                            rmiObjectPort
                    );

            server.addNode(
                    new NodeInfo(
                            1,
                            node1IP,
                            2001
                    )
            );

            server.addNode(
                    new NodeInfo(
                            2,
                            node2IP,
                            2002
                    )
            );

            server.addNode(
                    new NodeInfo(
                            3,
                            node3IP,
                            2003
                    )
            );

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
                    "Tailscale IP  : " + localIP
            );

            System.out.println(
                    "Registry Port : " + registryPort
            );

            System.out.println(
                    "RMI Port      : " + rmiObjectPort
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

            HeartbeatManager heartbeat =
                    new HeartbeatManager(
                            nodeId,
                            server.getNodes(),
                            server
                    );

            Thread heartbeatThread =
                    new Thread(
                            heartbeat,
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