package server;

import common.NodeInfo;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class ClockServer {

    public static void main(String[] args) {

        try {

            /*
             * Arguments:
             *
             * args[0] = Node ID
             * args[1] = Port
             * args[2] = Clock Offset
             * args[3] = Primary
             */

            if (args.length < 5) {

                System.out.println(
                        "Usage:"
                );

                System.out.println(
                        "java server.ClockServer " +
                        "<nodeId> <port> " +
                        "<offset> <primary>"
                );

                return;
            }

            int nodeId =
                    Integer.parseInt(args[0]);

            int port =
                    Integer.parseInt(args[1]);

            long offset =
                    Long.parseLong(args[2]);

            boolean primary =
                    Boolean.parseBoolean(
                            args[3]
                    );

             ConsistencyMode consistencyMode =
                ConsistencyMode.valueOf(
                        args[4].toUpperCase()
                );

            String advertisedHost = System.getProperty(
                    "java.rmi.server.hostname",
                    "localhost"
            );
            int exportPort = Integer.getInteger(
                    "syncchat.rmi.exportPort",
                    port + 1000
            );
            String clusterConfig = System.getProperty(
                    "syncchat.nodes",
                    "1@localhost@2001,2@localhost@2002,3@localhost@2003"
            );

            /*
             * Create RMI registry
             */
            Registry registry =
                    LocateRegistry.createRegistry(
                            port
                    );

            /*
             * Create server
             */
            ChatServer server =
        new ChatServer(
                nodeId,
                offset,
                primary,
                consistencyMode,
                exportPort
        );

            /*
             * Register services
             */
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
            /*
             * Add all cluster nodes
             */
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

            /*
             * Start heartbeat manager
             */
            Thread heartbeat =
                    new Thread(
                            new HeartbeatManager(
                                    nodeId,
                                    server.getNodes(),
                                    server
                            )
                    );

            heartbeat.setDaemon(true);
            heartbeat.start();

            /*
             * Server information
             */
            System.out.println();
            System.out.println(
                    "================================="
            );

            System.out.println(
                    "       SYNCCHAT NODE"
            );

            System.out.println(
                    "================================="
            );

            System.out.println(
                    "Node ID  : " + nodeId
            );

            System.out.println(
                    "Port     : " + port
            );
            System.out.println("RMI object port: " + exportPort);
            System.out.println("Advertised host: " + advertisedHost);

            System.out.println(
                    "Primary  : " + primary
            );

            System.out.println(
                    "Clock    : " +
                    server.getTime()
            );

            System.out.println(
                    "Heartbeat: ACTIVE"
            );

            System.out.println(
                "Consistency: " +
                consistencyMode
        );
        
        
            System.out.println(
                    "================================="
            );

        } catch (Exception e) {

            e.printStackTrace();
        }
    }
}