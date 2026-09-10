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

            if (args.length < 4) {

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
                            primary
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

            /*
             * Add all cluster nodes
             */
            server.addNode(
                    new NodeInfo(
                            1,
                            "localhost",
                            2001
                    )
            );

            server.addNode(
                    new NodeInfo(
                            2,
                            "localhost",
                            2002
                    )
            );

            server.addNode(
                    new NodeInfo(
                            3,
                            "localhost",
                            2003
                    )
            );

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
                    "================================="
            );

        } catch (Exception e) {

            e.printStackTrace();
        }
    }
}