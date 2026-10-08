package client;

import common.ChatService;
import common.NodeService;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class FailoverClient {

    private static final String DEFAULT_HOST =
            "localhost";

    private static final int[] DEFAULT_PORTS = {
            2001,
            2002,
            2003
    };

    public static ChatService connect(
            String host,
            int preferredPort)
            throws Exception {

        int[] ports =
                buildPortList(preferredPort);

        Exception lastException = null;

        for (int port : ports) {

            try {

                Registry registry =
                        LocateRegistry.getRegistry(
                                host,
                                port
                        );

                NodeService nodeService =
                        (NodeService)
                                registry.lookup(
                                        "NodeService"
                                );

                if (!nodeService.isAlive()) {
                    continue;
                }

                if (!nodeService.isPrimary()) {
                    continue;
                }

                ChatService chatService =
                        (ChatService)
                                registry.lookup(
                                        "ChatService"
                                );

                System.out.println(
                        "Connected to PRIMARY Node "
                                + nodeService.getNodeId()
                );

                return chatService;

            } catch (Exception e) {

                lastException = e;
            }
        }

        for (int port : ports) {

            try {

                Registry registry =
                        LocateRegistry.getRegistry(
                                host,
                                port
                        );

                NodeService nodeService =
                        (NodeService)
                                registry.lookup(
                                        "NodeService"
                                );

                if (!nodeService.isAlive()) {
                    continue;
                }

                ChatService chatService =
                        (ChatService)
                                registry.lookup(
                                        "ChatService"
                                );

                System.out.println(
                        "Connected to available Node "
                                + nodeService.getNodeId()
                );

                return chatService;

            } catch (Exception e) {

                lastException = e;
            }
        }

        if (lastException != null) {
            throw lastException;
        }

        throw new Exception(
                "No SyncChat server is available."
        );
    }

    public static ChatService connect()
            throws Exception {

        return connect(
                DEFAULT_HOST,
                DEFAULT_PORTS[0]
        );
    }

    private static int[] buildPortList(
            int preferredPort) {

        int[] result =
                new int[DEFAULT_PORTS.length + 1];

        result[0] = preferredPort;

        int index = 1;

        for (int port : DEFAULT_PORTS) {

            if (port == preferredPort) {
                continue;
            }

            result[index++] = port;
        }

        return result;
    }
}