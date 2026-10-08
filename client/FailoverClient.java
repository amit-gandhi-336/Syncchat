package client;

import common.ChatService;
import common.NodeService;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class FailoverClient {

    public static ChatService connect(
            String node1IP,
            String node2IP,
            String node3IP)
            throws Exception {

        String[] hosts = {
                node1IP,
                node2IP,
                node3IP
        };

        int[] ports = {
                2001,
                2002,
                2003
        };

        Exception lastException = null;

        for (int i = 0; i < hosts.length; i++) {

            try {

                Registry registry =
                        LocateRegistry.getRegistry(
                                hosts[i],
                                ports[i]
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

        for (int i = 0; i < hosts.length; i++) {

            try {

                Registry registry =
                        LocateRegistry.getRegistry(
                                hosts[i],
                                ports[i]
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
                        "Connected to Node "
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
                "No SyncChat node is available."
        );
    }
}