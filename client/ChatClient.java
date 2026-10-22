package client;

import common.ChatService;
import common.Message;

import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class ChatClient {

    private static String node1IP;
    private static String node2IP;
    private static String node3IP;
        private static String endpointHost;
        private static int endpointPort;
        private static boolean failoverMode;

    private static ChatService chatService;

    public static void main(String[] args) {

                if (args.length != 0 && args.length != 2 && args.length != 3) {

                        System.out.println("Usage:");
                        System.out.println("java -cp out client.ChatClient [<serverHost> <registryPort>]");
                        System.out.println("java -cp out client.ChatClient <node1IP> <node2IP> <node3IP>  (failover mode)");

            return;
        }

                if (args.length == 0) {
                        endpointHost = "localhost";
                        endpointPort = 2001;
                } else if (args.length == 2) {
                        endpointHost = args[0];
                        try {
                                endpointPort = Integer.parseInt(args[1]);
                                if (endpointPort < 1 || endpointPort > 65535) {
                                        throw new NumberFormatException();
                                }
                        } catch (NumberFormatException e) {
                                System.err.println("Registry port must be an integer between 1 and 65535.");
                                return;
                        }
                } else {
                        node1IP = args[0];
                        node2IP = args[1];
                        node3IP = args[2];
                        failoverMode = true;
                }

        try {

            Scanner scanner =
                    new Scanner(System.in);

            connect();

            System.out.println();
            System.out.println(
                    "================================="
            );

            System.out.println(
                    "          SYNCCHAT CLIENT"
            );

            System.out.println(
                    "================================="
            );

            System.out.print(
                    "Enter username: "
            );

            String username =
                    scanner.nextLine();

            boolean registered;

            try {

                registered =
                        chatService.registerUser(
                                username
                        );

            } catch (Exception e) {

                System.out.println(
                        "Primary unavailable."
                );

                reconnect();

                registered =
                        chatService.registerUser(
                                username
                        );
            }

            if (!registered) {

                System.out.println(
                        "Username already exists."
                );

                return;
            }

            System.out.println(
                    "Welcome, " + username + "!"
            );

            while (true) {

                System.out.println();
                System.out.println(
                        "1. Send Message"
                );
                System.out.println(
                        "2. Check Inbox"
                );
                System.out.println(
                        "3. Server Status"
                );
                System.out.println(
                        "4. Chat Activity Analytics (MapReduce)"
                );
                System.out.println(
                        "5. MPI Broadcast"
                );
                System.out.println(
                        "6. MPI Scatter & Gather"
                );
                System.out.println(
                        "7. Parallel Matrix Multiplication (MPI)"
                );
                System.out.println(
                        "8. Exit"
                );

                System.out.print(
                        "Choose option: "
                );

                String choice =
                        scanner.nextLine();

                switch (choice) {

                    case "1":

                        System.out.print(
                                "Receiver: "
                        );

                        String receiver =
                                scanner.nextLine();

                        System.out.print(
                                "Message: "
                        );

                        String message =
                                scanner.nextLine();

                        sendMessage(
                                username,
                                receiver,
                                message
                        );

                        break;

                    case "2":

                        getInbox(username);

                        break;

                    case "3":

                        getServerStatus();

                        break;

                    case "4":

                        getAnalytics();

                        break;

                    case "5":

                        System.out.print("Enter broadcast message: ");
                        String bcastMsg = scanner.nextLine();
                        runMPIBroadcast(bcastMsg);

                        break;

                    case "6":

                        runMPIScatterGather(scanner);

                        break;

                    case "7":

                        runMatrixMultiplication(scanner);

                        break;

                    case "8":

                        System.out.println(
                                "Goodbye!"
                        );

                        return;

                    default:

                        System.out.println(
                                "Invalid option."
                        );
                }
            }

        } catch (Exception e) {

            e.printStackTrace();
        }
    }

    private static void connect()
            throws Exception {
                if (failoverMode) {
                        chatService = FailoverClient.connect(node1IP, node2IP, node3IP);
                } else {
                        Registry registry = LocateRegistry.getRegistry(endpointHost, endpointPort);
                        chatService = (ChatService) registry.lookup("ChatService");
                        System.out.println("Connected to SyncChat at " + endpointHost + ":" + endpointPort);
                }
    }

    private static void reconnect() {

        int attempts = 0;

        while (attempts < 20) {

            try {

                Thread.sleep(2000);

                connect();

                System.out.println(
                        "Failover connection established."
                );

                return;

            } catch (Exception e) {

                attempts++;

                System.out.println(
                        "Waiting for new PRIMARY..."
                );
            }
        }

        throw new RuntimeException(
                "Unable to reconnect to SyncChat cluster."
        );
    }

    private static void sendMessage(
            String username,
            String receiver,
            String message) {

        try {

            chatService.sendMessage(
                    username,
                    receiver,
                    message
            );

            System.out.println(
                    "Message sent!"
            );

        } catch (Exception e) {

            System.out.println(
                    "Primary server failed."
            );

            System.out.println(
                    "Waiting for failover..."
            );

            try {

                reconnect();

                chatService.sendMessage(
                        username,
                        receiver,
                        message
                );

                System.out.println(
                        "Message sent after failover!"
                );

            } catch (Exception retryException) {

                System.out.println(
                        "Unable to send message."
                );
            }
        }
    }

    private static void getInbox(
            String username) {

        try {

            List<Message> messages =
                    chatService.getMessages(
                            username
                    );

            printInbox(messages);

        } catch (Exception e) {

            System.out.println(
                    "Primary server unavailable."
            );

            try {

                reconnect();

                List<Message> messages =
                        chatService.getMessages(
                                username
                        );

                printInbox(messages);

            } catch (Exception retryException) {

                System.out.println(
                        "Unable to retrieve inbox."
                );
            }
        }
    }

    private static void printInbox(
            List<Message> messages) {

        System.out.println();
        System.out.println(
                "========== INBOX =========="
        );

        if (messages.isEmpty()) {

            System.out.println(
                    "No messages."
            );

        } else {

            for (Message message :
                    messages) {

                System.out.println(
                        message
                );
            }
        }

        System.out.println(
                "============================"
        );
    }

    private static void getServerStatus() {

        try {

            System.out.println(
                    chatService.getServerStatus()
            );

        } catch (Exception e) {

            System.out.println(
                    "Primary server unavailable."
            );

            try {

                reconnect();

                System.out.println(
                        chatService.getServerStatus()
                );

            } catch (Exception retryException) {

                System.out.println(
                        "Unable to contact SyncChat cluster."
                );
            }
        }
    }

    private static void getAnalytics() {

        try {

            Map<String, Long> senderCounts =
                    chatService
                            .getMessageCountsBySender();

            printAnalytics(senderCounts);

        } catch (Exception e) {

            System.out.println(
                    "Primary server unavailable."
            );

            try {

                reconnect();

                Map<String, Long> senderCounts =
                        chatService
                                .getMessageCountsBySender();

                printAnalytics(senderCounts);

            } catch (Exception retryException) {

                System.out.println(
                        "Unable to run analytics."
                );
            }
        }
    }

    private static void printAnalytics(
            Map<String, Long> senderCounts) {

        System.out.println();
        System.out.println(
                "===== MESSAGES BY SENDER (MAPREDUCE) ====="
        );

        if (senderCounts.isEmpty()) {

            System.out.println(
                    "No messages to analyze."
            );

        } else {

            senderCounts.forEach(
                    (sender, count) ->
                            System.out.println(
                                    sender +
                                    ": " +
                                    count
                            )
            );
        }

        System.out.println(
                "==========================================="
        );
    }

    private static void runMPIBroadcast(String message) {
        try {
            chatService.mpiBroadcast(message);
            System.out.println("[MPI SUCCESS] Broadcast successfully dispatched by Root.");
        } catch (Exception e) {
            System.out.println("Broadcast failed: " + e.getMessage());
            try {
                reconnect();
                chatService.mpiBroadcast(message);
                System.out.println("[MPI SUCCESS] Broadcast successfully dispatched by Root (after reconnect).");
            } catch (Exception ex) {
                System.out.println("Unable to complete broadcast: " + ex.getMessage());
            }
        }
    }

    private static void runMPIScatterGather(Scanner scanner) {
        System.out.print("Enter comma-separated items to scatter (e.g. data1,data2,data3,data4,data5,data6): ");
        String input = scanner.nextLine().trim();
        List<String> items = new java.util.ArrayList<>();
        if (input.isEmpty()) {
            items.addAll(java.util.Arrays.asList("task-A", "task-B", "task-C", "task-D", "task-E", "task-F"));
            System.out.println("Using default items: " + items);
        } else {
            for (String s : input.split(",")) {
                if (!s.trim().isEmpty()) {
                    items.add(s.trim());
                }
            }
        }

        try {
            System.out.println("\n[MPI INITIATED] Scattering " + items.size() + " items and gathering processed results...");
            List<String> gathered = chatService.mpiScatterGather(items);
            System.out.println("\n[MPI GATHER FINAL RESULT]");
            for (int i = 0; i < gathered.size(); i++) {
                System.out.println("  Item " + (i + 1) + ": " + gathered.get(i));
            }
        } catch (Exception e) {
            System.out.println("Scatter-Gather failed: " + e.getMessage());
            try {
                reconnect();
                List<String> gathered = chatService.mpiScatterGather(items);
                System.out.println("\n[MPI GATHER FINAL RESULT]");
                for (int i = 0; i < gathered.size(); i++) {
                    System.out.println("  Item " + (i + 1) + ": " + gathered.get(i));
                }
            } catch (Exception ex) {
                System.out.println("Unable to complete Scatter-Gather: " + ex.getMessage());
            }
        }
    }

    private static void runMatrixMultiplication(Scanner scanner) {
        System.out.print("Enter matrix dimension N (e.g. 150 for 150x150, or press Enter for default 150): ");
        String input = scanner.nextLine().trim();
        int size = 150;
        if (!input.isEmpty()) {
            try {
                size = Integer.parseInt(input);
                if (size <= 0) size = 150;
            } catch (NumberFormatException e) {
                System.out.println("Invalid number, using default 150.");
            }
        }

        try {
            System.out.println("\nRunning MPI Parallel Matrix Multiplication for size " + size + "x" + size + "...");
            String report = chatService.runParallelMatrixMultiplication(size);
            System.out.println("\n" + report);
        } catch (Exception e) {
            System.out.println("Matrix multiplication failed: " + e.getMessage());
            try {
                reconnect();
                String report = chatService.runParallelMatrixMultiplication(size);
                System.out.println("\n" + report);
            } catch (Exception ex) {
                System.out.println("Unable to complete matrix multiplication: " + ex.getMessage());
            }
        }
    }
}