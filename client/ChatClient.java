package client;

import common.ChatService;
import common.Message;

import java.util.List;
import java.util.Map;
import java.util.Scanner;

public class ChatClient {

    private static String node1IP;
    private static String node2IP;
    private static String node3IP;

    private static ChatService chatService;

    public static void main(String[] args) {

        if (args.length < 3) {

            System.out.println(
                    "Usage:"
            );

            System.out.println(
                    "java client.ChatClient " +
                    "<node1IP> " +
                    "<node2IP> " +
                    "<node3IP>"
            );

            return;
        }

        node1IP = args[0];
        node2IP = args[1];
        node3IP = args[2];

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
                        "5. Exit"
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

        chatService =
                FailoverClient.connect(
                        node1IP,
                        node2IP,
                        node3IP
                );
    }

    private static void reconnect() {

        int attempts = 0;

        while (attempts < 20) {

            try {

                Thread.sleep(2000);

                chatService =
                        FailoverClient.connect(
                                node1IP,
                                node2IP,
                                node3IP
                        );

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
}