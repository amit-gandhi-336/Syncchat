package client;

import common.ChatService;
import common.Message;
import java.util.Map;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.List;
import java.util.Scanner;

public class ChatClient {

    public static void main(String[] args) {

        try {

            Scanner scanner =
                    new Scanner(System.in);

            String serverHost = args.length > 0 ? args[0] : "localhost";
            int serverPort = args.length > 1
                    ? Integer.parseInt(args[1])
                    : 2001;

            /*
             * Connect to RMI registry.
             */
            Registry registry =
                    LocateRegistry.getRegistry(
                            serverHost,
                            serverPort
                    );

            /*
             * Get remote object.
             */
            ChatService chatService =
                    (ChatService)
                            registry.lookup(
                                    "ChatService"
                            );

            System.out.println();
            System.out.println(
                    "================================="
            );

            System.out.println(
                    "       SYNCCHAT CLIENT"
            );

            System.out.println(
                    "================================="
            );

            /*
             * Login / username.
             */
            System.out.print(
                    "Enter username: "
            );

            String username =
                    scanner.nextLine();

            /*
             * Register user.
             */
            boolean registered =
                    chatService.registerUser(
                            username
                    );

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

                        chatService.sendMessage(
                                username,
                                receiver,
                                message
                        );

                        System.out.println(
                                "Message sent!"
                        );

                        break;

                    case "2":

                        List<Message> messages =
                                chatService.getMessages(
                                        username
                                );

                        System.out.println();
                        System.out.println(
                                "========== INBOX =========="
                        );

                        if (messages.isEmpty()) {

                            System.out.println(
                                    "No messages."
                            );

                        } else {

                            for (
                                    Message msg :
                                    messages
                            ) {

                                System.out.println(
                                        msg
                                );
                            }
                        }

                        System.out.println(
                                "============================"
                        );

                        break;

                    case "3":

                        System.out.println(
                                chatService
                                        .getServerStatus()
                        );

                        break;

                    case "4":

                        Map<String, Long> senderCounts =
                                chatService.getMessageCountsBySender();

                        System.out.println();
                        System.out.println(
                                "===== MESSAGES BY SENDER (MAPREDUCE) ====="
                        );
                        if (senderCounts.isEmpty()) {
                            System.out.println("No messages to analyze.");
                        } else {
                            senderCounts.forEach((sender, count) ->
                                    System.out.println(sender + ": " + count)
                            );
                        }
                        System.out.println(
                                "==========================================="
                        );
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
}