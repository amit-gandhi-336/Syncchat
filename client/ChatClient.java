package client;

import common.ChatService;
import common.Message;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.List;
import java.util.Scanner;

public class ChatClient {

    public static void main(String[] args) {

        try {

            Scanner scanner =
                    new Scanner(System.in);

            /*
             * Connect to RMI registry.
             */
            Registry registry =
                    LocateRegistry.getRegistry(
                            "localhost",
                            2001
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
                        "4. Exit"
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