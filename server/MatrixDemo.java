package server;

import common.ChatService;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class MatrixDemo {

    public static void main(String[] args) {
        String host = "localhost";
        int port = 2001;
        int size = 150;

        if (args.length > 0) {
            try {
                size = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                host = args[0];
            }
        }
        if (args.length > 1) {
            try {
                size = Integer.parseInt(args[1]);
            } catch (NumberFormatException e) {
                port = Integer.parseInt(args[1]);
            }
        }

        System.out.println("=================================================");
        System.out.println("  EXP 10: PARALLEL MATRIX MULTIPLICATION (MPI)");
        System.out.println(" Connecting to Root / Primary at " + host + ":" + port);
        System.out.println(" Matrix Dimension N: " + size + "x" + size);
        System.out.println("=================================================");

        try {
            Registry registry = LocateRegistry.getRegistry(host, port);
            ChatService service = (ChatService) registry.lookup("ChatService");

            System.out.println("Invoking MPI Matrix Multiplication...");
            String report = service.runParallelMatrixMultiplication(size);
            System.out.println(report);

        } catch (Exception e) {
            System.err.println("Execution failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
