package server;

import common.ChatService;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Arrays;
import java.util.List;

public class MPIDemo {

    public static void main(String[] args) {
        String host = args.length > 0 ? args[0] : "localhost";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 2001;

        System.out.println("=================================================");
        System.out.println("      MPI COLLECTIVE COMMUNICATION DEMO");
        System.out.println(" Connecting to Root Node / Primary at " + host + ":" + port);
        System.out.println("=================================================");

        try {
            Registry registry = LocateRegistry.getRegistry(host, port);
            ChatService service = (ChatService) registry.lookup("ChatService");

            // 1. MPI BROADCAST DEMO
            System.out.println("\n>>> [TEST 1] Triggering MPI Broadcast from Root...");
            service.mpiBroadcast("EMERGENCY CLUSTER BROADCAST: ALL NODES SYNC STATE");
            System.out.println(">>> MPI Broadcast completed successfully.");

            Thread.sleep(1500);

            // 2. MPI SCATTER & GATHER DEMO
            System.out.println("\n>>> [TEST 2] Triggering MPI Scatter & Gather from Root...");
            List<String> dataset = Arrays.asList(
                    "record_1", "record_2", "record_3",
                    "record_4", "record_5", "record_6"
            );
            System.out.println(">>> Original Root Buffer (6 items): " + dataset);
            System.out.println(">>> Scattering chunks across processes, transforming on ranks, and gathering back...");

            List<String> gathered = service.mpiScatterGather(dataset);

            System.out.println("\n>>> [FINAL ASSEMBLED BUFFER GATHERED AT ROOT]:");
            for (int i = 0; i < gathered.size(); i++) {
                System.out.println("    [" + i + "] " + gathered.get(i));
            }

            System.out.println("\n=================================================");
            System.out.println("   MPI COLLECTIVE COMMUNICATION TEST PASSED!");
            System.out.println("=================================================");

        } catch (Exception e) {
            System.err.println("Error during MPI demo execution: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
