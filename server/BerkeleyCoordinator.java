package server;

import common.ClockService;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class BerkeleyCoordinator {

    public static void main(String[] args) {

        try {
            String host1 = args.length > 0 ? args[0] : "localhost";
            String host2 = args.length > 1 ? args[1] : "localhost";
            String host3 = args.length > 2 ? args[2] : "localhost";

            Registry r1 =
                LocateRegistry.getRegistry(host1, 2001);

            Registry r2 =
                LocateRegistry.getRegistry(host2, 2002);

            Registry r3 =
                LocateRegistry.getRegistry(host3, 2003);

            ClockService s1 =
                (ClockService) r1.lookup("ClockService");

            ClockService s2 =
                (ClockService) r2.lookup("ClockService");

            ClockService s3 =
                (ClockService) r3.lookup("ClockService");

            long t1 = s1.getTime();
            long t2 = s2.getTime();
            long t3 = s3.getTime();

            System.out.println("\nBefore Synchronization");
            System.out.println("Server 1: " + t1);
            System.out.println("Server 2: " + t2);
            System.out.println("Server 3: " + t3);

            long average = (t1 + t2 + t3) / 3;

            System.out.println("\nAverage: " + average);

            long a1 = average - t1;
            long a2 = average - t2;
            long a3 = average - t3;

            System.out.println("\nAdjustments");
            System.out.println("Server 1: " + a1);
            System.out.println("Server 2: " + a2);
            System.out.println("Server 3: " + a3);

            s1.adjustClock(a1);
            s2.adjustClock(a2);
            s3.adjustClock(a3);

            System.out.println("\nAfter Synchronization");
            System.out.println("Server 1: " + s1.getTime());
            System.out.println("Server 2: " + s2.getTime());
            System.out.println("Server 3: " + s3.getTime());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}