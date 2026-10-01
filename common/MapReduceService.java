package common;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;
import java.util.Map;

/** Worker-side map operation for chat analytics jobs. */
public interface MapReduceService extends Remote {
    Map<String, Long> mapMessageCounts(List<Message> batch)
            throws RemoteException;
}