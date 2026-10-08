package common;

package common;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;
import java.util.Map;

public interface ReplicationService extends Remote {

    void replicateMessage(
            String sender,
            String receiver,
            String content)
            throws RemoteException;

    boolean replicateUser(
            String username)
            throws RemoteException;

    Map<String, List<Message>> getState()
            throws RemoteException;

    void installState(
            Map<String, List<Message>> state)
            throws RemoteException;
}