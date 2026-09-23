package common;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface ReplicationService extends Remote {

    void replicateMessage(
            String sender,
            String receiver,
            String content)
            throws RemoteException;

    boolean replicateUser(
            String username)
            throws RemoteException;
}