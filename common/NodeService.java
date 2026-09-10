package common;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface NodeService extends Remote {

    int getNodeId() throws RemoteException;

    boolean isAlive() throws RemoteException;

    boolean isPrimary() throws RemoteException;

    void setPrimary(boolean primary)
            throws RemoteException;

    void startBullyElection()
            throws RemoteException;

    void startRingElection(int candidateId)
            throws RemoteException;

    void announcePrimary(int winnerId)
            throws RemoteException;
}