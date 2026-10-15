package common;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface NodeService extends Remote {

    int getNodeId() throws RemoteException;

    boolean isAlive() throws RemoteException;

    boolean isPrimary() throws RemoteException;

    void setPrimary(boolean primary)
            throws RemoteException;

    /** Initiate a new ring election from this node. */
    void startRingElection()
            throws RemoteException;

    /**
     * Receive a ring-election token carrying the given candidateId
     * and apply the Chang-Roberts forwarding logic.
     */
    void forwardElectionMessage(int candidateId)
            throws RemoteException;

    void announcePrimary(int winnerId)
            throws RemoteException;
}