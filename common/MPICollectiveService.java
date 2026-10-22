package common;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

/**
 * Remote interface for MPI Collective Communication operations:
 * Broadcast, Scatter, and Gather.
 */
public interface MPICollectiveService extends Remote {

    /**
     * MPI Broadcast: Called on each process to receive the exact same data from the root.
     */
    void receiveBroadcast(String rootRankInfo, String message) throws RemoteException;

    /**
     * MPI Scatter: Called on each process to receive its assigned data chunk from root.
     */
    void receiveScatterChunk(int rank, List<String> chunk) throws RemoteException;

    /**
     * MPI Gather: Called by root on each process to collect its processed chunk.
     */
    List<String> getGatherChunk(int rank) throws RemoteException;

    /**
     * MPI Broadcast: Receive full Matrix B on all processes.
     */
    void mpiBcastMatrixB(int[][] matrixB) throws RemoteException;

    /**
     * MPI Scatter: Receive assigned subset of rows of Matrix A for this rank.
     */
    void mpiScatterRowsA(int rank, int[][] rowsA) throws RemoteException;

    /**
     * Local compute and MPI Gather: Worker multiplies assigned rows of A with Matrix B,
     * and returns the resulting partial rows of Matrix C to the Root process.
     */
    int[][] mpiComputeAndGatherRowsC(int rank) throws RemoteException;
}
