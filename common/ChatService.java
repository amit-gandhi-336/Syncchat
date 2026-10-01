package common;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;
import java.util.Map;

public interface ChatService extends Remote {
    boolean registerUser(String username) throws RemoteException;
    void sendMessage(String sender, String receiver, String content)
            throws RemoteException;
    List<Message> getMessages(String username) throws RemoteException;
    Map<String, Long> getMessageCountsBySender() throws RemoteException;
    String getServerStatus() throws RemoteException;
}