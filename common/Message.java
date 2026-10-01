package common;

import java.io.Serializable;
import java.time.LocalDateTime;

public class Message implements Serializable {
    private final String sender;
    private final String receiver;
    private final String content;
    private final LocalDateTime time;

    public Message(String sender, String receiver, String content) {
        this.sender = sender;
        this.receiver = receiver;
        this.content = content;
        this.time = LocalDateTime.now();
    }

    public String getSender() {
        return sender;
    }

    public String toString() {
        return "[" + time + "] " + sender +
               " -> " + receiver + ": " + content;
    }
}