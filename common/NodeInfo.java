package common;

import java.io.Serializable;

public class NodeInfo implements Serializable {

    private final int nodeId;
    private final String host;
    private final int port;

    public NodeInfo(int nodeId, String host, int port) {
        this.nodeId = nodeId;
        this.host = host;
        this.port = port;
    }

    public int getNodeId() {
        return nodeId;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    @Override
    public String toString() {
        return "Node " + nodeId +
                " (" + host + ":" + port + ")";
    }
}