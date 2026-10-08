# Syncchat experiments

Run all commands from the project root directory.

## Compile

```sh
mkdir -p out
javac -d out common/*.java server/*.java client/*.java
```

The server command format is:

```text
java ... server.ClockServer <nodeId> <registryPort> <clockOffset> <primary> <STRONG|EVENTUAL>
```

Keep every server running in its own terminal. Start the primary before connecting clients.

## Local three-node cluster

Start one node per terminal on the same computer:

```sh
java -cp out server.ClockServer 1 2001 0 true STRONG
java -cp out server.ClockServer 2 2002 100 false STRONG
java -cp out server.ClockServer 3 2003 -100 false STRONG
```

Start the client in another terminal. If it is local, the default endpoint is `localhost:2001`:

```sh
java -cp out client.ChatClient
```

The client menu includes send message, check inbox, server status, MapReduce chat activity analytics, and exit. Choose different usernames for separate clients.

## Run three servers across Tailscale

To use the web client with manually started servers, start Node 1, Node 2, and
Node 3 using the commands below, then start the load balancer on the machine
whose Tailscale address serves the dashboard. Compile the updated project on
each machine first; servers and the load balancer mirror terminal output to
`logs/node1.log`, `logs/node2.log`, `logs/node3.log`, and
`logs/load-balancer.log`, respectively. The dashboard reads logs from its local
`logs/` directory, so each server-log tab shows only files present on that
dashboard host. The load-balancer log is visible on the laptop running the
load-balancer process.

Start the dashboard on the load-balancer laptop. For example, on Node 1:

```sh
java --add-modules jdk.httpserver -Dsyncchat.dashboard.host=100.68.59.4 -cp out server.SimulationDashboard
```

Open <http://100.68.59.4:8080> from a trusted tailnet device. The dashboard UI
must be hosted on the load-balancer laptop because the backend connects to a
load balancer on its own host. Allow TCP 8080 to the dashboard host, TCP 2000
and 2200 to the load-balancer host, and the node
registry ports plus service port 2100 between cluster nodes. Keep these ports
available to tailnet peers only. Browser registration, chat, status, and
analytics are routed through the load balancer at the dashboard host's Tailscale
IP on port 2000.

The configured Tailscale IPs are:

- Node 1 (primary): `100.68.59.4`
- Node 2: `100.67.169.23`
- Node 3: `100.120.104.65`

Compile the same project on each computer:

```sh
mkdir -p out
javac -d out common/*.java server/*.java client/*.java
```

Start each command on its respective computer. The peer-list property must be identical on all three servers. Each server advertises its own Tailscale IP, uses its registry port, and exports its RMI service on TCP port 2100.

Node 1:

```sh
java -Djava.rmi.server.hostname=100.68.59.4 -Dsyncchat.rmi.exportPort=2100 -Dsyncchat.nodes="1@100.68.59.4@2001,2@100.67.169.23@2002,3@100.120.104.65@2003" -cp out server.ClockServer 1 2001 0 true STRONG
```

Node 2:

```sh
java -Djava.rmi.server.hostname=100.67.169.23 -Dsyncchat.rmi.exportPort=2100 -Dsyncchat.nodes="1@100.68.59.4@2001,2@100.67.169.23@2002,3@100.120.104.65@2003" -cp out server.ClockServer 2 2002 100 false STRONG
```

Node 3:

```sh
java -Djava.rmi.server.hostname=100.120.104.65 -Dsyncchat.rmi.exportPort=2100 -Dsyncchat.nodes="1@100.68.59.4@2001,2@100.67.169.23@2002,3@100.120.104.65@2003" -cp out server.ClockServer 3 2003 -100 false STRONG
```

Allow TCP port 2100 and the node's registry port through each server firewall for tailnet traffic only: node 1 needs 2100 and 2001, node 2 needs 2100 and 2002, node 3 needs 2100 and 2003. Do not expose RMI ports to the public internet. Tailscale ping confirms VPN reachability, but does not confirm the firewall permits these TCP ports.

### Exact role-by-role commands

Use these roles for the three laptops whose Tailscale IPs are listed above:

1. **Your laptop / Node 1 (`100.68.59.4`)**: run the Node 1 server command above, then start the load balancer below in a second terminal. Keep both running.
2. **Teammate laptop / Node 2 (`100.67.169.23`)**: run the Node 2 server command above and keep it running.
3. **Teammate laptop / Node 3 (`100.120.104.65`)**: run the Node 3 server command above and keep it running.
4. **Every laptop that will use the Java client**: compile the project, then connect to Node 1's load balancer on port `2000`. Teammates who are only clients do not start another server or load balancer.

On each server laptop, allow its registry port and RMI object port `2100` from the Tailscale network. If that laptop uses UFW, run the corresponding commands on that laptop:

Node 1:

```sh
sudo ufw allow in on tailscale0 from 100.64.0.0/10 to any port 2001 proto tcp
sudo ufw allow in on tailscale0 from 100.64.0.0/10 to any port 2100 proto tcp
```

Node 2:

```sh
sudo ufw allow in on tailscale0 from 100.64.0.0/10 to any port 2002 proto tcp
sudo ufw allow in on tailscale0 from 100.64.0.0/10 to any port 2100 proto tcp
```

Node 3:

```sh
sudo ufw allow in on tailscale0 from 100.64.0.0/10 to any port 2003 proto tcp
sudo ufw allow in on tailscale0 from 100.64.0.0/10 to any port 2100 proto tcp
```

On Node 1, also allow the load-balancer registry and object ports for Tailscale clients:

```sh
sudo ufw allow in on tailscale0 from 100.64.0.0/10 to any port 2000 proto tcp
sudo ufw allow in on tailscale0 from 100.64.0.0/10 to any port 2200 proto tcp
sudo ufw allow in on tailscale0 from 100.64.0.0/10 to any port 8080 proto tcp
```

These UFW examples assume the Tailscale interface is named `tailscale0` (`ip addr` shows the interface name). If UFW is not the active firewall, add equivalent inbound TCP rules restricted to the Tailscale interface/network in the active firewall or Tailscale ACL. Keep these ports restricted to the tailnet; do not expose them publicly.

After all three servers have started, start the load balancer **only on Node 1 (`100.68.59.4`)**, in another terminal:

```sh
java -Djava.rmi.server.hostname=100.68.59.4 -cp out server.LoadBalancer 2000 2200 "1@100.68.59.4@2001,2@100.67.169.23@2002,3@100.120.104.65@2003"
```

On each teammate laptop that will run a Java client, from the project root:

```sh
mkdir -p out
javac -d out common/*.java server/*.java client/*.java
java -cp out client.ChatClient 100.68.59.4 2000
```

Use a different username on each client. The client command must point to the **load balancer** at `100.68.59.4:2000`, not a teammate's backend registry port. If the dashboard web client is being used instead, it connects through the same load balancer; the dashboard process itself runs on Node 1.

For browser-based teammates, start the dashboard on Node 1 in another terminal after the load balancer is running:

```sh
java --add-modules jdk.httpserver -Dsyncchat.dashboard.host=100.68.59.4 -cp out server.SimulationDashboard
```

Each teammate opens `http://100.68.59.4:8080` in a browser while connected to Tailscale. They do not launch a server or dashboard locally.

From each client laptop, verify both load-balancer TCP ports are reachable before launching Java:

```sh
nc -vz 100.68.59.4 2000
nc -vz 100.68.59.4 2200
```

From Node 1, verify it can reach each backend's registry and RMI object ports:

```sh
nc -vz 100.68.59.4 2001
nc -vz 100.68.59.4 2100
nc -vz 100.67.169.23 2002
nc -vz 100.67.169.23 2100
nc -vz 100.120.104.65 2003
nc -vz 100.120.104.65 2100
```

If a check says `Connection refused`, the target host is reachable but no process is listening on that port or its firewall is actively rejecting it. Confirm the corresponding server/load-balancer process is running and its startup output lists the expected ports. If it times out, check Tailscale connectivity, firewall rules, and ACLs. RMI needs **both** the registry and exported-object port open; opening only `2000` or `2001` is not sufficient.

For a direct-to-primary test without the load balancer, a Tailscale-connected client can use:

```sh
java -cp out client.ChatClient 100.68.59.4 2001
```

## Run clients through the load balancer

`server.LoadBalancer` is a separate RMI gateway. It probes configured nodes
through their existing `NodeService` (`isAlive()` and `isPrimary()`) every two
seconds. It round-robins inbox and status reads across nodes it can reach.
Registration, message writes, and MapReduce analytics are sent only to the
node that currently reports itself as primary. If no unique healthy primary is
reported, it refuses those operations rather than writing to a backup. Failed
nodes are excluded from reads and probed again so they can rejoin when reachable.
Primary-only requests refresh the election state before routing, so the gateway
can follow a newly elected primary without a restart.

First start all three backend servers using the Tailscale commands above. Then
start one load balancer process on node 1 (or another tailnet host). The gateway
uses registry port 2000 and exported-object port 2200 in this example:

```sh
java -Djava.rmi.server.hostname=100.68.59.4 -cp out server.LoadBalancer 2000 2200 "1@100.68.59.4@2001,2@100.67.169.23@2002,3@100.120.104.65@2003"
```

Allow TCP 2000 and 2200 on the gateway host for tailnet clients. Continue to
allow the backend registry ports and service port 2100 between backend nodes.
Clients should connect to the gateway, not directly to node 1:

```sh
java -cp out client.ChatClient 100.68.59.4 2000
```

If using a different gateway computer, use its Tailscale IP both for
`-Djava.rmi.server.hostname` and as the client host. The configured backends
remain the same. Reads from a backup may be stale under EVENTUAL consistency;
a node that rejoins after losing its in-memory state is reachable but does not
automatically catch up its data.

If a backup is unreachable, the primary now skips that peer for the current
replication attempt, including in STRONG mode, so a live primary can continue
serving clients after failover. STRONG mode waits for reachable backups to
acknowledge; this is not quorum replication, and an offline/restarted node may
miss writes because this project does not yet synchronize it back up. A
reachable backup that rejects replication still causes a STRONG write to fail.

Run the same client command on a second computer for a second client, and use a different username. To run the Berkeley clock coordinator from a tailnet machine:

```sh
java -cp out server.BerkeleyCoordinator 100.68.59.4 100.67.169.23 100.120.104.65
```

## One Tailscale server with EVENTUAL consistency

You can run only node 1 if you do not need backup or failover. Compile as above, then start this command on `100.68.59.4`:

```sh
java -Djava.rmi.server.hostname=100.68.59.4 -Dsyncchat.rmi.exportPort=2100 -Dsyncchat.nodes="1@100.68.59.4@2001" -cp out server.ClockServer 1 2001 0 true EVENTUAL
```

The one-node list keeps replication and MapReduce local. Start clients on any tailnet-connected computers with the same client command shown above. In this configuration, chat data exists only on the primary and is lost if that process stops.

## Berkeley clock synchronization (local cluster)

Start the three local nodes using the commands in **Local three-node cluster**, then run:

```sh
java -cp out server.BerkeleyCoordinator
```

## Bully election (local cluster)

Start three local nodes using the commands in **Local three-node cluster**. Stop a server to simulate failure and observe election messages in the remaining server terminals.

## MapReduce chat activity analytics

Start the three-node cluster, register users, and send messages. Select the client's **Chat Activity Analytics (MapReduce)** menu option to count messages by sender. The primary snapshots its in-memory messages, divides them into batches of up to 100, distributes map tasks to the configured nodes, and reduces their partial sender counts. It analyzes only the primary's snapshot, avoiding duplicate counts from replicated backup data. With the one-node setup, the map work runs locally. Chat storage remains in-memory and is not partitioned; restarting nodes clears their data.

To try EVENTUAL mode with all three local servers, stop the existing processes and restart the same three server commands with `EVENTUAL` as the last argument.
