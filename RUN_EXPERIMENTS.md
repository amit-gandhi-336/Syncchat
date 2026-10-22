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

## Local browser dashboard

For a local simulation, the dashboard starts the three server JVMs and the
load-balancer JVM, captures their terminal output, and provides a basic chat
client in a browser. Compile and launch the dashboard from the project root:

```sh
mkdir -p out
javac --add-modules jdk.httpserver -d out common/*.java server/*.java client/*.java
java --add-modules jdk.httpserver -cp out server.SimulationDashboard
```

By default, open <http://localhost:8080>, choose STRONG or EVENTUAL, and click **Start
3-node simulation**. Use the Chat client tab to register a username, send
messages, view the inbox, check status, and run MapReduce analytics. The Server
logs tab selects a node's output; the Load balancer logs tab shows routing and
health messages. **Stop simulation** shuts down the four child processes. The
dashboard also stops them when it exits normally.

Stop any manually started servers/load balancer first; the dashboard needs the
same RMI ports (2000-2003 and 3001-3003, plus load-balancer object port 2200).

To use the same dashboard from other laptops on your tailnet, stop it and
restart it bound to this computer's Tailscale IP. For node 1 in this setup:

```sh
java --add-modules jdk.httpserver -Dsyncchat.dashboard.host=100.68.59.4 -cp out server.SimulationDashboard
```

Then visit <http://100.68.59.4:8080> from the other Tailscale-connected laptops.
Allow TCP 8080 to this machine from trusted tailnet devices only. The dashboard
has no login and can start/stop local processes, show server/load-balancer logs,
and access chat functions; anyone who can reach it can use those controls, so
do not bind it to `0.0.0.0` or expose it to the public internet. Binding to a
Tailscale IP makes the UI remotely accessible, but simulation processes are
still launched on the dashboard host itself; it does not start server JVMs on
the other computers.

## Run three servers across Tailscale

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
java -Djava.rmi.server.hostname=100.68.59.4 -Dsyncchat.rmi.exportPort=2100 -Dsyncchat.nodes="1@100.68.59.4@2001,2@100.67.169.23@2002,3@100.120.104.65@2003" -cp out server.ClockServer 1 2001 0 true EVENTUAL
```

Node 2:

```sh
java -Djava.rmi.server.hostname=100.67.169.23 -Dsyncchat.rmi.exportPort=2100 -Dsyncchat.nodes="1@100.68.59.4@2001,2@100.67.169.23@2002,3@100.120.104.65@2003" -cp out server.ClockServer 2 2002 100 false EVENTUAL
```

Node 3:

```sh
java -Djava.rmi.server.hostname=100.120.104.65 -Dsyncchat.rmi.exportPort=2100 -Dsyncchat.nodes="1@100.68.59.4@2001,2@100.67.169.23@2002,3@100.120.104.65@2003" -cp out server.ClockServer 3 2003 -100 false EVENTUAL
```

Allow TCP port 2100 and the node's registry port through each server firewall for tailnet traffic only: node 1 needs 2100 and 2001, node 2 needs 2100 and 2002, node 3 needs 2100 and 2003. Do not expose RMI ports to the public internet. Tailscale ping confirms VPN reachability, but does not confirm the firewall permits these TCP ports.

From a Tailscale-connected client machine, connect to node 1:

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

Run the same client command on a second computer for a second client, and use a different username. To run the periodic Berkeley clock coordinator from a tailnet machine (default syncs every 60 seconds; specify custom interval as optional 4th argument or via `-Dsyncchat.berkeley.intervalSeconds`):

```sh
# Synchronize every 60 seconds (default)
java -cp out server.BerkeleyCoordinator 100.68.59.4 100.67.169.23 100.120.104.65

# Or synchronize every 2 minutes (120 seconds)
java -cp out server.BerkeleyCoordinator 100.68.59.4 100.67.169.23 100.120.104.65 120
```

## One Tailscale server with EVENTUAL consistency

You can run only node 1 if you do not need backup or failover. Compile as above, then start this command on `100.68.59.4`:

```sh
java -Djava.rmi.server.hostname=100.68.59.4 -Dsyncchat.rmi.exportPort=2100 -Dsyncchat.nodes="1@100.68.59.4@2001" -cp out server.ClockServer 1 2001 0 true EVENTUAL
```

The one-node list keeps replication and MapReduce local. Start clients on any tailnet-connected computers with the same client command shown above. In this configuration, chat data exists only on the primary and is lost if that process stops.

## Berkeley clock synchronization

### Automatic synchronization on Primary startup
When you start the server cluster, the **Primary node automatically starts the Berkeley Coordinator** in the background. It waits for initial node discovery (default: 60s) and then synchronizes the clocks of all active nodes automatically every configured interval (default: 60 seconds / 1 minute).

You can configure the interval (e.g., 2 minutes / 120s) and initial delay via JVM options when starting the primary node:

```sh
# Start Primary with Berkeley clock sync every 2 minutes (120 seconds)
java -Dsyncchat.berkeley.intervalSeconds=120 -cp out server.ClockServer 1 2001 0 true STRONG

# Optional: set initial delay before first sync (e.g., 30s instead of default interval)
java -Dsyncchat.berkeley.intervalSeconds=120 -Dsyncchat.berkeley.initialDelaySeconds=30 -cp out server.ClockServer 1 2001 0 true STRONG
```

If the primary node fails and a backup node wins the Bully election, the newly elected primary automatically takes over running the Berkeley clock coordinator.

### Manual / Standalone coordinator execution
You can also run the coordinator manually in its own terminal or from any machine:

```sh
# Default: runs every 60 seconds (1 minute)
java -cp out server.BerkeleyCoordinator

# Run every 2 minutes (120 seconds)
java -cp out server.BerkeleyCoordinator 120
```

## Bully election (local cluster)

Start three local nodes using the commands in **Local three-node cluster**. Stop a server to simulate failure and observe election messages in the remaining server terminals.

## MapReduce chat activity analytics

Start the three-node cluster, register users, and send messages. Select the client's **Chat Activity Analytics (MapReduce)** menu option to count messages by sender. The primary snapshots its in-memory messages, divides them into batches of up to 100, distributes map tasks to the configured nodes, and reduces their partial sender counts. It analyzes only the primary's snapshot, avoiding duplicate counts from replicated backup data. With the one-node setup, the map work runs locally. Chat storage remains in-memory and is not partitioned; restarting nodes clears their data.

To try EVENTUAL mode with all three local servers, stop the existing processes and restart the same three server commands with `EVENTUAL` as the last argument.

## MPI Collective Communication (Broadcast, Scatter, Gather)

This experiment demonstrates MPI-style collective communication operations across all nodes in the cluster:

1. **Broadcast (`MPI_Bcast`)**: Root process (Node 1) sends the exact same piece of data to all processes in the communicator.
2. **Scatter (`MPI_Scatter`)**: Root process partitions an array of items into equal chunks and sends a distinct chunk to each process in rank order.
3. **Gather (`MPI_Gather`)**: Root process collects distinct processed data chunks from all processes in rank order and assembles them into a single final buffer.

### How to Run:

Start the 3-node cluster in three terminals as usual:
```sh
java -cp out server.ClockServer 1 2001 0 true STRONG
java -cp out server.ClockServer 2 2002 100 false STRONG
java -cp out server.ClockServer 3 2003 -100 false STRONG
```

#### Method 1: Using the Standalone Demo Runner (Recommended for quick testing)
In another terminal, run:
```sh
java -cp out server.MPIDemo
```

#### Method 2: Through the Chat Client Menu
In another terminal, run:
```sh
java -cp out client.ChatClient
```
Select menu option **5** to test **MPI Broadcast**, or option **6** to test **MPI Scatter & Gather**.

## EXP 10: Parallel Matrix Multiplication using MPI

This experiment implements 1D Row-Wise Parallel Matrix Multiplication ($C = A \times B$) across all cluster nodes using MPI collective functions:

1. **`MPI_Bcast`**: Broadcasts the full Matrix $B$ ($N \times N$) from the Root process to all participating worker processes.
2. **`MPI_Scatter`**: Distributes equal chunks of rows of Matrix $A$ to each worker process in rank order.
3. **Local Compute**: Each process multiplies its assigned rows of Matrix $A$ with Matrix $B$.
4. **`MPI_Gather`**: Assembles the computed partial result rows into the complete Matrix $C$ at Root.
5. **Benchmarking & Verification**: Compares parallel execution time against sequential multiplication, verifies mathematical correctness, and reports speedup and parallel efficiency.

### How to Run:

Start the 3-node cluster in three terminals:
```sh
java -cp out server.ClockServer 1 2001 0 true STRONG
java -cp out server.ClockServer 2 2002 100 false STRONG
java -cp out server.ClockServer 3 2003 -100 false STRONG
```

#### Method 1: Using the Standalone Matrix Runner (Recommended)
In another terminal, run:
```sh
# Default (150x150)
java -cp out server.MatrixDemo

# Or specify custom matrix dimension (e.g. 200x200 or 300x300):
java -cp out server.MatrixDemo 200
```

#### Method 2: Through the Chat Client Menu
In another terminal, run:
```sh
java -cp out client.ChatClient
```
Select menu option **7** (**Parallel Matrix Multiplication (MPI)**) and enter the matrix size.
