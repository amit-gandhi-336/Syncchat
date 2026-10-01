cd /home/Amit/Desktop/amit/study/sem5/dc/Syncchat

## 1. Compile the project

Compile all common, server, and client classes into `out/`:

rm -rf out
mkdir out
javac -d out common/_.java server/_.java

## 2. Java RMI and chat experiment

### Terminal 1: start the RMI server

java -cp out server.ClockServer 1 1099 0 true

Arguments are `<nodeId> <port> <clockOffset> <primary>`. Keep this terminal running.

### Terminal 2: start a client

java -cp out client.ChatClient

## Run the cluster across Tailscale

On each Linux server, obtain that machine's Tailscale IPv4 address with
`tailscale ip -4`. Allow incoming TCP traffic on that machine's registry port
and TCP 2100 from the tailnet only. The registry ports below are 2001, 2002,
and 2003; every server uses fixed RMI object port 2100. Both ports are required
because RMI first contacts the registry and then the exported server object.

On all three machines, compile the same project version:

```sh
javac -d out common/*.java server/*.java client/*.java
```

Replace `100.x.y.1`, `.2`, and `.3` with the actual Tailscale IPs. Start the
following commands on their respective machines. Keep the identical
`syncchat.nodes` list on every server:

Node 1:

```sh
java -Djava.rmi.server.hostname=100.x.y.1 -Dsyncchat.rmi.exportPort=2100 -Dsyncchat.nodes="1@100.x.y.1@2001,2@100.x.y.2@2002,3@100.x.y.3@2003" -cp out server.ClockServer 1 2001 0 true STRONG
```

Node 2:

```sh
java -Djava.rmi.server.hostname=100.x.y.2 -Dsyncchat.rmi.exportPort=2100 -Dsyncchat.nodes="1@100.x.y.1@2001,2@100.x.y.2@2002,3@100.x.y.3@2003" -cp out server.ClockServer 2 2002 100 false STRONG
```

Node 3:

```sh
java -Djava.rmi.server.hostname=100.x.y.3 -Dsyncchat.rmi.exportPort=2100 -Dsyncchat.nodes="1@100.x.y.1@2001,2@100.x.y.2@2002,3@100.x.y.3@2003" -cp out server.ClockServer 3 2003 -100 false STRONG
```

Start the client on any tailnet-connected machine, passing node 1's Tailscale
IP and registry port:

```sh
java -cp out client.ChatClient 100.x.y.1 2001
```

All machines need matching compiled interfaces/classes. Tailscale ping only
confirms basic VPN reachability; verify firewall/ACL access to the registry and
RMI object ports as well. Never expose these Java RMI ports to the public
internet. The cluster configuration controls replication, election, and
MapReduce workers. The Berkeley clock coordinator can be run with the three
Tailscale IPs as arguments.

## 3. Multithreading experiment

There is no separate multithreading `main` class. Multithreading is implemented inside `ChatServer` with a fixed five-thread `ExecutorService`. Run the RMI server, then start multiple clients concurrently:

### Terminal 1: start the server

java -cp out server.ClockServer 1 1099 0 true

### Terminals 2 and 3: start clients

java -cp out client.ChatClient

## 4. Clock synchronization experiment

### Terminals 1, 2, and 3: start three nodes

java -cp out server.ClockServer 1 2001 5000 true

java -cp out server.ClockServer 2 2002 -3000 false

java -cp out server.ClockServer 3 2003 12000 false

java -cp out server.BerkeleyCoordinator

## 5. Bully election experiment

java -cp out server.ClockServer 1 2001 0 true

java -cp out server.ClockServer 2 2002 0 false

java -cp out server.ClockServer 3 2003 0 false

cd /home/Amit/Desktop/amit/study/sem5/dc/Syncchat

## 1. Compile the project

rm -rf out
mkdir out
javac -d out common/_.java server/_.java client/\*.java

## 2. Experiment 5 - Strong Consistency

### Terminal 1: start Primary Node

java -cp out server.ClockServer 1 2001 0 true STRONG

### Terminal 2: start Backup Node 2

java -cp out server.ClockServer 2 2002 100 false STRONG

### Terminal 3: start Backup Node 3

java -cp out server.ClockServer 3 2003 -100 false STRONG

### Terminal 4: start the client

java -cp out client.ChatClient

## MapReduce chat activity analytics

Syncchat includes a distributed MapReduce job that reports the number of
messages sent by each sender. The primary takes a consistent-enough snapshot
of its in-memory inboxes, splits it into batches of up to 100 messages, and
assigns the disjoint batches across configured cluster nodes. Each worker maps
its batch to `(sender, 1)` and locally combines counts; the primary reduces the
partial maps by summing each sender's counts. Only the primary snapshot is
processed, so replicated copies on backup nodes are not counted a second time.

Compile and start the three nodes using the strong-consistency commands above.
Register users and send some messages. In the client, choose menu option 4
(`Chat Activity Analytics (MapReduce)`) to see the reduced sender totals. The
worker service is registered in each node's RMI registry as `MapReduceService`.
If a remote worker is unavailable, its assigned batch is mapped locally so the
analytics request can still finish.

This is distributed computation over a primary-owned snapshot, not partitioned
chat storage: the existing application keeps messages in memory and replicates
them to backups. Restarting nodes clears that in-memory chat history.

## 3. Experiment 5 - Eventual Consistency

Stop the three server terminals using Ctrl+C before starting Eventual Consistency.

### Terminal 1: start Primary Node

java -cp out server.ClockServer 1 2001 0 true EVENTUAL

### Terminal 2: start Backup Node 2

java -cp out server.ClockServer 2 2002 100 false EVENTUAL

### Terminal 3: start Backup Node 3

java -cp out server.ClockServer 3 2003 -100 false EVENTUAL

### Terminal 4: start the client

java -cp out client.ChatClient
