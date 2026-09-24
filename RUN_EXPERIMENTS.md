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
javac -d out common/*.java server/*.java client/*.java

## 2. Experiment 5 - Strong Consistency

### Terminal 1: start Primary Node

java -cp out server.ClockServer 1 2001 0 true STRONG

### Terminal 2: start Backup Node 2

java -cp out server.ClockServer 2 2002 100 false STRONG

### Terminal 3: start Backup Node 3

java -cp out server.ClockServer 3 2003 -100 false STRONG

### Terminal 4: start the client

java -cp out client.ChatClient

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
