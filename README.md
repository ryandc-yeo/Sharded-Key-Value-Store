# Sharded Key Value Store Project

Primary-backup replication for a simple key-value store.

In each project I had to do substantial design. I was given a sketch of the overall design (and code for the utility functions), but I had to flesh it out and nail down a complete protocol. The tests explore the protocol's handling of failure scenarios as well as general correctness and basic performance.

## 3 stages

1. The `pbservice` uses primary/backup replication, assisted by a view service that decides which machines are alive. The view service allows the primary/backup service to work correctly in the presence of network partitions. The view service itself is not replicated, and is a single point of failure.

2. `paxos` uses the Paxos protocol to replicate the key/value database with no single point of failure, and handles network partitions correctly. This key/value service is slower than a non-replicated key/value server would be, but is fault tolerant.

3. The final stage is a sharded key/value database, where each shard replicates its state using Paxos. This key/value service can perform Put/Get operations in parallel on different shards, allowing it to support applications such as MapReduce that can put a high load on a storage system. This stage also has a replicated configuration service, which tells the shards what key range they are responsible for. It can change the assignment of keys to shards, for example, in response to changing load. It has the core of a real-world design for thousands of servers.

The services support three RPCs: Put(key, value), Append(key, arg), and Get(key). The service maintains a simple database of key/value pairs. Put() replaces the value for a particular key in the database. Append(key, arg) appends arg to key's value. Get() fetches the current value for a key.
