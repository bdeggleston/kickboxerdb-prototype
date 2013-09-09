punisher1
=========

##peers

there is a peer server, a client server, and a cluster

when the punisher server is created, a local node is created which connects to the local store. This
is added to a cluster object which is sent to the client and peer servers.

all messages sent between the nodes should be idempotent