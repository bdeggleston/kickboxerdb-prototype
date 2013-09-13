punisher1
=========

##peers

there is a peer server, a client server, and a cluster

when the punisher server is created, a local node is created which connects to the local store. This
is added to a cluster object which is sent to the client and peer servers.

all messages sent between the nodes should be idempotent


##adding new nodes

When a node is added to the cluster and claims a portion of the ring, it should
determine which nodes replicate it's data and immediately start accepting requests
on behalf of it's portion of the ring. In the meantime, it should start streaming
it's data from the old owner nodes, and forward requests for keys it does not
have to the nodes it's streaming data from.

Should it delete old data on the nodes it's taking data from?

How the joining process works:
    * node starts up
    * if initializing, the node builds a private view of the cluster
        without itself. This will be used for determining where
        to stream data from
    * node begins streaming keys from old replica nodes, and resolving
        differences between nodes and writes that have occured since
        streaming began
    * node notifies old owners that they can delete keys if applicable
    * node forwards requests for keys it doesn't have yet to the old replicas
    * node accepts writes for data in it's key range (1)

when a node leaves the cluster permanently, the above process is more or less reversed.

notes:
1. when a node has received a write while streaming, if that key is subsequently read,
    it would be possible that the old nodes have more data that the new node does not.
    Because of this, when receiving a write command, the new node should immediately
    fetch and reconcile existing nodes data, then perform it's write normally

## migrating data to a new node

The new node is responsible for handling the process.

Given this ring change:
```
old ring:
[     n1     ][     n2     ][     n3     ][     n4     ]

new ring
[     n1     ][  n2 ][  nZ ][     n3     ][     n4     ]
```

nZ will find the old token owner for it's token, in this case n2, and it's replica
nodes, in this case n3 & n4, assuming a replication factor of 3.

nZ will determine it's total token range (owned and replicated), and begin querying
each node for token ranges (start token, max token and size) the max token

# nanny process
each cluster (node) should have a background process that picks up after the node.
It is responsible for:
    * keeping an up to date picture of the cluster (pinging other nodes)
    * polling it's peers for 'hints' for write messages it's missed
    * removing data that's no longer in it's key range?