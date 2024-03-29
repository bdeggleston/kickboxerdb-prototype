from kickboxer.cluster.node.local import LocalNode
from kickboxer.cluster.cluster import Cluster
from kickboxer.cluster.peer_server import PeerServer
from kickboxer.cluster.client_server import RedisClientServer
from kickboxer.partitioner.md5 import MD5Partitioner
from kickboxer.store.redis import RedisStore


class Kickboxer(object):
    """ punisher server """

    def __init__(self,
                 client_address=('', 6379),
                 peer_address=('', 4379),
                 token=None,
                 seed_peers=None,
                 name=None,
                 node_id=None,
                 replication_factor=3,
                 cluster_status=Cluster.Status.INITIALIZING,
                 partitioner=None):
        super(Kickboxer, self).__init__()

        self.partitioner = partitioner or MD5Partitioner()
        self.store = RedisStore(self.partitioner)

        self.client_address = client_address
        self.peer_address = peer_address
        #todo: load some config
        self.name = name
        self.local_node = LocalNode(
            self.store,
            address=self.peer_address,
            node_id=node_id,
            name=name,
            token=token,
        )

        self.seed_peers = seed_peers
        self.replication_factor = replication_factor
        self.cluster = Cluster(
            self.local_node,
            self.partitioner,
            seed_peers=self.seed_peers,
            replication_factor=self.replication_factor,
            status=cluster_status
        )

        self.peer_server = PeerServer(
            self.peer_address,
            cluster=self.cluster
        )

        self.client_server = RedisClientServer(
            self.client_address,
            cluster=self.cluster) if self.client_address else None

    def __repr__(self):
        return '<Kickboxer name={} token={}>'.format(self.name, self.token)

    @property
    def node_id(self):
        return self.local_node.node_id

    @property
    def token(self):
        return self.local_node.token

    def start(self):
        self.peer_server.start()
        self.peer_server.start_event.wait(timeout=1)
        self.cluster.start()
        if self.client_server: self.client_server.start()

    def stop(self):
        if self.client_server: self.client_server.stop()
        self.peer_server.stop()
        self.cluster.stop()

    def kill(self):
        if self.client_server: self.client_server.stop()
        self.peer_server.stop()
        self.cluster.kill()

    def replicates_key(self, key):
        return self.local_node in self.cluster.get_nodes_for_key(key)



