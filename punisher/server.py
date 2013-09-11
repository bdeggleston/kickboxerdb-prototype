from punisher.cluster.node.local import LocalNode
from punisher.cluster.cluster import Cluster
from punisher.cluster.peer_server import PeerServer
from punisher.cluster.client_server import RedisClientServer


class Punisher(object):
    """ punisher server """

    def __init__(self,
                 client_address=('', 6379),
                 peer_address=('', 4379),
                 token=None,
                 seed_peers=None,
                 name=None,
                 node_id=None,
                 replication_factor=3,
                 cluster_status=Cluster.Status.INITIALIZING):
        super(Punisher, self).__init__()

        self.client_address = client_address
        self.peer_address = peer_address
        #todo: load some config
        self.name = name
        self.local_node = LocalNode(
            address=self.peer_address,
            node_id=node_id,
            name=name,
            token=token,
        )

        self.seed_peers = seed_peers
        self.replication_factor = replication_factor
        self.cluster = Cluster(
            self.local_node,
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

    @property
    def node_id(self):
        return self.local_node.node_id

    @property
    def store(self):
        return self.local_node.store

    def start(self):
        self.peer_server.start()
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



