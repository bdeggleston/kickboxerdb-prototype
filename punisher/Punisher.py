from punisher.cluster.LocalNode import LocalNode
from punisher.cluster.Cluster import Cluster
from punisher.cluster.PeerServer import PeerServer
from punisher.cluster.ClientServer import RedisClientServer


class Punisher(object):
    """ punisher server """

    def __init__(self,
                 client_address=('', 6379),
                 peer_address=('', 4379),
                 token=None,
                 seed_peers=None,
                 name=None,
                 node_id=None,
                 replication_factor=3):
        super(Punisher, self).__init__()

        self.client_address = client_address
        self.peer_address = peer_address
        #todo: load some config
        self.name = name
        self.node_id = node_id
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
            replication_factor=self.replication_factor
        )
        self.peer_server = PeerServer(
            self.peer_address,
            cluster=self.cluster
        )
        self.client_server = RedisClientServer(
            self.client_address,
            cluster=self.cluster) if self.client_address else None

    def start(self):
        self.peer_server.start()
        self.cluster.start()
        if self.client_server: self.client_server.start()

    def stop(self):
        self.peer_server.stop()
        self.cluster.stop()
        if self.client_server: self.client_server.stop()

    def kill(self):
        self.peer_server.kill()
        self.cluster.kill()
        if self.client_server: self.client_server.kill()



