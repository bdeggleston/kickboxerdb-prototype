from punisher.cluster.BaseNode import BaseNode

from punisher.cluster.Cluster import Cluster
from punisher.cluster.RemoteNode import RemoteNode


class LocalNode(BaseNode):

    def __init__(self,
                 client_address=('', 6379),
                 peer_address=('', 4379),
                 seed_peers=None,
                 replication_factor=3,
                 node_id=None,
                 name=None,
                 token=None):
        """
        :param client_address:
        :param peer_address:
        :param seed_peers:
        :param replication_factor:
        :param node_id:
        :param name:
        :param token:
        """
        super(LocalNode, self).__init__(node_id, name, token)
        self.client_address = client_address
        self.peer_address = peer_address
        self.seed_peers = seed_peers or []
        self.replication_factor = replication_factor
