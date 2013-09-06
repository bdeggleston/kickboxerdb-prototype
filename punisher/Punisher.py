from punisher.cluster.LocalNode import LocalNode
from punisher.cluster.Cluster import Cluster


class Punisher(object):
    """ punisher server """

    def __init__(self,
                 client_address=('', 6379),
                 peer_address=('', 4379),
                 token=None,
                 seed_peers=None,
                 replication_factor=3):
        super(Punisher, self).__init__()

        self.client_address = client_address
        self.peer_address = peer_address
        #todo: load some config
        self.local_node = LocalNode(address=self.peer_address)
        self.cluster = Cluster(self.local_node, self.replication_factor)


