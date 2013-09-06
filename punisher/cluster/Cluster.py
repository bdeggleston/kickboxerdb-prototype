from punisher.cluster.LocalNode import LocalNode

__author__ = 'bdeggleston'


class Cluster(object):
    """
    Maintains the local view of the cluster, and coordinates client requests, and
    is responsible for handling replication and routing
    """

    def __init__(self, local_node, seed_peers=None, replication_factor=3):
        super(Cluster, self).__init__()
        self.seed_peers = seed_peers or []
        self.replication_factor = replication_factor

        assert isinstance(local_node, LocalNode)
        self.local_node = local_node
        self.peers = {self.local_node.token: self.local_node}

        # cluster token data
        self.min_token = None
        self.max_token = None

    def __contains__(self, item):
        return item in self.peers

    def add_node(self, node_id, address, name):
        pass

    def remove_node(self, node):
        pass

    def _refresh_ring(self):
        pass

    def execute_retrieval_instruction(self, instruction, key, args):
        pass

    def execute_mutation_instruction(self, instruction, key, args, timestamp):
        pass
