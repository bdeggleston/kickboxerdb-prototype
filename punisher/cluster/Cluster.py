__author__ = 'bdeggleston'


class Cluster(object):
    """
    Maintains the local view of the cluster, and routes requests between them
    """

    def __init__(self):
        super(Cluster, self).__init__()
        self.min_token = None
        self.max_token = None

    def add_node(self, node):
        pass

    def remove_node(self, node):
        pass

    def _refresh_ring(self):
        pass

    def execute_retrieval_instruction(self, instruction, key, args):
        pass

    def execute_mutation_instruction(self, instruction, key, args, timestamp):
        pass
