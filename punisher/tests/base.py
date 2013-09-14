import time
from unittest import TestCase

from punisher.cluster.cluster import Cluster
from punisher.partitioner.md5 import MD5Partitioner
from punisher.server import Punisher


class BaseNodeTestCase(TestCase):

    def setUp(self):
        super(BaseNodeTestCase, self).setUp()
        self.nodes = []
        self.next_port = 4379

    def tearDown(self):
        super(BaseNodeTestCase, self).tearDown()
        if self.nodes:
            for node in self.nodes:
                try:
                    node.stop()
                except Exception:
                    pass
            while any([n.peer_server.started for n in self.nodes]):
                time.sleep(0.1)

    _default_partitioner=MD5Partitioner()

    def create_node(self,
                    seeds=None,
                    node_id=None,
                    cluster_status=Cluster.Status.NORMAL,
                    token=None,
                    partitioner=_default_partitioner):
        port = self.next_port
        self.next_port += 1
        if not seeds:
            is_online = lambda p: p.cluster.is_online
            seeds = [n for n in self.nodes if is_online(n)] or self.nodes
            seeds = [seeds[0].peer_address] if seeds else None

        node = Punisher(
            client_address=None,
            peer_address=('localhost', port),
            seed_peers=seeds,
            name='Node{}'.format(port),
            node_id=node_id,
            cluster_status=cluster_status,
            token=token,
            partitioner=partitioner
        )
        self.nodes.append(node)
        return node

    def create_nodes(self,
                     num_nodes,
                     cluster_status=Cluster.Status.NORMAL,
                     tokens=None,
                     partitioner=_default_partitioner):
        if tokens:
            assert len(tokens) == num_nodes
        else:
            tokens = [None] * num_nodes
        return [self.create_node(cluster_status=cluster_status,
                                 token=tokens[i],
                                 partitioner=partitioner)
                for i in range(num_nodes)]

