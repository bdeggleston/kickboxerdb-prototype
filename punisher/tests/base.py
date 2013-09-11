import time
from unittest import TestCase

from punisher.cluster.Cluster import Cluster
from punisher.Punisher import Punisher


class BaseNodeTestCase(TestCase):

    def setUp(self):
        super(BaseNodeTestCase, self).setUp()
        self.nodes = []
        self.next_port = 4379

    def tearDown(self):
        super(BaseNodeTestCase, self).tearDown()
        for node in self.nodes:
            try:
                node.stop()
            except Exception:
                pass
        time.sleep(0.01)

    def _create_node(self, seeds=None, node_id=None, cluster_status=Cluster.Status.NORMAL):
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
            cluster_status=cluster_status
        )
        self.nodes.append(node)
        return node

    def _create_nodes(self, num, cluster_status=Cluster.Status.NORMAL):
        return [self._create_node(cluster_status=cluster_status) for i in range(num)]

