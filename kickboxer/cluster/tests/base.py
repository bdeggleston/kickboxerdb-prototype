import gevent
from kickboxer.tests.base import BaseNodeTestCase, LiteralPartitioner


class BaseClusterModificationTest(BaseNodeTestCase):
    """
    sets up a cluster with a literal partitioner and data
    """

    def setUp(self):
        super(BaseClusterModificationTest, self).setUp()
        num_nodes = 10
        tokens = [1000 * i for i in range(num_nodes)]
        self.create_nodes(num_nodes, tokens=tokens, partitioner=LiteralPartitioner())
        self.start_cluster()

        # add a bunch of data
        self.total_data = {}
        for i in range(500):
            node = self.nodes[i % len(self.nodes)]
            key = str(i * 20)
            val = str(i)
            node.cluster.execute_mutation_instruction('set', key, [val], synchronous=True)
            self.total_data[key] = val
        gevent.sleep(0)

        # sanity checks
        self.n0 = self.nodes[0]
        self.n1 = self.nodes[1]
        for key in self.n0.store.all_keys():
            assert int(key) < 1000 or int(key) >= 8000, key
        for key in self.n1.store.all_keys():
            assert int(key) < 4000 or int(key) >= 9000, key
