import gevent

from punisher.partitioner.base import BasePartitioner
from punisher.tests.base import BaseNodeTestCase


class LiteralPartitioner(BasePartitioner):
    """
    returns the number passed into the key,
    keys can only be stringified ints
    """
    max_token = 10000

    @classmethod
    def get_key_token(cls, key):
        return int(key)


class KeyRoutingTest(BaseNodeTestCase):

    def test_key_routing(self):
        num_nodes = 10
        tokens = [1000 * i for i in range(num_nodes)]
        self.create_nodes(num_nodes, tokens=tokens, partitioner=LiteralPartitioner())

        for node in self.nodes:
            node.start()
        gevent.sleep(0)

        # sanity check
        for i in range(10):
            self.assertEqual(self.nodes[i].cluster.replication_factor, 3)
            self.assertEqual(self.nodes[i].token, 1000 * i)

        # test data
        fixture_data = [
            {'token':'0', 'expected': self.nodes[0:3]},
            {'token':'999', 'expected': self.nodes[0:3]},
            {'token':'9000', 'expected': [self.nodes[-1]] + self.nodes[0:2]},
            {'token':'9001', 'expected': [self.nodes[-1]] + self.nodes[0:2]},
            {'token':'9999', 'expected': [self.nodes[-1]] + self.nodes[0:2]},
            {'token':'5000', 'expected': self.nodes[5:8]},
            {'token':'5555', 'expected': self.nodes[5:8]},
            {'token':'5999', 'expected': self.nodes[5:8]},
        ]

        # test get nodes for token
        for fixture in fixture_data:
            for i, node in enumerate(self.nodes):
                nodes = node.cluster.get_nodes_for_key(fixture['token'])
                self.assertEqual(len(nodes), len(fixture['expected']))
                self.assertEqual([n.node_id for n in nodes], [n.node_id for n in fixture['expected']])

