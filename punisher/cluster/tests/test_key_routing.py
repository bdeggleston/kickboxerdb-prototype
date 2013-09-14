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
            self.assertEqual(self.nodes[i].token, 1000 * i)

