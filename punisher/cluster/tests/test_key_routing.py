import gevent

from punisher.tests.base import BaseNodeTestCase, LiteralPartitioner


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

    def test_clustered_key_distribution(self):
        """ Tests that keys are properly routed to their owners and replicas in a cluster """
        num_nodes = 10
        tokens = [1000 * i for i in range(num_nodes)]
        self.create_nodes(num_nodes, tokens=tokens, partitioner=LiteralPartitioner())
        self.start_cluster()

        # add a bunch of data
        total_data = {}
        for i in range(500):
            node = self.nodes[i % len(self.nodes)]
            key = str(i * 20)
            val = str(i)
            node.cluster.execute_mutation_instruction('set', key, [val], synchronous=True)
            total_data[key] = val
        gevent.sleep(0)

        # sanity check data distribution
        # assuming a replication factor of 3, each node should
        # replicate the data from it's two predecessor nodes
        for i in range(len(self.nodes)):
            node = self.nodes[i]
            self.assertEqual(len(node.cluster.store._data), 50 * 3)
            max_token = node.token + 1000 - 20
            min_token = self.nodes[(i - (node.cluster.replication_factor - 1)) % len(self.nodes)].token
            keys = {int(k) for k in node.cluster.store._data.keys()}
            if min_token < max_token:
                print i
                self.assertEqual(min(keys), min_token)
                self.assertEqual(max(keys), max_token)
            else:
                self.assertEqual(min(keys), 0)
                self.assertIn(min_token, keys)
                self.assertNotIn(min_token - 20, keys)

                self.assertEqual(max(keys), (10000 - 20))
                self.assertIn(max_token, keys)
                self.assertNotIn((max_token + 20), keys)

