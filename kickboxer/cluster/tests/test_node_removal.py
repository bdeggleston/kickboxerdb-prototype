import gevent
from kickboxer.cluster.cluster import Cluster

from kickboxer.tests.base import BaseNodeTestCase, LiteralPartitioner


class NodeRemovalIntegrationTest(BaseNodeTestCase):

    def test_node_removal(self):
        """
        tests removing a node redistributes keys properly

        removing N1
        N0      N1      N2      N3      N4      N5      N6      N7      N8      N9
        [0     ][10    ][20    ][30    ][40    ][50    ][60    ][70    ][80    ][90    ]
                |xxxxxx|
        to this:
        N0              N2      N3      N4      N5      N6      N7      N8      N9
        [0             ][20    ][30    ][40    ][50    ][60    ][70    ][80    ][90    ]

        N0 should now control N1's old tokens

        N0 should stream data from N2

        If a node
        """
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

        # sanity checks
        n0 = self.nodes[0]
        n1 = self.nodes[1]

        for key in n0.store.all_keys():
            assert int(key) < 1000 or int(key) >= 8000, key

        for key in n1.store.all_keys():
            assert int(key) < 4000 or int(key) >= 9000, key

        # remove N1
        n1.cluster.remove_node()

        # wait for streaming to complete
        if Cluster.Status.STREAMING in [n0.cluster.status, n1.cluster.status]:
            gevent.sleep(0)

        self.assertNotEqual(n0.cluster.status, Cluster.Status.STREAMING)
        self.assertNotEqual(n1.cluster.status, Cluster.Status.STREAMING)

        # check the keys for n0, check the keys in sorted order, to make it easier
        # to understand where problems started in the streaming logic
        expected = sorted(list({int(k) for k, v in total_data.items() if 1000 <= int(k) < 2000}))
        all_keys = n1.store.all_keys()
        for key in expected:
            self.assertIn(str(key), all_keys)

