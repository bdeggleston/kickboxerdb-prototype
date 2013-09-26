import gevent
from kickboxer.cluster.cluster import Cluster

from kickboxer.tests.base import BaseNodeTestCase, LiteralPartitioner


class TokenChangeIntegrationTest(BaseNodeTestCase):

    def test_changing_node_token(self):
        """
        tests changing a node's token streams keys properly

        changing the tokens from this:
        N0      N1      N2      N3      N4      N5      N6      N7      N8      N9
        [00    ][10    ][20    ][30    ][40    ][50    ][60    ][70    ][80    ][90    ]
                 --> --> --> --> --> --> --> --> --> --> -->|
        to this:
        N0              N2      N3      N4      N5      N6  N1* N7      N8      N9
        [00            ][20    ][30    ][40    ][50    ][60][65][70    ][80    ][90    ]

        N0 should now control N1's old tokens, and N1 should control half of N6's tokens

        It seems like the best thing to do is have N1 stream it's data to N0, and N6 stream
        it's data to N1, or to have each displaced node stream data from both adjacent nodes

        The options for streaming nodes are:
            * Both displaced nodes stream data from both adjacent nodes
            * N0 streams data from N2, and N1 streams data from N6
                * If a preceding node's id has not changed, it should stream data
                  from the next node, if the preceding node's id has changed, it
                  should stream data from the preceding node
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

        # change the token
        n1.cluster.change_token(6500)

        # wait for streaming to complete
        if Cluster.Status.STREAMING in [n0.cluster.status, n1.cluster.status]:
            gevent.sleep(0)

        self.assertNotEqual(n0.cluster.status, Cluster.Status.STREAMING)
        self.assertNotEqual(n1.cluster.status, Cluster.Status.STREAMING)

        # check the keys for n0
        expected = {k for k, v in total_data.items() if 1000 <= int(k) < 2000 }
        all_keys = n1.store.all_keys()
        for key in expected:
            self.assertIn(key, all_keys)

        # check the keys for n1
        expected = {k for k, v in total_data.items() if 4000 <= int(k) < 8000 }
        all_keys = n1.store.all_keys()
        for key in expected:
            self.assertIn(key, all_keys)

    def test_minor_token_change(self):
        """
        When changing the token ring from this:
        N0      N1      N2      N3      N4      N5      N6      N7      N8      N9
        [00    ][10    ][20    ][30    ][40    ][50    ][60    ][70    ][80    ][90    ]

        to this:
        N0         N1   N2      N3      N4      N5      N6      N7      N8      N9
        [00        [15][20    ][30    ][40    ][50    ][60    ][70    ][80    ][90    ]
                |-|------->
            not required ^^
        N2 will be responsible for more data from N0's token space, but it should
        already have it from replicating N1

        :return:
        """
