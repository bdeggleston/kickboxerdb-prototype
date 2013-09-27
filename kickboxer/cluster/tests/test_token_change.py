import gevent
from kickboxer.cluster.cluster import Cluster
from kickboxer.cluster.tests.base import BaseClusterModificationTest

class TokenChangeIntegrationTest(BaseClusterModificationTest):

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
        # change the token
        self.n1.cluster.change_token(6500)

        # wait for streaming to complete
        if Cluster.Status.STREAMING in [self.n0.cluster.status, self.n1.cluster.status]:
            gevent.sleep(0)

        self.assertNotEqual(self.n0.cluster.status, Cluster.Status.STREAMING)
        self.assertNotEqual(self.n1.cluster.status, Cluster.Status.STREAMING)

        # check that all nodes have the correct token
        for i, node in enumerate(self.nodes):
            if i == 1:
                self.assertEquals(node.token, 6500)
            else:
                self.assertEquals(node.token, i * 1000)

        # check the keys for n0
        expected = {k for k, v in self.total_data.items() if 1000 <= int(k) < 2000}
        all_keys = self.n1.store.all_keys()
        for key in expected:
            self.assertIn(key, all_keys)

        # check the keys for n1, check the keys in sorted order, to make it easier
        # to understand where problems started in the streaming logic
        expected = sorted(list({int(k) for k, v in self.total_data.items() if 5000 <= int(k) < 7000}))
        all_keys = self.n1.store.all_keys()
        for key in expected:
            self.assertIn(str(key), all_keys)

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
        """
