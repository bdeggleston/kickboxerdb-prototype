import gevent

from mock import patch
from kickboxer.cluster.tests.base import BaseClusterModificationTest


class NodeRemovalIntegrationTest(BaseClusterModificationTest):

    def test_abrupt_node_removal(self):
        """
        tests removing a node redistributes keys properly when the removed node is not reachable

        expected behavior:

        removing N1
        N0      N1      N2      N3      N4      N5      N6      N7      N8      N9
        [0     ][10    ][20    ][30    ][40    ][50    ][60    ][70    ][80    ][90    ]
                |xxxxxx|

        N0              N2      N3      N4      N5      N6      N7      N8      N9
        [0             ][20    ][30    ][40    ][50    ][60    ][70    ][80    ][90    ]
                 <------|------|
        """
        # remove N1, and verify that data is streamed from n2
        n2 = self.nodes[2]
        with patch.object(n2.cluster, 'stream_to_node', wraps=n2.cluster.stream_to_node) as stream_to_node:
            self.n1.stop()
            gevent.sleep(0)
            self.nodes[5].cluster.remove_node(self.n1.node_id)
            # self.n1.cluster.remove_node()
            self.block_while_streaming()
        self.assertEqual(stream_to_node.call_count, 1)

        # check the keys for n0, check the keys in sorted order, to make it easier
        # to understand where problems started in the streaming logic
        expected = sorted(list({int(k) for k, v in self.total_data.items() if 1000 <= int(k) < 2000}))
        all_keys = self.n1.store.all_keys()
        for key in expected:
            self.assertIn(str(key), all_keys)

    def test_clean_node_removal(self):
        """
        tests removing a node redistributes keys properly when the removed node is not reachable

        expected behavior:

        removing N1
        N0      N1      N2      N3      N4      N5      N6      N7      N8      N9
        [0     ][10    ][20    ][30    ][40    ][50    ][60    ][70    ][80    ][90    ]
                |xxxxxx|
        to this:
        N0              N2      N3      N4      N5      N6      N7      N8      N9
        [0             ][20    ][30    ][40    ][50    ][60    ][70    ][80    ][90    ]
                 ^^^^^^
                [10xxxx]
        """
        # remove N1, and verify that data is streamed from n2
        with patch.object(self.n1.cluster, 'stream_to_node', wraps=self.n1.cluster.stream_to_node) as stream_to_node:
            self.n1.cluster.remove_node()
            self.block_while_streaming()
        self.assertEqual(stream_to_node.call_count, 1)

        # check the keys for n0, check the keys in sorted order, to make it easier
        # to understand where problems started in the streaming logic
        expected = sorted(list({int(k) for k, v in self.total_data.items() if 1000 <= int(k) < 2000}))
        all_keys = self.n1.store.all_keys()
        for key in expected:
            self.assertIn(str(key), all_keys)
