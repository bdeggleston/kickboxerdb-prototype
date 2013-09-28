from kickboxer.cluster.cluster import Cluster
from kickboxer.tests.base import BaseNodeTestCase, LiteralPartitioner, MockLocalNode, MockRemoteNode


class StreamingQueryTest(BaseNodeTestCase):
    """
    tests querying nodes that are receiving streaming data
    forward queries to the streaming node
    """

    def setUp(self):
        super(StreamingQueryTest, self).setUp()
        self.local_node = MockLocalNode()
        self.remote_node = MockRemoteNode()
        self.cluster = Cluster(self.local_node, LiteralPartitioner())
        self.cluster.status = Cluster.Status.STREAMING
        self.cluster._streaming_node = self.remote_node

    def test_read_routing(self):
        """ reads should be routed directly to the streaming nodes """
        self.cluster.route_local_retrieval_instruction('get', 'a', ['b'])
        assert self.local_node.store.get.call_count == 0
        assert self.remote_node.execute_retrieval_instruction.call_count == 1

    def test_write_routing(self):
        """ writes should be routed to the streaming node and mirrored to the local node """
        self.cluster.route_local_mutation_instruction('set', 'a', ['b'], 0)
        assert self.local_node.store.set.call_count == 1
        assert self.remote_node.execute_mutation_instruction.call_count == 1


class StreamingTest(StreamingQueryTest):
    """
    tests different streaming scenarios
    """

    def test_data_from_non_src_node_fails(self):
        """ data should not be accepted from nodes we're not streaming from  """