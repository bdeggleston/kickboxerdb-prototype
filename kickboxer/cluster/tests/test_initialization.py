import random
import string

import gevent

from kickboxer.cluster.cluster import Cluster
from kickboxer.cluster.tests.base import BaseClusterModificationTest
from kickboxer.tests.base import LiteralPartitioner, BaseNodeTestCase


def random_string(size=6):
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(size))


class InitializationIntegrationTests(BaseClusterModificationTest):

    def test_initialization_with_linear_partitioner(self):
        """
        Tests that a node joining an existing cluster properly
        transfers data from existing nodes
        """
        new_node = self.create_node(
            cluster_status=Cluster.Status.INITIALIZING,
            token=5500,
            partitioner=LiteralPartitioner()
        )
        new_node.start()

        # new node is responsible for 5500 - 5999
        # and replicates 4000 - 5499
        expected_data = {k: v for k, v in self.total_data.items() if 4000 <= int(k) < 6000}
        store_data = new_node.store._data
        self.assertEquals(len(store_data.keys()), len(expected_data.keys()))
        self.assertEquals(set(store_data.keys()), set(expected_data.keys()))
        for key in expected_data.keys():
            expected = expected_data[key]
            actual = store_data[key]
            self.assertEqual(expected, actual.data)


class MD5InitializationTests(BaseNodeTestCase):

    def test_data_is_properly_transferred_on_initialization(self):
        """
        Tests that a node joining an existing cluster properly
        transfers data from existing nodes
        """
        self.create_nodes(10)
        for node in self.nodes:
            node.start()
        gevent.sleep(0)

        # add a bunch of data
        total_data = {}
        for i in range(500):
            node = self.nodes[i % len(self.nodes)]
            key = random_string()
            val = random_string()
            node.cluster.execute_mutation_instruction('set', key, [val], synchronous=True)
            total_data[key] = val

        new_node = self.create_node(cluster_status=Cluster.Status.INITIALIZING)
        new_node.start()

        expected_data = {k: v for k, v in total_data.items() if new_node.replicates_key(k)}
        store_data = new_node.store._data
        self.assertEquals(set(store_data.keys()), set(expected_data.keys()))
        for key in expected_data.keys():
            expected = expected_data[key]
            actual = store_data[key]
            self.assertEqual(expected, actual.data)

