import random
import string
from unittest import skip

import gevent

from punisher.cluster.cluster import Cluster
from punisher.tests.base import BaseNodeTestCase, LiteralPartitioner


def random_string(size=6):
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(size))


class InitializationIntegrationTests(BaseNodeTestCase):

    def test_initialization_with_linear_partitioner(self):
        """
        Tests that a node joining an existing cluster properly
        transfers data from existing nodes
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

        new_node = self.create_node(
            cluster_status=Cluster.Status.INITIALIZING,
            token=5500,
            partitioner=LiteralPartitioner()
        )
        new_node.start()

        # new node is responsible for 5500 - 5999
        # and replicates 4000 - 5499
        expected_data = {k: v for k, v in total_data.items() if 4000 <= int(k) < 6000}
        store_data = new_node.store._data
        self.assertEquals(len(store_data.keys()), len(expected_data.keys()))
        self.assertEquals(set(store_data.keys()), set(expected_data.keys()))
        for key in expected_data.keys():
            expected = expected_data[key]
            actual = store_data[key]
            self.assertEqual(expected, actual.data)

    @skip('deprecated')
    def test_remote_key_retiring(self):
        """ tests that the proper remote keys are removed after node initialization """
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

        # saturate tokens in the range to be retired
        for i in range(700):
            node = self.nodes[i % len(self.nodes)]
            key = str(5400 + i)
            val = str(i)
            node.cluster.execute_mutation_instruction('set', key, [val], synchronous=True)
        gevent.sleep(0)

        new_node = self.create_node(
            cluster_status=Cluster.Status.INITIALIZING,
            token=5500,
            partitioner=LiteralPartitioner()
        )
        new_node.start()
        new_node.cluster._initializer.join()

        # new node is responsible for 5500 - 5999
        # and replicates 4000 - 5499
        # 5500 - 5999 should be retired on node[5]
        # 5000 - 5499 should be retired on node[7]

        # test previous owner
        node = self.nodes[5]
        self.assertEqual(node.token, 5000)
        max_token = max([int(k) for k in node.cluster.store._data.keys()])
        self.assertLess(max_token, 5500)

        # test previous replica
        node = self.nodes[7]
        self.assertEqual(node.token, 7000)
        min_token = min([int(k) for k in node.cluster.store._data.keys()])
        self.assertGreaterEqual(min_token, 5500)

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

    def test_data_is_properly_reconciled_on_initialization(self):
        """
        tests that a node joining an existing cluster properly
        reconciles conflicting data
        """

    def test_data_is_retired_after_initialization(self):
        """ tests that data taken by a new node is removed if a node no longer uses it """


class InitializationNodeQueryTest(BaseNodeTestCase):
    pass