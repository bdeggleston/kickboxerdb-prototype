import random
import string

import gevent

from punisher.cluster.cluster import Cluster
from punisher.tests.base import BaseNodeTestCase


def random_string(size=6):
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(size))


class InitializationIntegrationTests(BaseNodeTestCase):

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
        new_node.cluster._initializer.join()

        expected_data = {k: v for k, v in total_data.items() if new_node.replicates_key(k)}
        store_data = new_node.store._data
        self.assertEquals(set(store_data.keys()), set(expected_data.keys()))
        self.assertEquals(store_data, expected_data)

    def test_data_is_properly_reconciled_on_initialization(self):
        """
        tests that a node joining an existing cluster properly
        reconciles conflicting data
        """

    def test_data_is_retired_after_initialization(self):
        """ tests that data taken by a new node is removed if a node no longer uses it """
