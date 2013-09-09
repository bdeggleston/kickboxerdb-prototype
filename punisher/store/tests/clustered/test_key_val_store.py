from datetime import datetime, timedelta
import time

from punisher.tests.base import BaseNodeTestCase


class BaseClusteredStorageTest(BaseNodeTestCase):
    pass


class GetTest(BaseClusteredStorageTest):

    def test_get_agreed_upon_value(self):
        """ Tests getting a value when all nodes agree on the value """
        num_nodes = 10
        key = 'a'
        self._create_nodes(num_nodes)
        ts = datetime.utcnow()

        for node in self.nodes:
            node.start()
        time.sleep(0.01)

        for node in self.nodes:
            if not node.replicates_key(key): continue
            node.cluster.store.set(key, 'b', timestamp=ts)

        # test all of them
        for node in self.nodes:
            val = node.cluster.execute_retrieval_instruction('get', key, [])
            self.assertEquals(val, 'b')

    def test_get_contested_values(self):
        """
        Tests getting a value when all nodes agree on the value
        but have different timestamps on the value.

        returned value should be a result of reconciling the different
        values across the cluster, and the nodes with out of date data
        should be updated
        """
        num_nodes = 10
        self._create_nodes(num_nodes)
        ts = datetime.utcnow()
        key = 'a'

        for node in self.nodes:
            node.start()
        time.sleep(0.01)

        expected = None
        latest_val = None
        latest_ts = None
        num = 0
        for node in self.nodes:
            if not node.replicates_key(key): continue
            latest_val = str(num)
            latest_ts = ts + timedelta(seconds=num)
            node.cluster.store.set(key, latest_val, timestamp=latest_ts)
            expected = node.cluster.store.get(key)

        # test all of them
        node0 = [n for n in self.nodes if not n.replicates_key(key)][0]
        val = node0.cluster.execute_retrieval_instruction('get', key, [])
        self.assertEquals(val, latest_val)

        # give a sec for cluster reconciliation
        time.sleep(0.01)

        # check that the cluster reconciled the contested value
        for node in self.nodes:
            if not node.replicates_key(key): continue
            val = node.cluster.store.get(key)
            self.assertEqual(val, expected)

    def test_unknown_value(self):
        pass

    def test_value_resolution(self):
        pass


class SetTests(BaseClusteredStorageTest):

    def test_value_is_distributed(self):
        """
        Tests that setting a value on one node distributes the
        value to it's peers
        """
        num_nodes = 10
        self._create_nodes(num_nodes)
        for node in self.nodes:
            node.start()
        time.sleep(0.01)

        # sanity check
        for node in self.nodes:
            self.assertIsNone(node.store.get('a'))

        # set value
        node0 = self.nodes[0]
        node0.execute_store_instruction('set', 'a', ['b'])
        expected = node0.store.get('a')

        # give a sec for update to propagate to peers
        time.sleep(0.01)

        for node in self.nodes:
            val = node.store.get('a')
            self.assertEquals(val, expected)


class DeleteTest(BaseClusteredStorageTest):

    def test_delete_is_distributed(self):
        pass
