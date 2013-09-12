from datetime import datetime, timedelta
import time

from punisher.tests.base import BaseNodeTestCase


class BaseClusteredStorageTest(BaseNodeTestCase):
    pass


class GetTest(BaseClusteredStorageTest):

    def test_get_agreed_upon_value(self):
        """ Tests getting a value when all nodes agree on the value """
        """
        Traceback (most recent call last):
          File "/Users/bdeggleston/.workon/punisher1/lib/python2.7/site-packages/gevent/greenlet.py", line 390, in run
            result = self._run(*self.args, **self.kwargs)
          File "/Users/bdeggleston/code/punisher1/punisher/cluster/cluster.py", line 318, in _execute
            result = node.execute_retrieval_instruction(instruction, key, args)
          File "/Users/bdeggleston/code/punisher1/punisher/cluster/node/remote.py", line 159, in execute_retrieval_instruction
            assert isinstance(response, messages.RetrievalValueResponse)
        AssertionError
        <Greenlet at 0x10c6c1050: _execute(<punisher.cluster.node.remote.RemoteNode object at)> failed with AssertionError
        """
        num_nodes = 10
        key = 'a'
        self.create_nodes(num_nodes)
        ts = datetime.utcnow()

        for node in self.nodes:
            node.start()
        time.sleep(0.1)

        #check replication responsibility
        expected_replicas = {n.node_id for n in self.nodes if n.replicates_key(key)}
        for node in self.nodes:
            replicas = {n.node_id for n in node.cluster.get_nodes_for_key(key)}
            self.assertEqual(replicas, expected_replicas)

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
        """
        Traceback (most recent call last):
          File "/Users/bdeggleston/.workon/punisher1/lib/python2.7/site-packages/gevent/greenlet.py", line 390, in run
            result = self._run(*self.args, **self.kwargs)
          File "/Users/bdeggleston/code/punisher1/punisher/cluster/cluster.py", line 318, in _execute
            result = node.execute_retrieval_instruction(instruction, key, args)
          File "/Users/bdeggleston/code/punisher1/punisher/cluster/node/remote.py", line 159, in execute_retrieval_instruction
            assert isinstance(response, messages.RetrievalValueResponse)
        AssertionError
        <Greenlet at 0x10cf95cd0: _execute(<punisher.cluster.node.remote.RemoteNode object at)> failed with AssertionError
        """
        num_nodes = 10
        self.create_nodes(num_nodes)
        ts = datetime.utcnow()
        key = 'a'

        for node in self.nodes:
            node.start()
        time.sleep(0.01)

        # set the values in the different node stores
        expected = None
        latest_val = None
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
            val = node.cluster.store.get(key)
            if node.replicates_key(key):
                self.assertEqual(val, expected)
            else:
                self.assertIsNone(val)

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
        self.create_nodes(num_nodes)
        for node in self.nodes:
            node.start()
        time.sleep(0.01)

        # sanity check
        for node in self.nodes:
            self.assertIsNone(node.cluster.store.get('a'))

        # set value
        node0 = [n for n in self.nodes if not n.replicates_key('a')][0]
        node0.cluster.execute_mutation_instruction('set', 'a', ['b'])

        # give a sec for update to propagate to peers
        time.sleep(0.01)

        node0 = [n for n in self.nodes if n.replicates_key('a')][0]
        expected = node0.cluster.store.get('a')
        self.assertIsNotNone(expected)
        self.assertEquals(expected.data, 'b')

        for node in self.nodes:
            val = node.cluster.store.get('a')
            if node.replicates_key('a'):
                self.assertEquals(val, expected)
            else:
                self.assertIsNone(val)


class DeleteTest(BaseClusteredStorageTest):

    def test_delete_is_distributed(self):
        pass
