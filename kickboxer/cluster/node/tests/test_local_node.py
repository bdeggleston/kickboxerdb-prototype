from datetime import datetime
from unittest import TestCase

from kickboxer.cluster.node.local import LocalNode
from kickboxer.tests.base import MockStore


class LocalNodeTests(TestCase):

    def setUp(self):
        super(LocalNodeTests, self).setUp()
        self.store = MockStore()
        self.node = LocalNode(self.store)

    def test_retrieval_instruction(self):
        """ tests that read instructions pass the proper arguments to the store """
        self.node.execute_retrieval_instruction('get', 'a', ['b', 'c'])
        self.assertEqual(self.store.get.call_count, 1)
        call_args = self.store.get.call_args
        self.assertEqual(call_args[0], ('a', 'b', 'c'))
        self.assertEqual(call_args[1], {})

    def test_mutation_instruction(self):
        """ tests that write instructions pass the proper arguments to the store """
        ts = datetime(1982, 3, 9)
        self.node.execute_mutation_instruction('set', 'a', ['b', 'c'], ts)
        self.assertEqual(self.store.set.call_count, 1)
        call_args = self.store.set.call_args
        self.assertEqual(call_args[0], ('a', 'b', 'c'))
        self.assertEqual(call_args[1], {'timestamp': ts})
