from punisher.tests.base import BaseNodeTestCase


class InitializationIntegrationTests(BaseNodeTestCase):

    def test_data_is_properly_transferred_on_initialization(self):
        """
        Tests that a node joining an existing cluster properly
        transfers data from existing nodes
        """
        self.create_nodes(10)
        for node in self.nodes:
            node.start()

        for i in range(500):
            pass
