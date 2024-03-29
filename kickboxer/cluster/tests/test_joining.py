from unittest.case import TestCase

import gevent
from kickboxer.cluster.cluster import Cluster

from kickboxer.tests.base import BaseNodeTestCase
from kickboxer.cluster.node.remote import RemoteNode


class ClusterStartupTest(BaseNodeTestCase):

    def test_converging_seed_addresses(self):
        """
        Tests that multiple seed addresses pointing to a single server
        are resolved to a single peer
        """
        n1, n2 = self.create_nodes(2)
        n1.start()
        n2.start()
        gevent.sleep(0.01)
        assert len(n2.cluster) == 2
        assert n1.node_id in n2.cluster


class ClusterTest(BaseNodeTestCase):

    def test_two_way_peer_connection_is_established(self):
        """
        Tests that
        """
        n1, n2 = self.create_nodes(2)
        n1.start()
        n2.start()
        gevent.sleep(0.01)

        assert n1.node_id in n2.cluster
        assert n2.node_id in n1.cluster

    def test_stopping_peer(self):
        """
        """
        node0 = self.create_nodes(10)[0]
        for node in self.nodes:
            node.start()

        gevent.sleep(0.01)

        # check that all the nodes know about each other
        for node in self.nodes:
            self.assertEqual(len(node.cluster), len(self.nodes))
            for peer in self.nodes:
                if node.node_id == peer.node_id: continue
                self.assertIn(peer.node_id, node.cluster)
                peer_node = node.cluster.get_node(peer.node_id)
                self.assertEquals(peer_node.status, RemoteNode.Status.UP)

        node0.stop()
        gevent.sleep(0.01)

        # ping all of them
        for node in self.nodes:
            if node.node_id == node0.node_id: continue
            for peer in node.cluster.nodes.values():
                peer.ping()
        gevent.sleep(0.01)

        # check that the nodes know that node0 is disconnected after
        # trying to send them a message
        for node in self.nodes:
            if node0.node_id == node.node_id: continue
            peer = node.cluster.get_node(node0.node_id)
            self.assertEquals(peer.status, RemoteNode.Status.DOWN)

    def test_rejoining_cluster_after_stop(self):
        """
        """
        node0 = self.create_nodes(10)[0]
        for node in self.nodes:
            node.start()

        gevent.sleep(0.01)

        # check that all the nodes know about each other
        for node in self.nodes:
            self.assertEqual(len(node.cluster), len(self.nodes))
            for peer in self.nodes:
                if node.node_id == peer.node_id: continue
                self.assertIn(peer.node_id, node.cluster)

        for node in self.nodes:
            if node.node_id == node0.node_id: continue
            self.assertIn(node0.node_id, node.cluster)
            peer = node.cluster.get_node(node0.node_id)
            self.assertEquals(peer.status, RemoteNode.Status.UP)

        node0.stop()
        gevent.sleep(0.01)

        # ping all of them
        for node in self.nodes:
            if node.node_id == node0.node_id: continue
            for peer in node.cluster.nodes.values():
                peer.ping()
        gevent.sleep(0.01)

        # check that the nodes know that node0 is disconnected
        for node in self.nodes:
            if node.node_id == node0.node_id: continue
            peer = node.cluster.get_node(node0.node_id)
            self.assertEquals(peer.status, RemoteNode.Status.DOWN)

        # restart the node
        node0.start()
        gevent.sleep(0.05)

        for node in self.nodes:
            if node.node_id == node0.node_id: continue
            self.assertIn(node0.node_id, node.cluster)
            peer = node.cluster.get_node(node0.node_id)
            self.assertEquals(peer.status, RemoteNode.Status.UP)
            self.assertIn(node0.node_id, node.cluster)


class PeerDiscoveryTests(BaseNodeTestCase):

    def test_uniform_peer_discovery(self):
        """ tests that all peers discover each other when starting up """
        self.create_nodes(10)
        expected = {n.node_id for n in self.nodes}

        for node in self.nodes:
            node.start()

        #yield
        gevent.sleep(0)

        for node in self.nodes:
            actual = set(node.cluster.nodes.keys())
            self.assertEqual(expected, actual)

    def test_uniform_token_ring_view(self):
        """ tests that all peers have the same view of the token ring after starting up """
        self.create_nodes(10)

        for node in self.nodes:
            node.start()

        # for node in self.nodes:
        #     node.cluster.discover_peers()

        #yield
        gevent.sleep(0)

        expected = {n.node_id: n.token for n in self.nodes}

        for i, node in enumerate(self.nodes):
            self.maxDiff = None
            actual = {n.node_id: n.token for n in node.cluster.token_ring}
            self.assertEqual(len(expected), len(actual))
            self.assertEqual(set(expected.keys()), set(actual.keys()))
            self.assertEqual(set(expected.values()), set(actual.values()))
            self.assertEqual(expected, actual)


class NodeActivationTest(BaseNodeTestCase):

    def test_single_node_activates_itself(self):
        """ tests that a node started in isolation will set itself to normal """
        node = self.create_node(cluster_status=Cluster.Status.INITIALIZING)
        node.start()

        self.assertEqual(node.cluster.status, Cluster.Status.NORMAL)


class RejoinClusterTests(TestCase):
    pass
