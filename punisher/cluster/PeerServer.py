from gevent.server import StreamServer

from punisher.cluster.Cluster import Cluster
from punisher.cluster.Connection import Connection
from punisher.cluster import messages


class PeerServer(StreamServer):
    """ handles incoming requests from other nodes in the cluster """

    def __init__(self, listener, cluster, backlog=None, spawn='default', **ssl_args):
        listener = listener or ('', 4379)
        super(PeerServer, self).__init__(listener, self.handle, backlog, spawn, **ssl_args)
        assert isinstance(cluster, Cluster)
        self.cluster = cluster

    def _accept_connection(self, conn):
        message = messages.Message.read(conn)
        if not isinstance(message, messages.ConnectionRequest):
            messages.ConnectionRefusedResponse(
                self.node_id,
                'first message must be a ConnectionMessage'
            ).send(conn)
            conn.close()
            return
        assert isinstance(message, messages.ConnectionRequest)

        node_id = message.sender

        # accept response and identify
        messages.ConnectionAcceptedResponse(sender_id=self.cluster.local_node.node_id).send(conn)

        if self.cluster.local_node.name: print self.cluster.local_node.name,
        print node_id, 'connected'

        if node_id not in self.cluster:
            self.cluster.add_node(node_id, message.sender_address, message.sen)
        peer = self.peers.get(node_id)
        if peer is None:
            peer = self.connect_to_peer(message.sender_address, expected_node_id=node_id)
        else:
            if peer.status == Peer.Status.CLOSED:
                peer.connect()

        return peer

    def handle(self, socket, address):
        """
        entry point for new connections

        :param socket:
        :param address:
        """
        conn = Connection(socket)
        pass

