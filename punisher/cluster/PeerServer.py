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
        response = messages.Message.read(conn)
        if not isinstance(response, messages.ConnectionRequest):
            messages.ConnectionRefusedResponse(
                self.node_id,
                'first message must be a ConnectionMessage'
            ).send(conn)
            conn.close()
            return
        assert isinstance(response, messages.ConnectionRequest)

        node_id = response.sender

        # accept response and identify
        messages.ConnectionAcceptedResponse(sender_id=self.cluster.local_node.node_id).send(conn)

        if self.cluster.local_node.name: print self.cluster.local_node.name,
        print node_id, 'connected'
        return self.cluster.add_node(node_id, response.sender_address, response.token, name=response.sender_name)

    def _execute_request(self, request, peer):
        pass

    def handle(self, socket, address):
        """
        entry point for new connections

        :param socket:
        :param address:
        """
        conn = Connection(socket)
        peer = self._accept_connection(conn)
        while True:
            request = messages.Message.read(conn)

