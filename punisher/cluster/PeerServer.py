import pickle
import uuid

from gevent.server import StreamServer

from punisher.cluster.Cluster import Cluster
from punisher.cluster.Connection import Connection
from punisher.cluster import messages

from punisher.utils import deserialize_timestamp


class PeerServer(StreamServer):
    """ handles incoming requests from other nodes in the cluster """

    def __init__(self, listener, cluster, backlog=None, spawn='default', **ssl_args):
        listener = listener or ('', 4379)
        super(PeerServer, self).__init__(listener, self.handle, backlog, spawn, **ssl_args)
        assert isinstance(cluster, Cluster)
        self.cluster = cluster

        self.connections = {}

    @property
    def node_id(self):
        return self.cluster.node_id

    @property
    def token(self):
        return self.cluster.token

    @property
    def name(self):
        return self.cluster.name

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
        messages.ConnectionAcceptedResponse(
            self.node_id,
            str(self.token),
            self.name
        ).send(conn)

        peer = self.cluster.add_node(
            node_id,
            response.sender_address,
            long(response.token) if response.token else None,
            name=response.sender_name
        )
        peer.connect()
        return peer

    def _execute_request(self, request, peer):
        """
        handles incoming request messages

        :param request:
        :param peer:
        :rtype: messages.Message
        """

        if isinstance(request, messages.NoopMessage):
            return messages.NoopMessage(self.node_id)

        elif isinstance(request, messages.PingRequest):
            return messages.PingResponse(self.node_id)

        elif isinstance(request, messages.DiscoverPeersRequest):
            peer_data = [p.peer_data for p in self.cluster.get_peers()]
            return messages.DiscoverPeersResponse(self.node_id, peer_data)

        elif isinstance(request, messages.RetrievalValueRequest):
            if request.instruction not in self.cluster.store.retrieval_instructions:
                return messages.ErrorResponse(
                    self.node_id, '{} is not a valid read instruction'.format(request.instruction)
                )

            val = getattr(self.cluster.store, request.instruction)(request.key, *request.args)
            if val is None:
                return messages.UnknownKeyResponse(self.node_id)
            else:
                return messages.RetrievalValueResponse(
                    self.node_id,
                    pickle.dumps(val, protocol=pickle.HIGHEST_PROTOCOL)
                )

        elif isinstance(request, messages.MutationOperationRequest):
            if request.instruction not in self.cluster.store.mutation_instructions:
                return messages.ErrorResponse(
                    self.node_id, '{} is not a mutation instruction'.format(request.instruction)
                )

            try:
                ts = deserialize_timestamp(request.timestamp) if request.timestamp else request.timestamp
                result = getattr(self.cluster.store, request.instruction)(request.key, *request.args, timestamp=ts)
                return messages.MutationOperationResponse(self.node_id, result)
            except Exception as ex:
                return messages.ErrorResponse(
                    self.node_id, 'error processing request: {} \n {}'.format(request, ex)
                )

        else:
            return messages.ErrorResponse(self.node_id, 'unexpected message: {}'.format(request))

    def handle(self, socket, address):
        """
        entry point for new connections

        :param socket:
        :param address:
        """
        conn = Connection(socket)
        connection_id = uuid.uuid1()
        self.connections[connection_id] = conn
        try:
            peer = self._accept_connection(conn)
            while True:
                request = messages.Message.read(conn)
                response = self._execute_request(request, peer)
                response.send(conn)
        except Connection.ClosedException:
            pass
        finally:
            conn.close()
            try:
                del self.connections[connection_id]
            except KeyError:
                pass

    def kill(self):
        super(PeerServer, self).kill()
        for conn in self.connections.viewvalues():
            conn.close()
        self.connections.clear()




