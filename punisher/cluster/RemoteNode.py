from contextlib import contextmanager
from datetime import datetime
import time

from gevent.queue import Queue, Empty

from punisher.cluster.BaseNode import BaseNode
from punisher.cluster.Connection import Connection
from punisher.cluster import messages


class RemoteNode(BaseNode):

    class ConnectionError(Exception): pass

    class Status(object):
        INITIALIZED = 0
        UP          = 1
        DOWN        = 2
        PENDING     = 3
        CLOSED      = 4
        REFUSED     = 5

    def __init__(self, address, node_id=None, name=None, token=None, local_node=None):
        super(RemoteNode, self).__init__(node_id, name, token)
        self.address = address

        from punisher.cluster.LocalNode import LocalNode
        assert isinstance(local_node, LocalNode)
        self.local_node = local_node
        self.pool = Queue()

        self.status = RemoteNode.Status.INITIALIZED
        self.message_queue = Queue()

    @property
    def peer_data(self):
        return self.address, self.node_id, self.token, self.name

    @contextmanager
    def _connection(self):
        """
        context manager that pulls a connection from this remote node's connection
        pool, and returns it to the pool when it's done being used
        """
        try:
            # get a connection
            conn = self.pool.get(block=False)
        except Empty:
            # or create a new one
            conn = Connection.connect(self.address)
            messages.ConnectionRequest(
                self.local_node.node_id,
                self.local_node.address,
                self.local_node.token,
                sender_name=self.local_node.name
            ).send(conn)
            response = messages.Message.read(conn)
            if not isinstance(response, messages.ConnectionAcceptedResponse):
                raise RemoteNode.ConnectionError

        yield conn

        if conn.is_open:
            self.pool.put(conn)

    def add_conn(self, conn):
        assert isinstance(conn, Connection)
        self.pool.put(conn)

    def connect(self):
        """ establishes a connection with this remote node """
        with self._connection() as _: pass
        self.status = RemoteNode.Status.UP

    def send_message(self, request, save=False, retries=3):
        """
        Sends a messages to a remote node and returns it's reply. If there is
        an error sending a message, it will be retried, and saved if the save
        parameter is set to True

        :param request:
        :type request: messages.Message
        :param save: indicates that the message should be saved if it can't be delivered
        :param retries: the number of times to attempt to send a messages before
            considering this node down
        """
        assert isinstance(request, messages.Message)
        for i in range(retries):
            try:
                with self._connection() as conn:
                    request.send(conn)
                    return messages.Message.read(conn)
            except Connection.ClosedException:
                if i + 1 >= retries:
                    self.status = RemoteNode.Status.DOWN
                    if save:
                        self.message_queue.put(request)
                    raise

    def ping(self):
        """ pings the remote node """
        self.last_ping = datetime.utcnow()
        start_time = time.time()
        try:
            response = self.send_message(messages.PingRequest(self.local_node.node_id))
            assert isinstance(response, messages.PingResponse)
        except Exception:
            pass
        end_time = time.time()
        self.ping_time = end_time - start_time

    def execute_retrieval_instruction(self, instruction, key, args, digest=False):
        #TODO: send message
        pass

    def execute_mutation_instruction(self, instruction, key, args, timestamp):
        #TODO: send message
        pass




