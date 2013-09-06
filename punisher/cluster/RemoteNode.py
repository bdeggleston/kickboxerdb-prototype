from contextlib import contextmanager

from gevent.queue import Queue

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

    @contextmanager
    def _connection(self):
        """
        context manager that pulls a connection from this remote node's connection
        pool, and returns it to the pool when it's done being used
        """
        conn = self.pool.get(block=False)
        if conn is None:
            conn = Connection.connect(self.address)
            messages.ConnectionRequest(
                self.local_node.node_id,
                self.local_node.address,
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
        with self._connection as _: pass


    def execute_retrieval_instruction(self, instruction, key, args, digest=False):
        #TODO: send message
        pass

    def execute_mutation_instruction(self, instruction, key, args, timestamp):
        #TODO: send message
        pass




