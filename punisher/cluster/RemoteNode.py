from contextlib import contextmanager

from gevent.queue import Queue

from punisher.cluster.BaseNode import BaseNode


class RemoteNode(BaseNode):

    def __init__(self, address, node_id=None, name=None, token=None):
        super(RemoteNode, self).__init__(node_id, name, token)
        self.address = address
        self.pool = Queue()

    def _connection(self):
        conn = self.pool.get(block=False)
        if conn is None:
            conn

