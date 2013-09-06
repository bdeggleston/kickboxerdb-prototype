from gevent.server import StreamServer

from punisher.cluster.Cluster import Cluster
from punisher.cluster.Connection import Connection


class RedisClientServer(StreamServer):

    def __init__(self, listener, cluster, backlog=None, spawn='default', **ssl_args):
        listener = listener or ('', 4379)
        super(RedisClientServer, self).__init__(listener, self.handle, backlog, spawn, **ssl_args)
        assert isinstance(cluster, Cluster)
        self.cluster = cluster

    def handle(self, socket, address):
        """
        entry point for new connections

        :param socket:
        :param address:
        """
        conn = Connection(socket)
