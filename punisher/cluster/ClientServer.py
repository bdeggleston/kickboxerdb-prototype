from punisher.cluster.Cluster import Cluster


class RedisClientServer(object):

    def __init__(self, cluster, address=('', 6379)):
        super(RedisClientServer, self).__init__()
        assert isinstance(cluster, Cluster)
        self.address = address