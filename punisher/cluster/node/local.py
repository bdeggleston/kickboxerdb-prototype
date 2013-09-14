from datetime import datetime

from punisher.cluster.node.base import BaseNode
from punisher.utils import deserialize_timestamp


class LocalNode(BaseNode):

    def __init__(self, store, address=None, node_id=None, name=None, token=None):
        """
        :param store:
        :param store: punisher.store.redis.RedisStore
        :param address:
        :param address:
        :param node_id:
        :param node_id:
        :param name:
        :param name:
        :param token:
        :param token:
        """
        super(LocalNode, self).__init__(node_id, name, token)
        self.address = address

        # storage
        self.store = store
        self.token = self.token if self.token is not None else self.store.get_random_token()

    def ping(self):
        self.last_ping = datetime.utcnow()
        self.ping_time = 0

    def execute_retrieval_instruction(self, instruction, key, args, digest=False):
        return getattr(self.store, instruction)(key, *args)

    def execute_mutation_instruction(self, instruction, key, args, timestamp):
        if isinstance(timestamp, (int, long)):
            timestamp = deserialize_timestamp(timestamp)
        return getattr(self.store, instruction)(key, *args, timestamp=timestamp)




