from datetime import datetime

from punisher.cluster.BaseNode import BaseNode
from punisher.store.RedisStore import RedisStore
from punisher.utils import deserialize_timestamp


class LocalNode(BaseNode):

    def __init__(self, address=None, node_id=None, name=None, token=None):
        """
        :param node_id:
        :param name:
        :param token:
        """
        super(LocalNode, self).__init__(node_id, name, token)
        self.address = address

        # storage
        self.store = RedisStore()

    def ping(self):
        self.last_ping = datetime.utcnow()
        self.ping_time = 0

    def execute_retrieval_instruction(self, instruction, key, args, digest=False):
        return getattr(self.store, instruction)(key, *args)

    def execute_mutation_instruction(self, instruction, key, args, timestamp):
        if isinstance(timestamp, (int, long)):
            timestamp = deserialize_timestamp(timestamp)
        return getattr(self.store, instruction)(key, *args, timestamp=timestamp)




