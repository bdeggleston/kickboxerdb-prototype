from punisher.cluster.BaseNode import BaseNode

from punisher.store.RedisStore import RedisStore


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

    def execute_retrieval_instruction(self, instruction, key, args, digest=False):
        return super(LocalNode, self).execute_retrieval_instruction(instruction, key, args)

    def execute_mutation_instruction(self, instruction, key, args, timestamp):
        return super(LocalNode, self).execute_mutation_instruction(instruction, key, args, timestamp)




