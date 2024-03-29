import uuid


class BaseNode(object):

    def __init__(self, node_id=None, name=None, token=None):
        super(BaseNode, self).__init__()
        self.node_id = node_id or uuid.uuid4()
        self.name = name
        self.token = token

        # status tracking
        self.last_ping = None
        self.ping_time = None

    def __hash__(self):
        return hash(self.node_id)

    def ping(self):
        raise NotImplementedError

    def execute_retrieval_instruction(self, instruction, key, args, digest=False):
        raise NotImplementedError

    def execute_mutation_instruction(self, instruction, key, args, timestamp):
        raise NotImplementedError



