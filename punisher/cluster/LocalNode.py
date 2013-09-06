from punisher.cluster.BaseNode import BaseNode


class LocalNode(BaseNode):

    def __init__(self, node_id=None, name=None, token=None):
        super(LocalNode, self).__init__(node_id, name, token)