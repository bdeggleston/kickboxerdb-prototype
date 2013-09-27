from kickboxer.tests.base import BaseNodeTestCase


class BaseStreamingQueryTest(BaseNodeTestCase):
    """
    tests querying nodes that are receiving streaming data
    forward queries to the streaming node
    """


class JoiningNodeQueryTest(BaseStreamingQueryTest):
    """
    when a node joins, and is streaming data from the
    node to it's left, it should forward all read queries
    to the streaming node
    """
    pass


class TokenChangeQueryTest(BaseStreamingQueryTest):
    """
    when a node changes it's token, and is streaming data
    from the node to it's left, it should forward all read
    queries to the streaming node
    """
    pass


class NodeRemovalQueryTest(BaseStreamingQueryTest):
    pass
