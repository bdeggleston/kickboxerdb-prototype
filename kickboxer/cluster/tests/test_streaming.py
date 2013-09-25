from kickboxer.tests.base import BaseNodeTestCase


class StreamingQueryTest(BaseNodeTestCase):
    """
    tests querying nodes that are receiving streaming data
    forward queries to the streaming node
    """
