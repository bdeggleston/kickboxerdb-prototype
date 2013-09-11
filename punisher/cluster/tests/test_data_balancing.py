from unittest.case import TestCase


class TestInboundDataTransfer(TestCase):
    """ tests streaming of data when new nodes join the cluster """


class TestOutboundDataTransfer(TestCase):
    """ tests dissemination of data to other nodes """


class TestDataCleanup(TestCase):
    """ tests nodes delete data that they've transferred to other nodes """