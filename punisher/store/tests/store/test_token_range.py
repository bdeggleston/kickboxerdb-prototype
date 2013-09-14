from datetime import datetime
from unittest import TestCase

from punisher.store.redis import RedisStore

from punisher.tests.base import LiteralPartitioner


class TokenRangeTest(TestCase):

    def test_proper_range_is_returned(self):
        ts = datetime.utcnow()
        store = RedisStore(LiteralPartitioner())
        for i in range(1000):
            store.set(str(i), str(i), ts)

        start_token = 100
        stop_token = 120
        slice_size = 10
        results = store.get_token_range(start_token, stop_token, slice_size)

        for i, (key, val) in enumerate(results):
            self.assertEqual(key, str(start_token + i))
            self.assertEqual(val.data, str(start_token + i))

