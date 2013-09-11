from datetime import datetime, timedelta
from unittest import TestCase

from punisher.store.redis import RedisStore, Value
from punisher.utils import serialize_timestamp, deserialize_timestamp


class ValueTests(TestCase):

    def test_serialization(self):
        ts = datetime.utcnow()
        val = Value('a', ts)
        assert val.serialize(), ('a', serialize_timestamp(ts))

    def test_deserialization(self):
        val = Value.deserialize(('a', 123))
        assert val.data == 'a'
        assert val.timestamp == deserialize_timestamp(123)


class StoreTests(TestCase):

    def setUp(self):
        super(StoreTests, self).setUp()
        self.store = RedisStore()

    def test_set(self):
        ts = datetime.utcnow()
        self.store.set('a', 'b', timestamp=ts)
        val = self.store._data['a']
        assert val.data == 'b'
        assert val.timestamp == ts

    def test_conflicting_set(self):
        """
        If set if called with a timestamp, and the timestamp is
        less than the value to be overwritten, the write should
        be ignored
        """
        ts = datetime.utcnow()
        self.store.set('a', 'b', timestamp=ts)
        self.store.set('a', 'c', timestamp=ts-timedelta(seconds=1))
        val = self.store.get('a')
        assert val.data == 'b'

    def test_get(self):
        ts = datetime.utcnow()
        self.store.set('a', 'b', timestamp=ts)
        val = self.store.get('a')
        assert val.data == 'b'
        assert val.timestamp == ts

    def test_delete(self):
        self.store.set('a', 'b', datetime.utcnow())
        ts = datetime.utcnow()
        self.store.delete('a', timestamp=ts)
        val = self.store.get('a')
        assert val.data is None
        assert val.timestamp == ts

    def test_value_resolution(self):
        ts = datetime.utcnow()
        num_values = 10
        values = [Value(i, ts + timedelta(seconds=i)) for i in range(num_values)]
        val = RedisStore.resolve(values)
        assert val.data == num_values - 1
        assert val.timestamp == ts + timedelta(seconds=num_values-1)

