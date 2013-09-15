from collections import defaultdict
from datetime import datetime

from blist import sorteddict

from punisher.utils import serialize_timestamp, deserialize_timestamp

# checkout the multiprocessing module
# http://cython.org/
# http://tokutek.com/downloads/mysqluc-2010-fractal-trees.pdf


class Value(object):
    """
    values held by the store, competing values are resolved by their timestamps
    most recent one wins

    deleting a value results in a Value with a None value
    """

    def __init__(self, value, timestamp=None):
        """
        :param value:
        :type value: str
        :param timestamp: the time this value was added
        :type timestamp: datetime
        :return:
        """
        self.data = value
        self.timestamp = timestamp
        if isinstance(self.timestamp, (int, long)):
            self.timestamp = self._deserialize_timestamp(self.timestamp)

    def __repr__(self):
        return '<Value data={} ts={}>'.format(self.data, self.timestamp)

    def __eq__(self, other):
        if isinstance(other, Value):
            return other.data == self.data and other.timestamp == self.timestamp
        return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def serialize(self):
        return self.data, serialize_timestamp(self.timestamp)

    @classmethod
    def deserialize(cls, data):
        val, ts = data
        return Value(val, deserialize_timestamp(ts))


class Instruction(object):

    def __init__(self, instruction, key, args, timestamp):
        super(Instruction, self).__init__()
        self.instruction = instruction
        self.key = key
        self.args = args
        self.timestamp = timestamp


class RedisStore(object):
    """
    Basic key/value store, values are stored as Value instance,
    which include the value data, and a timestamp of when it was
    added

    deleting a value results in a Value with a None value
    """

    retrieval_instructions = frozenset(['get'])
    mutation_instructions = frozenset(['set', 'delete'])

    def __init__(self, partitioner):
        """
        :param partitioner:
        :type partitioner: punisher.partitioner.base.BasePartitioner
        """
        super(RedisStore, self).__init__()
        self.partitioner = partitioner
        self._data = {}

    def __contains__(self, item):
        return item in self._data

    @property
    def token_map(self):
        """
        returns a map of token -> {key[1], ...key[n]}
        :return:
        """
        #TODO: keep track of this during normal storage operation
        # using a set in case there are token collisions
        token_map = defaultdict(set)
        for key in self._data.keys():
            token_map[self.partitioner.get_key_token(key)].add(key)
        token_map = sorteddict(token_map)
        return token_map

    def get_token_range(self, start_token, max_token, count):
        """
        returns raw value data for the given token range, sorted
        by token, then by key (for collisions)
        :param start_token:
        :param count:
        :return:
        """
        assert count > 1
        token_map = self.token_map

        rdata = []
        key_view = token_map.viewkeys()
        start_idx = key_view.bisect_left(start_token)
        stop_idx = start_idx + count
        for token in key_view[start_idx: stop_idx]:
            if token > max_token:
                break
            for key in sorted(list(token_map[token])):
                rdata.append((key, self._data.get(key)))
        return rdata

    def remove_token_range(self, start_token, stop_token):
        """
        removes all keys in the store with tokens between the
        given start and stop token, inclusively
        :param start_token:
        :param stop_token:
        :return:
        """
        token_map = self.token_map
        key_view = token_map.viewkeys()
        start_idx = key_view.bisect_left(start_token)
        for token in key_view[start_idx:]:
            if token > stop_token:
                break
            for key in token_map[token]:
                self._data.pop(key, None)

    def set_and_reconcile_raw_value(self, key, value):
        self._data[key] = value

    def set(self, key, val, timestamp):
        # if timestamp was provided, check against
        # check against existing value
        val = Value(val, timestamp)
        existing = self._data.get(key)
        if timestamp:
            if existing and existing.timestamp >= val.timestamp:
                return
        self._data[key] = val

    def get_random_token(self):
        return self.partitioner.get_random_token()

    @classmethod
    def resolve_set(cls, key, args, timestamp, values):
        return Value(True, None)

    def get(self, key):
        """ :rtype: Value """
        return self._data.get(key)

    def delete(self, key, timestamp):
        # if timestamp was provided, check against
        # check against existing value
        val = Value(None, timestamp)
        if timestamp:
            existing = self._data.get(key)
            if existing and existing.timestamp >= val.timestamp:
                return
        self._data[key] = val

    @classmethod
    def resolve_get(cls, key, args, values):
        """
        :param args: request args
        :param values: remote data received
        :param local: local data received
        :return: resolved values
        """
        _ = args
        return cls.resolve(values)

    @classmethod
    def resolve_get_instructions(cls, key, args, value_map):
        """
        resolves all values from all nodes, and returns a list of instructions
        to send to each one to fix any inconsistencies

        :param key:
        :param args:
        :param value_map:
        :return:
        """
        value = cls.resolve_get(key, args, filter(None, value_map.values()))
        if value.data:
            return {nid: [Instruction('set', key, [value.data], value.timestamp)] for nid, val in value_map.items() if val != value}
        else:
            return {nid: [Instruction('delete', key, [], value.timestamp)] for nid, val in value_map.items() if val != value}

    @classmethod
    def resolve(cls, values):
        """
        compares multiple values and returns
        the one with the highest timestamp

        :param cls:
        :param values:
        :rtype: Value
        """
        return max(values, key=lambda v: v.timestamp)

