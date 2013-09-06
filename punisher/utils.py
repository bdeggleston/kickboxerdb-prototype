from datetime import datetime

__epoch__ = datetime(1970, 1, 1)


def serialize_timestamp(ts):
    assert isinstance(ts, datetime)
    return long((ts - __epoch__).total_seconds() * 1000000)


def deserialize_timestamp(ts):
    assert isinstance(ts, (int, long))
    return datetime.utcfromtimestamp(ts / 1000000.0)

