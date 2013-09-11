from hashlib import md5
import struct

from punisher.partitioner.base import BasePartitioner


class MD5Partitioner(BasePartitioner):

    @classmethod
    def get_key_token(cls, key):
        digest = md5(key).digest()
        u1, u2 = struct.unpack('!QQ', digest())
        token = (u1 << 64) | u2
        return token
