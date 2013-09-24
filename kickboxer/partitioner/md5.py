from hashlib import md5
import random
import struct

from kickboxer.partitioner.base import BasePartitioner


class MD5Partitioner(BasePartitioner):


    @classmethod
    def get_key_token(cls, key):
        digest = md5(key).digest()
        u1, u2 = struct.unpack('!QQ', digest)
        token = (u1 << 64) | u2
        return token

    @classmethod
    def get_random_token(cls):
        return random.randint(0, MD5Partitioner.max_token)

