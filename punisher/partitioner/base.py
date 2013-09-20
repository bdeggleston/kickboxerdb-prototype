class TokenRange(object):

    def __init__(self, start, stop):
        self.start = start
        self.stop = stop




class BasePartitioner(object):

    max_token = long('f' * 32, 16)

    @classmethod
    def get_random_token(cls):
        raise NotImplementedError

    @classmethod
    def get_key_token(cls, key):
        raise NotImplementedError