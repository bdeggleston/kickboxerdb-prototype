class BasePartitioner(object):

    @classmethod
    def get_random_token(cls):
        raise NotImplementedError

    @classmethod
    def get_key_token(cls, key):
        raise NotImplementedError