class BaseStore(object):

    retrieval_instructions = frozenset()
    mutation_instructions = frozenset()

    @property
    def token_map(self):
        raise NotImplementedError

    def get_token_range(self, start_token, max_token, count):
        """
        returns raw value data for the given token range, sorted
        by token, then by key (for collisions)
        :param start_token:
        :param count:
        :return:
        """
        raise NotImplementedError

    def set_and_reconcile_raw_value(self, key, value):
        raise NotImplementedError
