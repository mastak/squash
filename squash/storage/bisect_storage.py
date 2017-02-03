from bisect import insort, bisect_left, bisect_right
from collections import defaultdict

from .base import BaseStorageEngine


class BisectStorageEngine(BaseStorageEngine):
    def __init__(self):
        self._data = defaultdict(list)
        self._scores = defaultdict(defaultdict)

    def zadd(self, key, score, member):
        data = self._data[key]
        scores = self._scores[key]

        existed_score = scores.get(member)
        if existed_score is not None:
            index = bisect_left(data, (existed_score,))
            data.pop(index)

        insort(data, (score, member))
        scores[member] = score

    def zcard(self, key):
        data = self._data.get(key)
        if data is None:
            return 0

        return len(data)

    def zrem(self, key, *member):
        data = self._data[key]
        scores = self._scores[key]

        score = scores[member]
        index = bisect_left(data, (score,))

        data.pop(index)
        del scores[member]

    def zrange(self, key, start, stop, withscores=None):
        stop += 1  # inclusive ranges
        if withscores is None:
            return (i[1] for i in self._data[key][start:stop])
        return ((i[1], i[0]) for i in self._data[key][start:stop])

    def zrangebyscore(self, key, score_min, score_max, withscores=None):
        data = self._data[key]
        start = bisect_left(data, (score_min,))
        stop = bisect_right(data, (score_max,))

        if withscores is None:
            return (i[1] for i in data[start:stop])
        return ((i[1], i[0]) for i in data[start:stop])


def bisect_storage_factory():
    return BisectStorageEngine()
