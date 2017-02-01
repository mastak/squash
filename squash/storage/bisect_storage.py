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
        return len(self._data[key])

    def zrem(self, key, member):
        data = self._data[key]
        scores = self._scores[key]

        score = scores[member]
        index = bisect_left(data, (score,))

        data.pop(index)
        del scores[member]

    def zrange(self, key, start, stop):
        return (i[1] for i in self._data[key][start:stop])

    def zrangebyscore(self, key, score_min, score_max):
        data = self._data[key]
        start = bisect_left(data, (score_min,))
        stop = bisect_left(data, (score_max,))
        return (i[1] for i in data[start:stop])
