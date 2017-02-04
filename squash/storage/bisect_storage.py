import logging
from bisect import insort, bisect_left, bisect_right
from collections import defaultdict
from itertools import chain

from .base import BaseStorageEngine
from squash.exceptions import BaseError

logger = logging.getLogger(__name__)


class BisectStorageEngine(BaseStorageEngine):
    def __init__(self):
        self._data = defaultdict(list)
        self._scores = defaultdict(defaultdict)

    def zadd(self, key, nx=None, xx=None, ch=None, incr=None, *items):
        """

        :param key: Key of sorted set
        :param nx: Don't update already existing elements.
                   Always add new elements.
        :param xx: Only update elements that already exist. Never add elements.
        :param ch: Modify the return value from the number of new elements
        added, to the total number of elements changed (CH is an abbreviation
        of changed). Changed elements are new elements added and elements
        already existing for which the score was updated. So elements
        specified in the command line having the same score as they had in
        the past are not counted. Note: normally the return value of ZADD only
        counts the number of new elements added.
        :param incr: Increments the score of member in the sorted set stored at
        key by increment. If member does not exist in the sorted set, it is
        added with increment as its score (as if its previous score was 0.0).
        Only one score-element pair can be specified in this mode.
        :param items: list of values (<score>, <member>), [(<score>, <member>)]
        :return: Integer reply, specifically:
            The number of elements added to the sorted sets, not including
            elements already existing for which the score was updated.
        If the INCR option is specified, the return value will be Bulk
        string reply:
            the new score of member (a double precision floating point number),
            represented as string.
        """
        logger.debug("nx {}, xx {}, ch {}, incr {}".format(nx, xx, ch, incr))
        if nx and xx:
            raise BaseError(
                "XX and NX options at the same time are not compatible")

        if incr and len(items) > 1:
            raise BaseError(
                "INCR option supports a single increment-element pair")

        data = self._data[key]
        scores = self._scores[key]

        saved = updated = 0
        for score, member in items:
            existed_score = scores.get(member)
            if existed_score is not None:
                if nx is not None or (existed_score == score and incr is None):
                    continue

                index = bisect_left(data, (existed_score, member))
                data.pop(index)
                updated += 1
            elif xx is not None:
                continue
            else:
                saved += 1

            if incr is not None and existed_score is not None:
                score += existed_score

            insort(data, (score, member))
            scores[member] = score

        logger.debug("saved {}, update {}".format(saved, updated))
        return saved if ch is None else saved + updated


    def zcard(self, key):
        data = self._data.get(key)
        if data is None:
            return 0

        return len(data)

    def zrem(self, key, *members):
        data = self._data[key]
        scores = self._scores[key]

        result = 0
        for member in members:
            score = scores.get(member)
            if score is None:
                continue

            index = bisect_left(data, (score,))
            data.pop(index)
            del scores[member]
            result += 1

        return result

    def zrange(self, key, start, stop, withscores=None):
        stop += 1  # inclusive ranges
        if withscores is None:
            return (i[1] for i in self._data[key][start:stop])

        result = ((i[1], i[0]) for i in self._data[key][start:stop])
        return chain.from_iterable(result)

    def zrangebyscore(self, key, score_min, score_max, withscores=None):
        data = self._data[key]
        start = bisect_left(data, (score_min,))
        stop = bisect_right(data, (score_max,))

        if withscores is None:
            return [i[1] for i in data[start:stop]]
        return chain.from_iterable((i[1], i[0]) for i in data[start:stop])


def bisect_storage_factory():
    return BisectStorageEngine()
