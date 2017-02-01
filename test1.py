import random
from bisect import insort, bisect_left, bisect_right
from collections import defaultdict
from time import time


class StoreEngine:
    """
    ZADD <key> <score> <member> (https://redis.io/commands/zadd)
    ZCARD <key> (https://redis.io/commands/zcard)
    ZREM <key> <member> (https://redis.io/commands/zrem)
    ZRANGE <key> <start> <stop> (https://redis.io/commands/zrange)
    ZRANGEBYSCORE <key> <min> <max> (https://redis.io/commands/zrangebyscore)
    """
    def __init__(self):
        self._data = defaultdict(list)
        self._scores = defaultdict(defaultdict(list))

    def zadd(self, key, score, member):
        insort(self._data[key], (score, member))
        insort(self._scores[key][member], score)

    def zcard(self, key):
        return len(self._data[key])

    def zrem(self, key, member):
        data = self._data[key]
        scores = self._scores[key][member]

        min_score = scores[0]
        max_score = scores[-1]

        start = bisect_left(data, (min_score,))
        stop = bisect_right(data, (max_score,))

        del data[start:stop]
        del self._scores[key][member]

    def zrange(self, key, start, stop):
        return (i[1] for i in self._data[key][start:stop])

    def zrangebyscore(self, key, min, max):
        data = self._data[key]
        start = bisect_left(data, (min,))
        stop = bisect_left(data, (max,))
        return (i[1] for i in data[start:stop])


def test_list(seq):
    res = []
    for i in seq:
        res.append(i)
        res.sort()
    return res


def test_bisect(seq):
    res = []
    for i in seq:
        insort(res, i)
    return res


def teset_remove1(src, rm):
    for i in rm:
        src.remove(i)


def teset_remove2(src, rm):
    for score, _ in rm:
        start = bisect_left(src, (score,))
        src.pop(start)

def teset_remove3(src, rm):
    for score, data in rm:
        start = bisect_left(src, (score, data))
        src.pop(start)


def main():
    src = tuple((random.randint(1, 100), 'asdasdc') for _ in range(8000))

    start = time()
    r1 = test_bisect(src)
    # print("TEST_LIST: {}".format(time() - start))

    start = time()
    r2 = test_bisect(src)
    # print("TEST_BISECT: {}".format(time() - start))

    print(r1 == r2)

    for_remove = [random.choice(r2) for _ in range(400)]

    start = time()
    teset_remove3(r1, for_remove)
    print("ReMOVE 1: {}".format(time() - start))

    start = time()
    teset_remove2(r2, for_remove)
    print("ReMOVE 2: {}".format(time() - start))

    print(r1 == r2)

    # print(r1)
    # print(r2)





    # s = bisect_left(r2, 4)
    # print(s, r2[s-1], r2[s])

if __name__ == "__main__":
    main()