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


def test_remove1(src, rm):
    for i in rm:
        src.remove(i)


def test_remove2(src, rm):
    for score, _ in rm:
        start = bisect_left(src, (score,))
        src.pop(start)


def test_remove3(src, rm):
    for score, data in rm:
        start = bisect_left(src, (score, data))
        src.pop(start)


def test_bytearray(src):
    buf = bytearray()
    for score, item in src:
        buf.extend(b'1' + item.encode() + b'\r\n')
    return buf


def test_join(src):
    result = []
    for score, item in src:
        result.append('1')
        result.append(item)
        result.append('\r\n')
    return ''.join(result).encode()

def test_join2(src):
    result = []
    for score, item in src:
        result.extend(('1', item, '\r\n'))
        # result.append('\r\n')
    return ''.join(result).encode()


def main():
    src = tuple((random.randint(1, 100), 'asdasdc') for _ in range(30))
    # src = tuple((random.randint(1, 100), 'asdasdc') for _ in range(8000))
    #
    # start = time()
    # r1 = test_bisect(src)
    # # print("TEST_LIST: {}".format(time() - start))
    #
    # start = time()
    # r2 = test_bisect(src)
    # # print("TEST_BISECT: {}".format(time() - start))
    #
    # print(r1 == r2)
    #
    # for_remove = [random.choice(r2) for _ in range(400)]
    #
    # start = time()
    # test_remove3(r1, for_remove)
    # print("ReMOVE 1: {}".format(time() - start))
    #
    # start = time()
    # test_remove2(r2, for_remove)
    # print("ReMOVE 2: {}".format(time() - start))
    #
    # print(r1 == r2)

    start = time()
    for i in range(10000):
        test_bytearray(src)
    print("BYTEARRAY: {}".format(time() - start))

    start = time()
    for i in range(10000):
        test_join2(src)
    print("JOIN2    : {}".format(time() - start))

    start = time()
    for i in range(10000):
        test_join(src)
    print("JOIN     : {}".format(time() - start))


if __name__ == "__main__":
    main()