class BaseStorageEngine:
    """
    ZADD <key> <score> <member> (https://redis.io/commands/zadd)
    ZCARD <key> (https://redis.io/commands/zcard)
    ZREM <key> <member> (https://redis.io/commands/zrem)
    ZRANGE <key> <start> <stop> (https://redis.io/commands/zrange)
    ZRANGEBYSCORE <key> <min> <max> (https://redis.io/commands/zrangebyscore)
    """

    def zadd(self, key, score, member):
        raise NotImplementedError

    def zcard(self, key):
        raise NotImplementedError

    def zrem(self, key, member):
        raise NotImplementedError

    def zrange(self, key, start, stop):
        raise NotImplementedError

    def zrangebyscore(self, key, min, max):
        raise NotImplementedError

