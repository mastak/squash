import logging


from squash.exceptions import InvalidArguments, BaseError

logger = logging.getLogger(__name__)


class CommandHandler:
    """
    ZADD <key> <score> <member> (https://redis.io/commands/zadd)
    ZCARD <key> (https://redis.io/commands/zcard)
    ZREM <key> <member> (https://redis.io/commands/zrem)
    ZRANGE <key> <start> <stop> (https://redis.io/commands/zrange)
    ZRANGEBYSCORE <key> <min> <max> (https://redis.io/commands/zrangebyscore)
    """
    VALID_COMMANDS = ('ZADD', 'ZCARD', 'ZREM', 'ZRANGE', 'ZRANGEBYSCORE',
                      'COMMAND')

    def __init__(self, connection, storage):
        self._connection = connection
        self._storage = storage

    async def handle_client(self):
        while True:
            data = await self._connection.read_command_data()
            if not data:
                break

            logger.debug("RECEIVED DATA: {}".format(data))
            await self.handle_command(*data)

    async def handle_command(self, cmd, key=None, *args):
        cmd = cmd.upper()
        if cmd not in self.VALID_COMMANDS:
            await self._connection.write_error("Invalid command {}".format(cmd))
            return

        try:
            await getattr(self, cmd)(key, *args)
        except BaseError as error:
            await self._connection.write_error(error.message)
        except Exception as error:
            logger.exception(error)
            await self._connection.write_error("SERVER ERROR")

    async def COMMAND(self, *args):
        await self._connection.write_string('OK')

    async def ZADD(self, key, *args):
        incr = nx = xx = ch = None

        start_idx = 0
        for i in args:
            if i.lower() == 'incr':
                incr = True
            elif i.lower() == 'nx':
                nx = True
            elif i.lower() == 'xx':
                xx = True
            elif i.lower() == 'ch':
                ch = True
            else:
                break
            start_idx += 1

        el_count = len(args) - start_idx
        if el_count % 2 or not el_count:
            raise InvalidArguments('zadd', args)

        result = self._storage.zadd(key, nx, xx, ch, incr, *args[start_idx:])
        await self._connection.write_string(result)

    async def ZCARD(self, key):
        result = self._storage.zcard(key)
        await self._connection.write_int(result)

    async def ZREM(self, key, *members):
        result = self._storage.zrem(key, *members)
        await self._connection.write_int(result)

    async def ZRANGE(self, key, start, stop, withscores=None):
        result = self._storage.zrange(key, int(start), int(stop), withscores)
        await self._connection.write_array(tuple(result))

    async def ZRANGEBYSCORE(self, key, score_min, score_max, withscores=None):
        # TODO:
        # - handling ( in score_min/score_max
        # - handling -inf/+inf in score_min/score_max
        # - add LIMIT

        result = self._storage.zrangebyscore(
            key, score_min, score_max, withscores)
        await self._connection.write_array(tuple(result))
