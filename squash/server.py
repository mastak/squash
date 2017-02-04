import asyncio
import logging
from asyncio.streams import start_server
from optparse import OptionParser

from squash.connection import Connection
from squash.handler import CommandHandler
from squash.storage.bisect_storage import bisect_storage_factory


class SquashServer:

    def __init__(self, storage_factory=bisect_storage_factory):
        self._storage = storage_factory()
        self._server = None
        self._clients = {}

    def _accept_client(self, client_reader, client_writer):
        connection = Connection(client_reader, client_writer)
        task = asyncio.Task(self._handle_client(connection))
        self._clients[task] = connection

        def client_done(finished_task):
            del self._clients[finished_task]
        task.add_done_callback(client_done)

    async def _handle_client(self, connection):
        handler = CommandHandler(connection, self._storage)
        await handler.handle_client()

    async def start(self, host, port):
        self._server = await start_server(self._accept_client, host, port)

    async def stop(self):
        for task, connection in self._clients.items():
            if not task.done():
                await connection.write_error("SUDDEN STOP")
                task.cancel()

        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None


def get_args():
    parser = OptionParser(usage="usage: %prog [options]", add_help_option=False)
    parser.add_option("-h", "--host", help="Server host", default="0.0.0.0")
    parser.add_option("-p", "--port", help="Server port", default=6379)
    parser.add_option("-v", "--verbose", help="Run in verbose mode",
                      action="store_true")
    parser.add_option("--help", action="help",
                      help="show this help message and exit")
    options, _ = parser.parse_args()
    return options


def main():
    args = get_args()
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)

    loop = asyncio.get_event_loop()
    server = SquashServer()

    loop.run_until_complete(server.start(args.host, args.port))
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        loop.run_until_complete(server.stop())
    finally:
        loop.close()


if __name__ == '__main__':
    main()
