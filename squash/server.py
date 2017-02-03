import asyncio
from asyncio.streams import start_server

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


def main():
    host, port = '192.168.1.156', '6379'

    loop = asyncio.get_event_loop()
    server = SquashServer()

    loop.run_until_complete(server.start(host, port))
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        loop.run_until_complete(server.stop())
    finally:
        loop.close()


if __name__ == '__main__':
    main()
