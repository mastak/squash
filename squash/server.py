import asyncio
from asyncio.streams import start_server

from squash.handler import CommandHandler
from squash.parser import CommandParser


class SquashServer:

    def __init__(self):
        self._handler = CommandHandler()
        self._server = None
        self._clients = {}  # task -> (reader, writer)

    def _accept_client(self, client_reader, client_writer):
        task = asyncio.Task(self._handle_client(client_reader, client_writer))
        self._clients[task] = (client_reader, client_writer)

        def client_done(task):
            del self._clients[task]

        task.add_done_callback(client_done)

    async def _handle_client(self, client_reader, client_writer):
        parser = CommandParser(client_reader)
        while True:
            data = await parser.parse_command()
            if not data:
                break

            print(data)
            response = self._handler.handle_command(*data)
            # client_writer.write("{}\r\n".format(response).encode())
            client_writer.write(b"+OK\r\n")
            await client_writer.drain()

    async def start(self, host, port):
        self._server = await start_server(self._accept_client, host, port)

    async def stop(self):
        for task in self._clients.keys():
            if not task.done():
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
