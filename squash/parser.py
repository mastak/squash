
FIRST_BYTE_INT = b':'[0]
FIRST_BYTE_ARRAY = b'*'[0]
FIRST_BYTE_BULK_STR = b'$'[0]


class CommandParser:
    def __init__(self, client_reader):
        self.client_reader = client_reader

    async def parse_command(self):
        line = await self.client_reader.readline()
        if not line:
            return

        int_val = int(line[1:])

        if line[0] == FIRST_BYTE_ARRAY:
            return await self.read_array(int_val)
        if line[0] == FIRST_BYTE_BULK_STR:
            return await self.read_bulk_string(int_val)
        if line[0] == FIRST_BYTE_INT:
            return int_val

        raise NotImplemented

    async def read_array(self, ar_len):
        return [await self.parse_command() for _ in range(ar_len)]

    async def read_bulk_string(self, str_len):
        line = await self.client_reader.readline()
        return line.decode("utf-8")[:str_len]
