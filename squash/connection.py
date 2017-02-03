from squash.parser import parse_command

FIRST_BYTE_INT = b':'[0]
FIRST_BYTE_ARRAY = b'*'[0]
FIRST_BYTE_BULK_STR = b'$'[0]


_converters = {
    tuple: lambda val: encode_array(val),
    list: lambda val: encode_array(val),
    int: lambda val: (':', val, '\r\n'),
    str: lambda val: ('$', val, '\r\n'),
    'default': lambda val: ('$', val, '\r\n'),
}


def encode_array(arr):
    parts = []
    parts.extend(('*', len(arr), '\r\n'))
    for item in arr:
        tp = type(item) if type(item) in _converters else 'default'
        res = _converters[tp](item)
        parts.extend(res)
    return ''.join(arr)


class Connection:

    def __init__(self, client_reader, client_writer):
        self._client_reader = client_reader
        self._client_writer = client_writer

    async def read_command_data(self):
        line = await self._client_reader.readline()
        if not line:
            return

        int_val = int(line[1:])

        if line[0] == FIRST_BYTE_ARRAY:
            return await self.read_array(int_val)
        if line[0] == FIRST_BYTE_BULK_STR:
            return await self.read_bulk_string(int_val)
        if line[0] == FIRST_BYTE_INT:
            return int_val

        await self.write_error("Invalid data")

    async def read_array(self, ar_len):
        return [await parse_command(self._client_reader) for _ in range(ar_len)]

    async def read_bulk_string(self, str_len):
        line = await self._client_reader.readline()
        return line.decode("utf-8")[:str_len]

    async def write_int(self, value):
        self._write(":{}\r\n".format(value))

    async def write_error(self, message):
        self._write("-{}\r\n".format(message))

    async def write_array(self, arr):
        result = encode_array(arr)
        self._write(result)

    async def _write(self, data):
        self._client_writer.write(data.encode())
        await self._client_writer.drain()
