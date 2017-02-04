from squash.parser import parse_command

PREFIX_STRING = '+'
PREFIX_ERROR = '-'
PREFIX_INT = ':'
PREFIX_ARRAY = '*'
PREFIX_BULK_STRING = '$'


BYTE_PREFIX_INT = PREFIX_INT.encode()[0]
BYTE_PREFIX_ARRAY = PREFIX_ARRAY.encode()[0]
BYTE_PREFIX_BULK_STRING = PREFIX_BULK_STRING.encode()[0]


_converters = {
    tuple: lambda val: encode_array(val),
    list: lambda val: encode_array(val),
    int: lambda val: (':', val, '\r\n'),
    str: lambda val: ('$', val, '\r\n'),
}


def encode_array(arr):
    parts = []
    parts.extend((PREFIX_ARRAY, len(arr), '\r\n'))
    for item in arr:
        tp = type(item) if type(item) in _converters else str
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

        if line[0] == BYTE_PREFIX_ARRAY:
            return await self.read_array(int_val)
        if line[0] == BYTE_PREFIX_BULK_STRING:
            return await self.read_bulk_string(int_val)
        if line[0] == BYTE_PREFIX_INT:
            return int_val

        await self.write_error("Invalid data")

    async def read_array(self, ar_len):
        return [await parse_command(self._client_reader) for _ in range(ar_len)]

    async def read_bulk_string(self, str_len):
        line = await self._client_reader.readline()
        return line.decode("utf-8")[:str_len]

    async def write_int(self, value):
        await self._write("{}{}\r\n".format(PREFIX_INT, value))

    async def write_string(self, value):
        await self._write("{}{}\r\n".format(PREFIX_STRING, value))

    async def write_error(self, message):
        await self._write("{}{}\r\n".format(PREFIX_ERROR, message))

    async def write_array(self, arr):
        result = encode_array(arr)
        await self._write(result)

    async def _write(self, data):
        self._client_writer.write(data.encode())
        await self._client_writer.drain()
