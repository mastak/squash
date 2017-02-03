FIRST_BYTE_INT = b':'[0]
FIRST_BYTE_ARRAY = b'*'[0]
FIRST_BYTE_BULK_STR = b'$'[0]


async def parse_command(client_reader):
    line = await client_reader.readline()
    if not line:
        return

    int_val = int(line[1:])

    if line[0] == FIRST_BYTE_ARRAY:
        return await read_array(client_reader, int_val)
    if line[0] == FIRST_BYTE_BULK_STR:
        return await read_bulk_string(client_reader, int_val)
    if line[0] == FIRST_BYTE_INT:
        return int_val

    raise NotImplemented

async def read_array(client_reader, ar_len):
    return [await parse_command(client_reader) for _ in range(ar_len)]

async def read_bulk_string(client_reader, str_len):
    line = await client_reader.readline()
    return line.decode("utf-8")[:str_len]
