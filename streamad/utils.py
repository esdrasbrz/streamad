import datetime as dt
import pickle
import base64
from aiofile import AIOFile, LineReader 
from streamad.models import Input


async def parse_csv_data(filename, ignore_header=True):
    async with AIOFile(filename, 'r') as f:
        n = 0
        async for line in LineReader(f):
            n += 1
            if ignore_header and n == 1:
                continue
            if not line:
                continue

            ts, value = line.split(',')
            yield Input(dt.datetime.strptime(ts, '%Y-%m-%d %H:%M:%S'), float(value))


def b64_pickle(obj):
    return base64.b64encode(pickle.dumps(obj)).decode('ascii')


def b64_unpickle(b):
    return pickle.loads(base64.b64decode(b.encode('ascii')))