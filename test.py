import os
import asyncio
import faust
from faust.cli import option
from streamad.models import Input
from streamad.utils import parse_csv_data


app = faust.App(
    'streamad',
    broker='kafka://' + os.getenv('KAFKA_BROKER', 'localhost:9092')
)
input_topic = app.topic(
    'streamad-input', value_type=Input, key_type=str)


@app.command(
    option('--path',
           type=str, default=None,
           help='Path with csv data to be produced'),
)
async def produce(self, path) -> None:
    if path is None:
        self.say("You must provide the --path option")

    async def produce_values(model_id, values):
        async for value in values:
            await input_topic.send(key=model_id, value=value)

    files = os.listdir(path)
    produce_args = ((fn.lower(), os.path.join(path, fn)) for fn in files)

    await asyncio.gather(*[
        produce_values(model_id, parse_csv_data(filepath))
        for model_id, filepath in produce_args
    ])


if __name__ == '__main__':
    app.main()