import os
import logging
import asyncio
import faust
from faust.web import View, Request, Response
from faust.cli import option
from streamad.models import HtmConfig, Input
from streamad.utils import parse_csv_data


app = faust.App(
    'streamad',
    broker='kafka://' + os.getenv('KAFKA_BROKER', 'localhost:9092')
)

config_table = app.Table(
    'config', key_type=str, value_type=HtmConfig)
update_config_topic = app.topic(
    'streamad-update-config', key_type=str, value_type=HtmConfig)
input_topic = app.topic(
    'streamad-input', key_type=str, value_type=Input)


@app.agent(update_config_topic)
async def update_config(config_stream):
    async for model_id, config in config_stream.items():
        config_table[model_id] = config


@app.page('/config/{model_id}')
class Config(View):
    async def post(self, request: Request, model_id: str) -> Response:
        body = await request.json()
        try:
            config = HtmConfig.from_data(body)
            await update_config.cast(value=config, key=model_id)

            logging.info('updated configuration for %s', model_id)
            return self.json({
                'message': f'Model {model_id} configured'
            })
        except ValueError as e:
            return self.error(400, str(e))

    @app.table_route(table=config_table, match_info='model_id')
    async def get(self, request: Request, model_id: str) -> Response:
        if model_id in config_table:
            config = config_table[model_id]
            return self.json(config)
        return self.error(404, f'Model {model_id} not configured')


@app.agent(input_topic)
async def input_agent(input_stream):
    async for model_id, row in input_stream.items():
        logging.info('%s -> %s', model_id, row)


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