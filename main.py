import os
import logging
from datetime import timedelta
import faust
from faust.web import View, Request, Response
from streamad.models import HtmConfig


app = faust.App(
    'streamad',
    broker='kafka://' + os.getenv('KAFKA_BROKER', 'localhost:9092')
)

config_table = app.Table(
    'config', key_type=str, value_type=HtmConfig)
update_config_topic = app.topic(
    'streamad-update-config', key_type=str, value_type=HtmConfig)


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