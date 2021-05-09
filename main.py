import os
import logging
import pickle
import faust
from faust.web import View, Request, Response
from htm.bindings.sdr import SDR
from streamad.models import HtmConfig, Input, EncoderInput, ModelMeta
from streamad.encoders import get_time_encoder, get_value_encoder


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
encoder_topic = app.topic(
    'streamad-encoder-topic', value_type=EncoderInput)


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


## ----
## Encoders
## ----


@app.agent(encoder_topic)
async def encoder(input_stream):
    async for enc_input in input_stream:
        time_enc = get_time_encoder(
            tuple(enc_input.meta.config.encoder.time.time_of_day),
            enc_input.meta.config.encoder.time.weekend)
        value_enc = get_value_encoder(
            enc_input.meta.config.encoder.value.size,
            enc_input.meta.config.encoder.value.sparsity,
            enc_input.meta.config.encoder.value.resolution)

        time_sdr = time_enc.encode(enc_input.ts)
        value_sdr = value_enc.encode(enc_input.value)

        # concatenate the encoding
        encoding = SDR(time_enc.size + value_enc.size).concatenate([value_sdr, time_sdr])
        encoding_bytes = pickle.dumps(encoding)

        logging.info('encoding finished, size = %d', len(encoding_bytes))


## ----
## Streamad Input 
## ----


@app.agent(input_topic)
async def input_agent(input_stream):
    async for model_id, row in input_stream.items():
        # check if model_id is configured
        if model_id in config_table:
            config = config_table[model_id]
        else:
            # start the default configuration
            config = HtmConfig()
            config_table[model_id] = config

        meta = ModelMeta(config=config, model_id=model_id)
        encoder_input = EncoderInput(
            meta=meta, ts=row.ts, value=row.value)

        await encoder_topic.send(value=encoder_input)


if __name__ == '__main__':
    app.main()