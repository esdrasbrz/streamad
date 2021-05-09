import os
import logging
import faust
from faust.web import View, Request, Response
from htm.bindings.sdr import SDR
from streamad.models import HtmConfig, Input, EncoderInput, ModelMeta, SpatialPoolerInput
from streamad.encoders import get_time_encoder, get_value_encoder
from streamad.htm import get_spatial_pooler
from streamad.utils import b64_pickle, b64_unpickle


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
    'streamad-encoder', value_type=EncoderInput)
spatial_pooler_topic = app.topic(
    'streamad-spatial-pooler', value_type=SpatialPoolerInput, key_type=str)


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
        encoding_bytes = b64_pickle(encoding)

        sp_input = SpatialPoolerInput(
            meta=enc_input.meta,
            encoding=encoding_bytes,
            encoding_width=time_enc.size + value_enc.size
        )
        await spatial_pooler_topic.send(key=enc_input.meta.model_id, value=sp_input)


## ----
## Spatial Pooler
## ----


@app.agent(spatial_pooler_topic)
async def spatial_pooler_agent(input_stream):
    sps = {}

    async for model_id, sp_input in input_stream.items():
        # load spatial pooler object
        if model_id in sps:
            sp = sps[model_id]
        else:
            sp = get_spatial_pooler(
                sp_input.encoding_width, sp_input.meta.config.spatial_pooler)
            sps[model_id] = sp

        # deserialize encoding
        encoding = b64_unpickle(sp_input.encoding)

        # compute the spatial pooling
        active_columns = SDR(sp.getColumnDimensions())
        sp.compute(encoding, True, active_columns)
        active_columns_bytes = b64_pickle(active_columns)


if __name__ == '__main__':
    app.main()