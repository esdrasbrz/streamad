import os
import logging
import asyncio
from aioredis import Redis
import faust
from faust.web import View, Request, Response
from htm.bindings.sdr import SDR
from streamad.models import HtmConfig, Input, EncoderInput, ModelMeta
from streamad.models import SpatialPoolerInput
from streamad.encoders import get_time_encoder, get_value_encoder
from streamad.htm import get_spatial_pooler, update_spatial_pooler_state
from streamad.utils import b64_pickle, b64_unpickle
from streamad.config import get_config, set_config


app = faust.App(
    'streamad',
    broker='kafka://' + os.getenv('KAFKA_BROKER', 'localhost:9092')
)
redis = Redis.from_url(os.getenv('REDIS_URL', 'redis://localhost'))

input_topic = app.topic(
    'streamad-input', value_type=Input, key_type=str)
encoder_topic = app.topic(
    'streamad-encoder', value_type=EncoderInput)
spatial_pooler_topic = app.topic(
    'streamad-spatial-pooler', value_type=SpatialPoolerInput, key_type=str)


@app.page('/config/{model_id}')
class Config(View):
    async def post(self, request: Request, model_id: str) -> Response:
        body = await request.json()
        try:
            config = HtmConfig.from_data(body)
            await set_config(redis, model_id, config)

            logging.info('updated configuration for %s', model_id)
            return self.json({
                'message': f'Model {model_id} configured'
            })
        except ValueError as e:
            return self.error(400, str(e))

    async def get(self, request: Request, model_id: str) -> Response:
        config = await get_config(redis, model_id)
        return self.json(config)


## ----
## Streamad Input 
## ----


@app.agent(input_topic)
async def input_agent(input_stream):
    async for model_id, row in input_stream.items():
        config = await get_config(redis, model_id)

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
            sp = await get_spatial_pooler(
                sp_input.encoding_width, sp_input.meta.config.spatial_pooler, model_id, redis)
            sps[model_id] = sp

        # deserialize encoding
        encoding = b64_unpickle(sp_input.encoding)

        # compute the spatial pooling
        active_columns = SDR(sp.getColumnDimensions())
        sp.compute(encoding, True, active_columns)
        active_columns_bytes = b64_pickle(active_columns)


if __name__ == '__main__':
    app.main()