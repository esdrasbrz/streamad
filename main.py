import os
import logging
import time
from aioredis import Redis
import faust
from faust.web import View, Request, Response
from htm.bindings.sdr import SDR
from prometheus_client import start_http_server, Counter, Histogram
from streamad.models import HtmConfig, Input, Output, EncoderInput, ModelMeta
from streamad.models import SpatialPoolerInput, TemporalMemoryInput, AnomalyInput
from streamad.encoders import get_time_encoder, get_value_encoder
from streamad.htm import get_spatial_pooler, get_temporal_memory, get_anomaly_likelihood, update_state
from streamad.utils import b64_pickle, b64_unpickle
from streamad.config import get_config, set_config


_MODEL_COMMIT_INTERVAL_SEC = int(os.getenv('MODEL_COMMIT_INTERVAL_SEC', '60'))
_PROMETHEUS_PORT = int(os.getenv('PROMETHEUS_PORT', '8000'))


app = faust.App(
    'streamad',
    broker='kafka://' + os.getenv('KAFKA_BROKER', 'localhost:9092')
)
redis = Redis.from_url(os.getenv('REDIS_URL', 'redis://localhost'))

input_topic = app.topic(
    'streamad-input', value_type=Input)
encoder_topic = app.topic(
    'streamad-encoder', value_type=EncoderInput, key_type=str)
spatial_pooler_topic = app.topic(
    'streamad-spatial-pooler', value_type=SpatialPoolerInput, key_type=str)
temporal_memory_topic = app.topic(
    'streamad-temporal-memory', value_type=TemporalMemoryInput, key_type=str)
anomaly_topic = app.topic(
    'streamad-anomaly', value_type=AnomalyInput, key_type=str)
output_topic = app.topic(
    'streamad-output', value_type=Output)


# Prometheus metrics

_msg_count = Counter(
    'faust_messages',
    'Count of faust messages processed',
    ['stage'])
_msg_latency = Histogram(
    'faust_message_latency_seconds',
    'Time in seconds to process a message',
    ['stage'],
    buckets=[0.001, 0.0025, 0.005, 0.0075, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10])


# used to save algorithm state
sps = {}
tms = {}
ans = {}


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
    async for row in input_stream:
        with _msg_latency.labels('input').time():
            config = await get_config(redis, row.model_id)

            meta = ModelMeta(config=config, model_id=row.model_id)
            encoder_input = EncoderInput(
                meta=meta, ts=row.ts, value=row.value)

            await encoder_topic.send(value=encoder_input, key=row.model_id)

            _msg_count.labels('input').inc()


## ----
## Encoders
## ----


@app.agent(encoder_topic)
async def encoder(input_stream):
    async for model_id, enc_input in input_stream.items():
        with _msg_latency.labels('encoder').time():
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
                inp=enc_input,
                encoding=encoding_bytes,
                encoding_width=time_enc.size + value_enc.size
            )
            await spatial_pooler_topic.send(key=model_id, value=sp_input)

            _msg_count.labels('encoder').inc()


## ----
## Spatial Pooler
## ----


@app.agent(spatial_pooler_topic)
async def spatial_pooler_agent(input_stream):
    async for model_id, sp_input in input_stream.items():
        with _msg_latency.labels('spatial_pooler').time():
            # load spatial pooler object
            try:
                sp = sps[model_id][0]
                last_commit = sps[model_id][1]
            except KeyError:
                sp = await get_spatial_pooler(
                    sp_input.encoding_width, sp_input.inp.meta.config.spatial_pooler, model_id, redis)
                last_commit = time.time()
                sps[model_id] = (sp, last_commit)

            # deserialize encoding
            encoding = b64_unpickle(sp_input.encoding)

            # compute the spatial pooling
            active_columns = SDR(sp.getColumnDimensions())
            sp.compute(encoding, True, active_columns)
            active_columns_bytes = b64_pickle(active_columns)

            tm_input = TemporalMemoryInput(inp=sp_input.inp, active_columns=active_columns_bytes)
            await temporal_memory_topic.send(key=model_id, value=tm_input)
            
            # commit model
            if time.time() - last_commit >= _MODEL_COMMIT_INTERVAL_SEC:
                logging.info('Commit spatial pooler for model %s', model_id)
                await update_state(sp, model_id, 'spatial-pooler', redis)

                sps[model_id] = (sp, time.time())

            _msg_count.labels('spatial_pooler').inc()


## ----
## Temporal Memory
## ----


@app.agent(temporal_memory_topic)
async def temporal_memory_agent(input_stream):
    async for model_id, tm_input in input_stream.items():
        with _msg_latency.labels('temporal_memory').time():
            # load temporal memory object
            try:
                tm = tms[model_id][0]
                last_commit = tms[model_id][1]
            except KeyError:
                tm = await get_temporal_memory(
                    tm_input.inp.meta.config.spatial_pooler, tm_input.inp.meta.config.temporal_memory, model_id, redis)
                last_commit = time.time()
                tms[model_id] = (tm, last_commit)

            # deserialize encoding
            active_columns = b64_unpickle(tm_input.active_columns)
            tm.compute(active_columns, learn=True)

            an_input = AnomalyInput(inp=tm_input.inp, anomaly=tm.anomaly)
            await anomaly_topic.send(key=model_id, value=an_input)

            # commit model
            if time.time() - last_commit >= _MODEL_COMMIT_INTERVAL_SEC:
                logging.info('Commit temporal memory for model %s', model_id)
                await update_state(tm, model_id, 'temporal-memory', redis)

                tms[model_id] = (tm, time.time())

            _msg_count.labels('temporal_memory').inc()


## ----
## Anomaly
## ----


@app.agent(anomaly_topic)
async def anomaly_agent(input_stream):
    async for model_id, an_input in input_stream.items():
        with _msg_latency.labels('anomaly').time():
            try:
                an = ans[model_id][0]
                last_commit = ans[model_id][1]
            except KeyError:
                an = await get_anomaly_likelihood(
                    an_input.inp.meta.config.anomaly_likelihood, model_id, redis)
                last_commit = time.time()
                ans[model_id] = (an, last_commit)

            # calculate anomaly probability
            like = an.anomalyProbability(an_input.inp.value, an_input.anomaly, an_input.inp.ts)
            log_score = an.computeLogLikelihood(like)

            output = Output(
                model_id=model_id,
                ts=an_input.inp.ts,
                value=an_input.inp.value,
                anomaly_score=log_score)
            await output_topic.send(value=output)

            # commit model
            if time.time() - last_commit >= _MODEL_COMMIT_INTERVAL_SEC:
                logging.info('Commit anomaly likelihood for model %s', model_id)
                await update_state(an, model_id, 'anomaly-likelihood', redis)

                ans[model_id] = (an, time.time())

            _msg_count.labels('anomaly').inc()


# prometheus metrics
start_http_server(_PROMETHEUS_PORT)


if __name__ == '__main__':
    app.main()