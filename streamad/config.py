from streamad.models import HtmConfig


async def get_config(redis, model_id):
    key = f'streamad/config/{model_id}'
    config_bytes = await redis.get(key)

    if not config_bytes:
        config = HtmConfig()
        await redis.set(key, config.dumps(serializer='pickle'))
    else:
        config = HtmConfig.loads(config_bytes, serializer='pickle')
    
    return config


async def set_config(redis, model_id, config):
    key = f'streamad/config/{model_id}'
    await redis.set(key, config.dumps(serializer='pickle'))
