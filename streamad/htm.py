import pickle
from functools import lru_cache
from htm.bindings.algorithms import SpatialPooler
from streamad.models import SpatialPoolerConfig


async def get_spatial_pooler(encoding_width: int, config: SpatialPoolerConfig, model_id: str, redis):
    key = f'streamad/spatial-pooler/{model_id}'
    sp_bytes = await redis.get(key)
    if sp_bytes:
        sp = pickle.loads(sp_bytes)
    else:
        sp = SpatialPooler(
            inputDimensions=(encoding_width,),
            columnDimensions=(config.column_count,),
            potentialPct=config.potential_pct,
            potentialRadius=encoding_width,
            globalInhibition=True,
            localAreaDensity=config.local_area_density,
            synPermInactiveDec=config.syn_perm_inactive_dec,
            synPermActiveInc=config.syn_perm_active_inc,
            synPermConnected=config.syn_perm_connected,
            boostStrength=config.boost_strength,
            wrapAround=True
        )
        await redis.set(key, pickle.dumps(sp))

    return sp


async def update_spatial_pooler_state(sp, model_id, redis):
    key = f'streamad/spatial-pooler/{model_id}'
    await redis.set(key, pickle.dumps(sp))