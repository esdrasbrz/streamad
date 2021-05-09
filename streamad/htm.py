from functools import lru_cache
from htm.bindings.algorithms import SpatialPooler
from streamad.models import SpatialPoolerConfig


@lru_cache(maxsize=None)
def get_spatial_pooler(encoding_width: int, config: SpatialPoolerConfig):
    return SpatialPooler(
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