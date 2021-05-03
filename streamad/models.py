from typing import Tuple
import faust


class EncoderValueConfig(faust.Record, validation=True):
    resolution: float = 0.001
    size: int = 4000
    sparsity: float = 0.10


class EncoderTimeConfig(faust.Record, validation=True):
    time_of_day: Tuple[int, float] = (21, 9.49)
    weekend: int = 0


class EncoderConfig(faust.Record, validation=True):
    value: EncoderValueConfig = EncoderValueConfig()
    time: EncoderTimeConfig = EncoderTimeConfig()


class SpatialPoolerConfig(faust.Record, validation=True):
    boost_strength: float = 0.0
    column_count: int = 2048
    local_area_aensity: float = 40/2048
    potential_pct: float = 0.4
    syn_perm_active_inc: float = 0.003
    syn_perm_connected: float = 0.2
    syn_perm_inactive_dec: float = 0.0005


class TemporalMemoryConfig(faust.Record, validation=True):
    activation_threshold: float = 13
    cells_per_column: int = 32
    initial_perm: float = 0.21
    max_segments_per_cell: int = 128
    max_synapses_per_segment: int = 32
    min_threshold: int = 10
    new_synapse_count: int = 20
    permanence_dec: float = 0.1
    permanence_inc: float = 0.1


class AnomalyLikelihoodConfig(faust.Record, validation=True):
    probationary_pct: float = 0.1
    reestimation_period: int = 100


class HtmConfig(faust.Record, validation=True):
    encoder: EncoderConfig = EncoderConfig()
    spatial_pooler: SpatialPoolerConfig = SpatialPoolerConfig()
    temporal_memory: TemporalMemoryConfig = TemporalMemoryConfig()
    anomaly_likelihood: AnomalyLikelihoodConfig = AnomalyLikelihoodConfig()