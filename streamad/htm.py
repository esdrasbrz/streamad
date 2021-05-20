import pickle
from htm.bindings.algorithms import SpatialPooler, TemporalMemory
from htm.algorithms.anomaly_likelihood import AnomalyLikelihood
from streamad.models import AnomalyLikelihoodConfig, SpatialPoolerConfig, TemporalMemoryConfig


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


async def get_temporal_memory(sp_config: SpatialPoolerConfig, tm_config: TemporalMemoryConfig, model_id: str, redis):
    key = f'streamad/temporal-memory/{model_id}'
    tm_bytes = await redis.get(key)
    if tm_bytes:
        tm = pickle.loads(tm_bytes)
    else:
        tm = TemporalMemory(
            columnDimensions=[sp_config.column_count,],
            cellsPerColumn=tm_config.cells_per_column,
            activationThreshold=tm_config.activation_threshold,
            initialPermanence=tm_config.initial_perm,
            connectedPermanence=sp_config.syn_perm_connected,
            minThreshold=tm_config.min_threshold,
            maxNewSynapseCount=tm_config.new_synapse_count,
            permanenceIncrement=tm_config.permanence_inc,
            permanenceDecrement=tm_config.permanence_dec,
            predictedSegmentDecrement=0.0,
            maxSegmentsPerCell=tm_config.max_segments_per_cell,
            maxSynapsesPerSegment=tm_config.max_synapses_per_segment
        )
        await redis.set(key, pickle.dumps(tm))

    return tm


async def get_anomaly_likelihood(an_config: AnomalyLikelihoodConfig, model_id: str, redis):
    key = f'streamad/anomaly-likelihood/{model_id}'
    an_bytes = await redis.get(key)
    if an_bytes:
        an = pickle.loads(an_bytes)
    else:
        an = AnomalyLikelihood(
            learningPeriod=an_config.learning_period,
            estimationSamples=an_config.estimation_samples,
            reestimationPeriod=an_config.reestimation_period
        )
        await redis.set(key, pickle.dumps(an))

    return an


async def update_state(obj, model_id, state_id, redis):
    key = f'streamad/{state_id}/{model_id}'
    await redis.set(key, pickle.dumps(obj))
