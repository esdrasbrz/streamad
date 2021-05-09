from functools import lru_cache
from htm.encoders.rdse import RDSE, RDSE_Parameters
from htm.encoders.date import DateEncoder


@lru_cache(maxsize=None)
def get_time_encoder(time_of_day, weekend):
    return DateEncoder(timeOfDay=time_of_day, weekend=weekend)


@lru_cache(maxsize=None)
def get_value_encoder(size, sparsity, resolution):
    params = RDSE_Parameters()
    params.size = size
    params.sparsity = sparsity
    params.resolution = resolution

    return RDSE(params)
