#######

from testbed import submit_python_function_mp


def dc_yearly_mean_AWS(data_xr):
    mask_func = None
    null_val = None
    mask_xr = None
    nd_p = len(data_xr.time)
    # handle the null values
    if (null_val is not None):
        data_xr = data_xr.where(data_xr != null_val)
    # mask product provide
    if (mask_xr is not None):
        nd_m = len(mask_xr.time)
        if (nd_m > 0):
            if (mask_func is not None):
                # returns xr dataset of type bool
                mask = mask_func(mask_xr)
                # get dates of mask and image product
                t1 = data_xr.time.to_series()
                t2 = mask.time.to_series()
                # do the dates of the datasets and pq products match up?
                diff = t2.index.difference(t1.index)
                isect = t2.index.intersection(t1.index)
                # select only those dates that are in main product
                mask = mask.sel(time=isect)
                if (len(diff) != 0):
                    # not all dates of product have matching pq
                    tmp = data_xr.sel(time=diff.tolist())
                    # create a set of dummy masks for dates
                    # which have no pq.  Set all values to True
                    maskB = tmp < tmp.max()
                    # add array of dummy masks to mask derived from pq
                    mask = mask.combine_first(maskB)
                    # where mask values are false set data values to NA...
                    # output is of type float64 are where operation,
                    # returns subset if mask smaller
                data_xr = data_xr.where(mask)
    yearly_average = data_xr.groupby('time.year').mean(dim='time')
    results = {}
    results['blue'] = yearly_average['blue']
    results['red'] = yearly_average['red']
    results['green'] = yearly_average['green']
    return results

query = {'product': 'fc_albers', 'measurements': ['blue', 'red', 'green'], 'x': (144.6826, 144.7826), 'y': (-21.71733, -21.81733)}
a = submit_python_function_mp(dc_yearly_mean_AWS, query, (1000, 1000), 'eetest', 'test')
b = a.ready()

(x: 1067, y: 548, year: 3)

import numpy as np
from datacube.drivers.s3.storage.s3aio.s3lio import S3LIO
s3lio = S3LIO(True, True, None, 30)
data_red = s3lio.get_data_unlabeled('test_red', (3, 1214, 1133), (3, 1000, 1000), np.float64, (slice(0, 3), slice(0, 1214), slice(0, 1133)), 'eetest')
data_blue = s3lio.get_data_unlabeled('test_blue', (3, 1214, 1133), (3, 1000, 1000), np.float64, (slice(0, 3), slice(0, 1214), slice(0, 1133)), 'eetest')
data_green = s3lio.get_data_unlabeled('test_green', (3, 1214, 1133), (3, 1000, 1000), np.float64, (slice(0, 3), slice(0, 1214), slice(0, 1133)), 'eetest')
