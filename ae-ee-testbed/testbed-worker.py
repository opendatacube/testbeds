from celery import Celery
import decimal
import datacube
import itertools
from itertools import repeat, product, tee
import xarray
from pprint import pprint
import zstd

app = Celery('tasks', broker='redis://52.63.99.100:6379/0', backend='redis://52.63.99.100:6379/0')

app.conf.update(
   task_serializer='pickle',
   result_serializer='pickle',
   accept_content=['pickle'],
   broker_transport_options={'confirm_publish': True},
   task_acks_late=True,
   worker_prefetch_multiplier=1)


dc = datacube.Datacube(app='dc-example')
s3io = datacube.drivers.s3.storage.s3aio.s3io.S3IO(True, None, 2)


##################
# Serial version #
##################

@app.task
def run_python_function(func, data, uid, bucket=None, base_name=None, chunk=None, chunk_id=None, *args, **kwargs):
    if not chunk:
        chunk_id = 0
    '''
    if isinstance(data, dict):
        tmp = {}
        for k, v in data.items():
            tmp[k] = get_data(v, chunk)
        data = tmp
    else:
        data = get_data(data, chunk)
    '''
    data = get_data(data, chunk)
    from pickle import loads
    func = loads(func)
    results = func(data, *args, **kwargs)
    pprint(results)
    print("Python function executed")
    s3_objects = []
    if bucket and base_name:
        s3_objects = put_results_in_s3(results, base_name, bucket, chunk_id)
    return s3_objects


def submit_python_function(func, data, bucket=None, key=None, chunk=None, chunk_id=None, *args, **kwargs):
    from cloudpickle import dumps
    from uuid import uuid4
    uid = str(uuid4())
    func = dumps(func)
    return run_python_function.delay(func, data, uid, bucket, key, chunk, chunk_id, *args, **kwargs)


@app.task
def run_python_code(func, data, uid, bucket=None, base_name=None, chunk=None, chunk_id=None, *args, **kwargs):
    if not chunk:
        chunk_id = 0
    data = get_data(data, chunk)
    print(func)
    exec(func)
    pprint(results)
    print("Python code executed")
    s3_objects = []
    if bucket and base_name:
        s3_objects = put_results_in_s3(results, base_name, bucket, chunk_id)
    return s3_objects


def submit_python_code(func, data, bucket=None, key=None, chunk=None, chunk_id=None, *args, **kwargs):
    from uuid import uuid4
    uid = str(uuid4())
    return run_python_code.delay(func, data, uid, bucket, key, chunk, chunk_id, *args, **kwargs)


###################
# Chunked version #
###################

@app.task
def run_python_function_mp(func, data, uid, chunk_size, bucket, base_name, *args, **kwargs):
    # do data decomposition
    print(func, data, uid, base_name)
    grouped, geobox, measurements_values = get_metadata(**data)
    indices = create_indices(geobox.shape, chunk_size, base_name)

    # launch workers
    results = []
    for idx in indices:
        results.append(run_python_function.delay(func, data, uid, bucket, base_name, idx[1], idx[2], *args, **kwargs))

    print("Python function executed")
    return results, grouped.shape + geobox.shape


def submit_python_function_mp(func, data, chunk_size, bucket, base_name, *args, **kwargs):
    from cloudpickle import dumps
    from uuid import uuid4
    uid = str(uuid4())
    func = dumps(func)
    return run_python_function_mp.delay(func, data, uid, chunk_size, bucket, base_name, *args, **kwargs)


@app.task
def run_python_code_mp(func, data, uid, chunk_size, bucket, base_name, *args, **kwargs):
    # do data decomposition
    print(func, data, uid, base_name)
    grouped, geobox, measurements_values = get_metadata(**data)
    indices = create_indices(geobox.shape, chunk_size, base_name)

    # launch workers
    results = []
    for idx in indices:
        results.append(run_python_code.delay(func, data, uid, bucket, base_name, idx[1], idx[2], *args, **kwargs))

    print("Python code executed")
    return results, grouped.shape + geobox.shape


def submit_python_code_mp(func, data, chunk_size, bucket, base_name, *args, **kwargs):
    from uuid import uuid4
    uid = str(uuid4())
    return run_python_code_mp.delay(func, data, uid, chunk_size, bucket, base_name, *args, **kwargs)


############
# Get Data #
############

def get_data(query, chunk=None):
    # dc = datacube.Datacube(app='dc-example')
    if chunk is None:
        return dc.load(**query)
    else:
        grouped, geobox, measurements_values = get_metadata(**query)
        return dc.load_data(grouped, geobox[chunk], measurements_values, driver_manager=dc.driver_manager, use_threads=True)


################
# Persist Data #
################

def put_results_in_s3(results, base_name, bucket, chunk_id):
    s3_objects = []
    # s3io = datacube.drivers.s3.storage.s3aio.s3io.S3IO(True, None, 2)
    for name, data in results.items():
        s3_key = '_'.join([base_name, name, str(chunk_id)])
        s3_object = {}
        s3_object['name'] = name
        s3_object['bucket'] = bucket
        s3_object['key'] = s3_key
        s3_object['shape'] = data.shape
        s3_object['dtype'] = data.dtype
        s3_objects.append(s3_object)
        print("Persisting to ", bucket, s3_key)
        data = bytes(data.data)
        cctx = zstd.ZstdCompressor(level=9, write_content_size=True)
        data = cctx.compress(data)
        s3io.put_bytes(bucket, s3_key, data)
    print("Result data persisted to S3 bucket: ", bucket)
    return s3_objects


################
# Get Metadata #
################

def get_metadata(product=None, measurements=None, **query):
    # dc = datacube.Datacube(app='dc-example')
    observations = dc.find_datasets(product=product, **query)
    if not observations:
        return xarray.Dataset()
    grid_spec = dc.index.products.get_by_name(product).grid_spec
    if not grid_spec or not grid_spec.crs:
        raise RuntimeError("Product has no default CRS. Must specify 'output_crs' and 'resolution'")
    crs = grid_spec.crs
    if not grid_spec.resolution:
        raise RuntimeError("Product has no default resolution. Must specify 'resolution'")
    resolution = grid_spec.resolution
    align = grid_spec.alignment
    geobox = datacube.utils.geometry.GeoBox.from_geopolygon(datacube.api.query.query_geopolygon(**query) or get_bounds(observations, crs),
                                                            resolution, crs, align)
    group_by = datacube.api.query.query_group_by(**query)
    grouped = dc.group_datasets(observations, group_by)
    measurements = dc.index.products.get_by_name(product).lookup_measurements(measurements)
    measurements = datacube.api.core.set_resampling_method(measurements, None)
    return grouped, geobox, measurements.values()

# grouped, geobox, measurements_values = get_metadata(product='ls5_nbar_albers', x=(149.25, 149.35), y=(-35.25, -35.35))

# result = dc.load_data(grouped, geobox, measurements_values, driver_manager=dc.driver_manager, use_threads=True)


####################
# integer chunking # - GOOD
####################

def chunk_indices_1d(begin, end, step, bound_slice=None, return_as_shape=False):
    if bound_slice is None:
        for i in range(begin, end, step):
            if return_as_shape:
                yield min(end, i + step) - i
            else:
                yield slice(i, min(end, i + step))
    else:
        bound_begin = bound_slice.start
        bound_end = bound_slice.stop
        end = min(end, bound_end)
        for i in range(begin, end, step):
            if i < bound_begin and i+step <= bound_begin:
                continue
            if return_as_shape:
                yield min(end, i + step) - max(i, bound_begin)
            else:
                yield slice(max(i, bound_begin), min(end, i + step))


def chunk_indices_nd(shape, chunk, array_slice=None, return_as_shape=False):
    if array_slice is None:
        array_slice = repeat(None)
    var1 = map(chunk_indices_1d, repeat(0), shape, chunk, array_slice, repeat(return_as_shape))
    return product(*var1)


def create_indices(shape, chunk_size, base_name, spread=False):
    idx = list(chunk_indices_nd(shape, chunk_size))
    chunk_ids = [i for i in range(len(idx))]
    keys = [base_name+'_'+str(i) for i in chunk_ids]
    if spread:
        keys = [hashlib.md5(k.encode('utf-8')).hexdigest()[0:6] + '_' + k for k in keys]
    return list(zip(keys, idx, chunk_ids))

# a = create_indices((10, 10, 10), (3, 3, 11), "array")


##################
# float chunking # - BAD
##################

def pairwise(iterable):
    a, b = itertools.tee(iterable)
    next(b, None)
    return zip(a, b)


def chunk_1d(begin, end, steps):
    return pairwise(np.linspace(begin, end, steps+1))


def chunk_nd(begin, end, chunk):
    var1 = map(chunk_1d, begin, end, chunk)
    return itertools.product(*var1)

# chunks = list(chunk_nd((0, 0, 0), (1, 1, 1), (10, 10, 10)))
# a = zip(range(0, len(chunks)), chunks)
