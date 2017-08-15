def check_credentials():
    import boto3
    session = boto3.Session()
    credentials = session.get_credentials()
    if credentials is None:
        return False
    else:
        import botocore
        try:
            s3 = boto3.resource('s3')
            r = list(s3.buckets.limit(1))[0].name
        except:
            return False
    return True


def check_ready(task_list):
    return all([t.ready() for t in task_list])

#######

from testbed import submit_python_function
def foo(data):
    print(data['blue'])
    print(data['red'])
    results = {}
    results['blue'] = data['blue']
    results['red'] = data['red']
    return results

query = {'product': 'ls5_nbar_albers', 'measurements': ['blue', 'red'], 'x': (149.25, 149.35), 'y': (-35.25, -35.35)}
a = submit_python_function(foo, query, 'eetest', 'test')
b = a.ready()

import numpy as np
from datacube.drivers.s3.storage.s3aio.s3aio import S3AIO
s3aio = S3AIO(True, True, None, 2)
data_red = s3aio.get_slice_by_bbox((slice(0, 12), slice(0, 490), slice(0, 421)), (12, 490, 421), np.int16, 'eetest', 'test_red_0')
data_blue = s3aio.get_slice_by_bbox((slice(0, 12), slice(0, 490), slice(0, 421)), (12, 490, 421), np.int16, 'eetest', 'test_blue_0')

#######

from testbed import submit_python_function_mp
def foo(data):
    print(data['blue'])
    print(data['red'])
    results = {}
    results['blue'] = data['blue']
    results['red'] = data['red']
    return results

query = {'product': 'ls5_nbar_albers', 'measurements': ['blue', 'red'], 'x': (149.25, 149.35), 'y': (-35.25, -35.35)}
a = submit_python_function_mp(foo, query, (100, 100), 'eetest', 'test')
b = a.get()

import numpy as np
from datacube.drivers.s3.storage.s3aio.s3lio import S3LIO
s3lio = S3LIO(True, True, None, 30)
data_red = s3lio.get_data_unlabeled('test_red', (12, 490, 421), (12, 100, 100), np.int16, (slice(0, 12), slice(0, 490), slice(0, 421)), 'eetest')
data_blue = s3lio.get_data_unlabeled('test_blue', (12, 490, 421), (12, 100, 100), np.int16, (slice(0, 12), slice(0, 490), slice(0, 421)), 'eetest')

#######

from testbed import submit_python_code

foo= \
"""
print(data['blue'])
print(data['red'])
global results
results = {}
results['blue'] = data['blue']
results['red'] = data['red']
"""
query = {'product': 'ls5_nbar_albers', 'measurements': ['blue', 'red'], 'x': (149.25, 149.35), 'y': (-35.25, -35.35)}
a = submit_python_code(foo, query, 'eetest', 'test')
b = a.ready()

import numpy as np
from datacube.drivers.s3.storage.s3aio.s3aio import S3AIO
s3aio = S3AIO(True, True, None, 2)
data_red = s3aio.get_slice_by_bbox((slice(0, 12), slice(0, 490), slice(0, 421)), (12, 490, 421), np.int16, 'eetest', 'test_red_0')
data_blue = s3aio.get_slice_by_bbox((slice(0, 12), slice(0, 490), slice(0, 421)), (12, 490, 421), np.int16, 'eetest', 'test_blue_0')

#######

from testbed import submit_python_code_mp

foo= \
"""
print(data['blue'])
print(data['red'])
global results
results = {}
results['blue'] = data['blue']
results['red'] = data['red']
"""
query = {'product': 'ls5_nbar_albers', 'measurements': ['blue', 'red'], 'x': (149.25, 149.35), 'y': (-35.25, -35.35)}
a = submit_python_code_mp(foo, query, (100, 100), 'eetest', 'test')
b = a.get()

import numpy as np
from datacube.drivers.s3.storage.s3aio.s3lio import S3LIO
s3lio = S3LIO(True, True, None, 30)
data_red = s3lio.get_data_unlabeled('test_red', (12, 490, 421), (12, 100, 100), np.int16, (slice(0, 12), slice(0, 490), slice(0, 421)), 'eetest')
data_blue = s3lio.get_data_unlabeled('test_blue', (12, 490, 421), (12, 100, 100), np.int16, (slice(0, 12), slice(0, 490), slice(0, 421)), 'eetest')

#######

from testbed import submit_python_code

code= \
"""
print(data['blue'])
print(data['red'])
"""
query = {'product': 'ls5_nbar_albers', 'measurements': ['blue', 'red'], 'x': (149.25, 149.35), 'y': (-35.25, -35.35)}
a = submit_python_code(code, query)
b = a.get()

#######

from testbed import submit_python_code_mp

code= \
"""
print(data['blue'])
print(data['red'])
"""
query = {'product': 'ls5_nbar_albers', 'measurements': ['blue', 'red'], 'x': (149.25, 149.35), 'y': (-35.25, -35.35)}
a = submit_python_code_mp(code, query, 'test')
b = a.get()

##############################################################

from testbed import submit_python_code

code= \
"""
import datacube
dc = datacube.Datacube(app='dc-example')
nbar = dc.load(product='ls5_nbar_albers', x=(149.25, 149.35), y=(-35.25, -35.35), use_threads=True)
print(nbar)
"""

submit_python_code(code)



from testbed import submit_python_code

code= \
"""
import datacube
dc = datacube.Datacube(app='dc-example')
nbar = dc.load(product='ls5_nbar_albers', x=(149.25, 149.35), y=(-35.25, -35.35), use_threads=True)
print(nbar)

from datacube.ndexpr import NDexpr
ne = NDexpr()
ne.set_ae(False)

blue = nbar.blue
result = ne.evaluate("median(band, 0)")
print(result)
"""

submit_python_code(code)
