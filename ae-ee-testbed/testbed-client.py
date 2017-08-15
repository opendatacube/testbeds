from celery import Celery

app = Celery('tasks', broker='redis://52.63.99.100:6379/0', backend='redis://52.63.99.100:6379/0')

app.conf.update(
   task_serializer='pickle',
   result_serializer='pickle',
   accept_content=['pickle'])


@app.task
def run_python_function(func, data, uid, bucket=None, base_name=None, chunk=None, chunk_id=None, *args, **kwargs):
    pass


@app.task
def run_python_code(func, data, uid, bucket=None, base_name=None, chunk=None, chunk_id=None, *args, **kwargs):
    pass


@app.task
def run_python_function_mp(func, data, uid, chunk_size, bucket, base_name, *args, **kwargs):
    pass


@app.task
def run_python_code_mp(func, data, uid, chunk_size, bucket, base_name, *args, **kwargs):
    pass


def submit_python_function(func, data, bucket=None, key=None, chunk=None, chunk_id=None, *args, **kwargs):
    from cloudpickle import dumps
    from uuid import uuid4
    uid = str(uuid4())
    func = dumps(func)

    #import redis
    #r = redis.StrictRedis(host='172.31.5.152', port=6379)
    #r.set('_'.join([uid, "state"]), 0)
    return run_python_function.delay(func, data, uid, bucket, key, chunk, chunk_id, *args, **kwargs)


def submit_python_function_mp(func, data, chunk_size, bucket, base_name, *args, **kwargs):
    from cloudpickle import dumps
    from uuid import uuid4
    uid = str(uuid4())
    func = dumps(func)
    return run_python_function_mp.delay(func, data, uid, chunk_size, bucket, base_name, *args, **kwargs)


def submit_python_code(func, data, bucket=None, key=None, chunk=None, chunk_id=None, *args, **kwargs):
    from uuid import uuid4
    uid = str(uuid4())
    return run_python_code.delay(func, data, uid, bucket, key, chunk, chunk_id, *args, **kwargs)


def submit_python_code_mp(func, data, chunk_size, bucket, base_name, *args, **kwargs):
    from uuid import uuid4
    uid = str(uuid4())
    return run_python_code_mp.delay(func, data, uid, chunk_size, bucket, base_name, *args, **kwargs)


def check_ready(task_list):
    return all([t.ready() for t in task_list])
