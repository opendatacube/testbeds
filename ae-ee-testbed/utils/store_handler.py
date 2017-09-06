'''This module provides manages interaction with a redis store. This module provides manages
 interaction with a redis store.
'''
from __future__ import absolute_import

import logging
from enum import Enum

from redis import StrictRedis
from dill import loads, dumps


FunctionTypes = Enum('FunctionTypes', ('PICKLED', 'TEXT', 'DSL'))
'''Valid function types.

TODO: I don't seem to be able to unpickled these. Pickle/cloudpickle/dill seem to still be
inconsistent in their handling of Enums. The former doesn't do lambdas, the last 2 choked on Enums,
defined in the global or class scopes.
'''


class Function(object):
    # See above: it would be better to use Enum if they can be made to work...
    #PICKLED = 0
    #TEXT = 1
    #DSL = 2
    #TYPES = frozenset((PICKLED, TEXT, DSL))
    #'''Valid function types.'''

    '''Function and its metadata.'''
    def __init__(self, function_type, function):
        if not function_type in FunctionTypes:
            raise ValueError('Invalid function type: %s', function_type)
        self.function_type = function_type
        self.function = function


class Job(object):
    '''Job metadata.'''
    def __init__(self, function_id, data_id, result_ids, ttl, chunk, timestamp):
        self.function_id = function_id
        self.data_id = data_id
        self.result_ids = result_ids
        self.ttl = ttl
        self.chunk = chunk
        self.timestamp = timestamp


class StoreHandler(object):
    '''Main interface to the data store.'''

    DELIMITER = ':'
    K_FUNCTIONS = 'functions'
    K_JOBS = 'jobs'
    K_DATA = 'data'
    K_QUEUED = 'queued'
    K_COMPLETED = 'completed'
    K_CANCELLED = 'cancelled'
    K_COUNT = 'total'

    def __init__(self, hostname='localhost', port=6379, db=0):
        '''Initialise the data store interface.'''
        self.logger = logging.getLogger(self.__class__.__name__)
        self._store = StrictRedis(hostname, port, db)


    def _make_key(self, *parts):
        '''Build a redis key using the agreed delimiter.

        All `parts` are expected to be strings as redis only understand strings.
        '''
        return self.DELIMITER.join(parts)


    def _add_item(self, key, item):
        '''Add an item to its queue list according to  its key type.

        TODO (talk to Peter): Decide whether to use a MULTI/EXEC block. I don't think it is required
        as the incr operation reserves an id, so it doesn't matter if the following operations are
        done in the same transaction.
        Use it for better performance: it should use a single packet.

        '''
        # Get new item id as incremental integer
        item_id = self._store.incr(self._make_key(key, self.K_COUNT))

        # Add pickled item to relevant list of items
        self._store.set(self._make_key(key, str(item_id)), dumps(item, byref=True))

        if key == self.K_JOBS:
            # Add job id to queued list of items
            self._store.rpush(self._make_key(key, self.K_QUEUED), item_id)

        return item_id


    def _queued_items(self, key):
        '''Returns the list of item ids currently in the queue, as integers.'''
        return [int(item_id) for item_id \
                in self._store.lrange(self._make_key(key, self.K_QUEUED), 0, -1)]


    def _get_item(self, key, item_id):
        '''Retrieve a specific item by its key type and id.'''
        return loads(self._store.get(self._make_key(key, str(item_id))))


    def add_job(self, function_type, function, data, ttl=-1, chunk=None):
        '''Add an new function and its data to the queue.

        Both get serialised for storage. `data` is optional in case a job doesn't have any explicit
        data attached to it.
        '''
        if not function:
            raise ValueError('Cannot add job without function')
        func = Function(function_type, function)
        function_id = self._add_item(self.K_FUNCTIONS, func)
        data_id = self._add_item(self.K_DATA, data) if data else -1
        timestamp = None # TODO: do we use redis or worker time?
        job = Job(function_id, data_id, [], ttl, chunk, timestamp)
        return self._add_item(self.K_JOBS, job)


    def add_result(self, result):
        '''
        if result is a list:
            recursive add_result(item)
        else:
            add result item
        '''

    def set_job_status(self):
        pass

    def queued_jobs(self):
        '''List jobs currently in the queue.'''
        return self._queued_items(self.K_JOBS)


    def get_job(self, job_id):
        '''Retrieve a specific Job.'''
        return self._get_item(self.K_JOBS, job_id)


    def get_function(self, function_id):
        '''Retrieve a specific function.'''
        return self._get_item(self.K_FUNCTIONS, function_id)


    def get_data(self, data_id):
        '''Retrieve a specific data.'''
        return self._get_item(self.K_DATA, data_id)


    def __str__(self):
        '''Returns information about the store. For now, all its keys.'''
        return 'Store keys: {}'.format(
            sorted([key.decode('utf-8') for key in self._store.keys()]))



if __name__ == '__main__':
    # Simple test

    # Set up a test logger
    logging.basicConfig(level=logging.DEBUG, format='%(levelname)s | %(message)s')
    logger = logging.getLogger('__main__')

    # Create a new store handler and flush the redis DB. Normally, the _store variable should never
    # be accessed directly. Flushing will have to be managed carefully (if any flushing at all).
    sh = StoreHandler('redis')
    sh._store.flushdb()

    # Create 2 users with 6 jobs each. The function simply returns a string denoting the user and
    # job number. Same for the data.
    funcTypes = list(FunctionTypes)
    for user in range(2):
        user_id = 'user{:03d}'.format(user)
        for job in range(6):
            function = lambda user=user, job=job: 'User {:03d}, job {:03d}'.format(user, job)
            data = 'Data for {:03d}-{:03d}'.format(user, job)
            # Asign function type to same number as job % 3, for testing only!
            sh.add_job(funcTypes[job % 3], function, data)

    # List all keys in the store
    logger.debug(sh)

    # List all queued jobs
    job_ids = sh.queued_jobs()
    logger.debug('Job IDs: %s', job_ids)

    # Retrieve all job functions and data
    for job_id in job_ids:
        job = sh.get_job(job_id)
        function_id = job.function_id
        function = sh.get_function(function_id)
        data_id = job.data_id
        data = sh.get_data(data_id)
        logger.debug('Job #%3s has function #%3s (%7s): "%s" and data #%3s: "%s"',
                     job_id, function_id, function.function_type.name,
                     function.function(), data_id, data)
