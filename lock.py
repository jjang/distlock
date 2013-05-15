import datetime
import functools
import hashlib
import random

from .pickle_utils import picklable_callable


HANDLE_KWARG = 'lock_handle'


def lock(keygen, timeout, redis_factory, cb=None, cb_args=tuple()):
    '''
        Apply this decorator to a function that you wish to be protected by a mutex.  The mutex applied has
        additional functionality that one might consider as re-entrant.  If the decorated function has an
        argument named 'lock_handle', the argument will be populated with the Lock object used to apply the
        mutex.  Refer to the docstring on the Lock class for details on how this object can be interacted
        with.
    '''
    def deco(f):
        wants_handle = HANDLE_KWARG in f.func_code.co_varnames

        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            with Lock(keygen(args, kwargs), timeout, redis_factory, cb=cb, cb_args=cb_args) as handle:
                if wants_handle:
                    kwargs[HANDLE_KWARG] = handle
                return f(*args, **kwargs)
        return wrapper

    return deco


def lock_command(lock_handle_extractor, command):
    '''
        Apply this decorator to a function to ensure that the command specified executes on method return.
        The first argument to the decorator is a function that takes the arguments passed to the wrapped
        function and returns a lock handle.
    '''
    def deco(f):

        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            try:
                f(*args, **kwargs)
            finally:
                getattr(lock_handle_extractor(*args, **kwargs), command)()
        return wrapper

    return deco


lock.decr = lambda lhe: lock_command(lhe, 'decr')
lock.incr = lambda lhe: lock_command(lhe, 'incr')


class Lock(object):
    '''
        This lock provides functionality such that it can be used to coordinate locking of resources in
        disparate locations.  The lock centers around a counter.  When the counter is greater than 0 the
        lock is active.  When the counter becomes 0 the lock unlocks.  The lock can be used as a standard
        mutex by applying is as a context manager.  In such an event the counter goes to 1 on entering the
        context manager and goes to 0 on exiting the context manager, thus unlocking the mutex.  However,
        if when inside the context manager 'incr' is called on the lock handle, the lock will not go to
        0 on exiting the context manager and the lock will remain active. ... **more docs to come**
    '''

    redis_template = 'lock:control:%s'
    redis_counter_template = 'lock:counter:%s'

    def __init__(self, key, timeout, redis_factory, cb=None, cb_args=tuple(), notheld_errback=None):
        self.key = key
        self.timeout = timeout

        self.created_at = datetime.datetime.utcnow()

        # On unlock the following callback will be executed against the specified callback arguments
        self.cb = cb
        self.cb_args = cb_args

        # Can bind a notheld errback
        self.notheld_errback = notheld_errback

        # Instatiate a Redis client via the factory
        self.redis_factory = redis_factory
        self._redis = self.redis_factory()
        self._redis_key = Lock.redis_template % self.key

        # We apply the integer count against the salted key.  See the class docstring.
        self._salted_key = Lock.salt_key(key)
        self._redis_counter_key = Lock.redis_counter_template % self._salted_key

    def __getstate__(self):
        state = dict(self.__dict__)
        del state['_redis']  # Redis client can't be pickled
        return state

    def __setstate__(self, state):
        self.__dict__ = state
        self._redis = self.redis_factory()  # Reinstantiate Redis client

    def _get_cb(self):
        return self._cb

    def _set_cb(self, cb):
        self._cb = picklable_callable(cb)

    cb = property(_get_cb, _set_cb)

    def _get_notheld_errback(self):
        return self._notheld_errback

    def _set_notheld_errback(self, notheld_errback):
        self._notheld_errback = picklable_callable(notheld_errback)

    notheld_errback = property(_get_notheld_errback, _set_notheld_errback)

    @staticmethod
    def salt_key(key):
        '''Method applies a salt to the key and hashes it.'''
        return hashlib.md5('%s%s' % (key, random.random())).hexdigest()

    @property
    def active(self):
        return self._redis.get(self._redis_key) == self._salted_key

    @property
    def count(self):
        return self._redis.get(self._redis_counter_key)

    def require(self):
        if not self.active:
            if self.notheld_errback:
                self.notheld_errback(*self.cb_args)
            raise LockNotHeld('Lock for key "%s" instantiated at "%s" is no longer valid' % (self.key, self.created_at))

    def lock(self):
        # NOTE (labrams) Redis 2.6.12 supports an expanded SET command that will consolidate the following two commands into one
        if not self._redis.setnx(self._redis_key, self._salted_key):
            raise LockAlreadyAcquired('Lock for key "%s" has already been acquired' % self.key)
        self.incr()

    def unlock(self):
        self.require()

        pipeline = self._redis.pipeline()
        pipeline.delete(self._redis_key)
        pipeline.delete(self._redis_counter_key)
        pipeline.execute()

        if self.cb:
            self.cb(*self.cb_args)

    def incr(self):
        self.require()

        pipeline = self._redis.pipeline()
        pipeline.incr(self._redis_counter_key)
        self._update_expiration(pipeline)
        val, _, _ = pipeline.execute()

        return val

    def decr(self):
        self.require()

        pipeline = self._redis.pipeline()
        pipeline.decr(self._redis_counter_key)
        self._update_expiration(pipeline)
        val, _, _ = pipeline.execute()

        if val == 0:
            self.unlock()
        return val

    def _update_expiration(self, pipeline):
        """ This method updates expiration to `self.timeout` seconds from now. """
        pipeline.expire(self._redis_key, self.timeout)
        pipeline.expire(self._redis_counter_key, self.timeout)

    def __enter__(self):
        self.lock()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.decr()


class LockNotHeld(Exception):
    pass


class LockAlreadyAcquired(Exception):
    pass
