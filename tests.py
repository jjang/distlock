import unittest

from misc.lock import lock, Lock, LockAlreadyAcquired
from redis_utils.mock import RedisMock


class LockTest(unittest.TestCase):

    def setUp(self):
        def keygen(args, kwargs):
            return 'key'
        self.keygen = keygen

    def test_lock_creation(self):
        rm = RedisMock()

        @lock(keygen=self.keygen, timeout=600, redis_factory=lambda: rm)
        def locked_function(lock_handle=None):
            self.assertTrue(isinstance(lock_handle, Lock))
            self.assertTrue(lock_handle.active)

        locked_function()

    def test_lock_data(self):
        rm = RedisMock()

        @lock(keygen=self.keygen, timeout=600, redis_factory=lambda: rm)
        def locked_function(lock_handle=None):
            self.assertEqual(lock_handle.key, 'key')
            self.assertEqual(lock_handle.count, '1')
            self.assertEqual(lock_handle.active, True)

        locked_function()

    def test_lock_conflict(self):
        rm = RedisMock()

        @lock(keygen=self.keygen, timeout=600, redis_factory=lambda: rm)
        def locked_function(lock_handle=None):
            lock_handle.incr()

        locked_function()
        self.assertRaises(LockAlreadyAcquired, locked_function)

    def test_lock_count(self):
        rm = RedisMock()

        @lock(keygen=self.keygen, timeout=600, redis_factory=lambda: rm)
        def locked_function(lock_handle=None):
            self._lock_handle = lock_handle
            self.assertEqual(lock_handle.count, '1')
            self._lock_handle.incr()
            plus_1_function()
            self.assertEqual(lock_handle.count, '1')

        def plus_1_function():
            self.assertEqual(self._lock_handle.count, '2')
            self._lock_handle.decr()

        locked_function()

    def tests_lock_timeout(self):
        rm = RedisMock()
        with Lock(key='key', timeout=600, redis_factory=lambda: rm) as lock_handle:
            self.assertEqual(lock_handle.timeout, 600)
            lock_handle.incr()

        with self.assertRaises(LockAlreadyAcquired):
            with Lock(key='key', timeout=100, redis_factory=lambda: rm):
                pass

        # mimic expire
        rm.delete(lock_handle._redis_key)

        with Lock(key='key', timeout=200, redis_factory=lambda: rm) as lock_handle:
            self.assertEqual(lock_handle.timeout, 200)

    def tests_lock_callback(self):

        def callback(a):
            self.assertEqual(a, 'calling_callback')

        rm = RedisMock()

        @lock(keygen=self.keygen, timeout=600, redis_factory=lambda: rm, cb=callback, cb_args=('calling_callback',))
        def locked_function(lock_handle=None):
            pass

        locked_function()
