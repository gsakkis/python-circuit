import threading
from circuit.breaker import CircuitBreaker

class ThreadSafeCircuitBreaker(CircuitBreaker):
    """Circuit breaker that is safe to share among different threads."""

    def __init__(self, *args, **kwds):
        super(ThreadSafeCircuitBreaker, self).__init__(*args, **kwds)
        self._state_lock = threading.Lock()

    def __enter__(self):
        with self._state_lock:
            return super(ThreadSafeCircuitBreaker, self).__enter__()

    def _success(self):
        with self._state_lock:
            return super(ThreadSafeCircuitBreaker, self)._success()

    def _error(self, exc_info=None):
        with self._state_lock:
            return super(ThreadSafeCircuitBreaker, self)._error(exc_info)
