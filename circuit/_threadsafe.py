import threading
from circuit.breaker import CircuitBreaker

class ThreadSafeCircuitBreaker(CircuitBreaker):
    """Circuit breaker that is safe to share among different threads."""

    def __init__(self, *args, **kwds):
        super(ThreadSafeCircuitBreaker, self).__init__(*args, **kwds)
        self._state_lock = threading.Lock()

    def __enter__(self):
        if self.state != 'open':
            return
        with self._state_lock:
            super(ThreadSafeCircuitBreaker, self).__enter__()

    def _success(self):
        if self.state != 'half-open':
            return
        with self._state_lock:
            super(ThreadSafeCircuitBreaker, self)._success()

    def _error(self, exc_info=None):
        with self._state_lock:
            super(ThreadSafeCircuitBreaker, self)._error(exc_info)
