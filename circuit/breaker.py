# Copyright 2012 Edgeware AB.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Functionality for managing errors when interacting with a remote service.

The circuit breaker monitors the communication and in the case of a high error
rate may break the circuit and not allow further communication for a short
period.  After a while the breaker will let through a single request to probe
to see if the service feels better. If not, it will open the circuit again.
"""

import collections
import functools
import logging
import timeit

LOGGER = logging.getLogger('python-circuit')
LOGGER.addHandler(logging.NullHandler())


class CircuitOpenError(Exception):
    """The circuit breaker is open."""


class CircuitBreaker(object):
    """A single circuit with breaker logic."""

    def __init__(self, max_fail, time_unit=60, reset_timeout=10, error_types=(),
                 log=LOGGER, log_tracebacks=False, clock=timeit.default_timer):
        """Initialize a circuit breaker.

        @param max_fail: The maximum number of allowed errors over the last
            C{time_unit}. If the breaker detects more errors than this, the
            circuit will open.

        @param time_unit: Time window (in seconds) for keeping track of errors.

        @param reset_timeout: Number of seconds to have the circuit open before
            it moves into C{half-open}.

        @param error_types: The exception types to be treated as errors by the
            circuit breaker.

        @param log: A L{logging.Logger} object that is used by the circuit breaker.
            Alternatively it can be a string specifying a descendant of L{LOGGER}.

        @param log_tracebacks: If true, log the traceback of the exceptions that
            cause the circuit to open.

        @param clock: A callable that takes no arguments and return the current
            time in seconds.
        """
        self.max_fail = max_fail
        self.time_unit = time_unit
        self.reset_timeout = reset_timeout
        self.error_types = tuple(error_types)
        if isinstance(log, basestring):
            log = LOGGER.getChild(log)
        self.log = log
        self.log_tracebacks = log_tracebacks
        self.clock = clock
        self.state = 'closed'
        self.last_change = None
        self.errors = collections.deque([None] * max_fail)

    def __call__(self, func):
        """Decorate a function to be called in this circuit breaker's context."""
        @functools.wraps(func)
        def wrapped(*args, **kwds):
            with self:
                return func(*args, **kwds)
        return wrapped

    def __enter__(self):
        """Context enter.

        @raise CircuitOpenError: if the circuit is still open
        """
        if self.state == 'open':
            delta = self.clock() - self.last_change
            if delta < self.reset_timeout:
                raise CircuitOpenError()
            self.state = 'half-open'
            self.log.debug('half-open - letting one through')

    def __exit__(self, exc_type, exc_val, tb):
        """Context exit."""
        if exc_type is None:
            self._success()
        elif isinstance(exc_val, self.error_types):
            self._error(self.log_tracebacks and (exc_type, exc_val, tb) or None)
        return False

    def _error(self, exc_info=None):
        """Update the circuit breaker with an error event."""
        now = self.clock()
        self.errors.append(now)
        earliest_error_time = self.errors.popleft()

        if self.state == 'closed':
            set_open = (earliest_error_time is not None and
                        now - earliest_error_time < self.time_unit)
        else:
            set_open = True

        if set_open:
            self.state = 'open'
            self.last_change = now
            self.log.log(exc_info and logging.ERROR or logging.INFO,
                         'opened circuit', exc_info=exc_info)

    def _success(self):
        if self.state == 'half-open':
            self.state = 'closed'
            self.log.info('closed circuit')
