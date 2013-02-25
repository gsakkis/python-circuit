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
from __future__ import division
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

    def __init__(self, max_fail, time_unit=None, max_error_rate=None,
                 reset_timeout=10, error_types=(),
                 log=LOGGER, log_tracebacks=False, clock=timeit.default_timer):
        """Initialize a circuit breaker.

        @param max_fail: The number of latest errors to keep track of. This is
            used to determine when the circuit opens as follows:
            1. If C{time_unit} is given, the circuit opens when the breaker
               detects more than C{max_fail} errors over the last C{time_unit}.
            2. If C{max_error_rate} is given, the circuit opens when the error
               rate (C{max_fail} divided by the total calls during the same
               period) is greater or equal than C{max_error_rate}.
            3. If both C{time_unit} and C{max_error_rate} are given, the circuit
               opens when both conditions of (1) and (2) are true.

        @param time_unit: Time window (in seconds) for keeping track of errors.

        @param max_error_rate: Maximum allowed running error rate for the
            circuit to remain closed.

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
        if time_unit is max_error_rate is None:
            raise ValueError("At least one of {time_unit, max_error_rate} must be specified")
        if max_error_rate is not None and not (0 < max_error_rate <= 1):
            raise ValueError('max_error_rate must be between 0 and 1')
        if isinstance(log, basestring):
            log = LOGGER.getChild(log)

        self._max_fail = max_fail
        self._time_unit = time_unit
        self._max_error_rate = max_error_rate
        self._reset_timeout = reset_timeout
        self._error_types = tuple(error_types)
        self._log = log
        self._log_tracebacks = log_tracebacks
        self._clock = clock

        self._last_change = None
        self._error_times = collections.deque([None] * max_fail)
        self._num_calls = collections.deque([0] * max_fail)
        self._state = 'closed'

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
        if self._state == 'open':
            delta = self._clock() - self._last_change
            if delta < self._reset_timeout:
                raise CircuitOpenError()
            self._state = 'half-open'
            self._log.debug('open => half-open (delta=%.2f sec)', delta)

    def __exit__(self, exc_type, exc_val, tb):
        """Context exit."""
        self._num_calls[-1] += 1
        if exc_type is None or not isinstance(exc_val, self._error_types):
            self._success()
        else:
            self._error(self._log_tracebacks and (exc_type, exc_val, tb) or None)
        return False

    def _error(self, exc_info=None):
        """Update the circuit breaker with an error event."""
        now = self._clock()
        self._error_times.append(now)
        earliest_error_time = self._error_times.popleft()

        total_calls = sum(self._num_calls)
        self._num_calls.append(0)
        self._num_calls.popleft()

        set_open = True
        if self._state == 'closed':
            if earliest_error_time is None:
                set_open = False
            else:
                delta = now - earliest_error_time
                error_rate = self._max_fail / total_calls
                assert error_rate <= 1.0

                if set_open and self._time_unit is not None:
                    set_open = delta < self._time_unit
                if set_open and self._max_error_rate is not None:
                    set_open = error_rate >= self._max_error_rate

        if set_open:
            if self._state == 'closed':
                self._log.debug('closed => open (delta=%.2f sec, error_rate=%.2f%%)',
                                delta, 100.0 * error_rate, exc_info=exc_info)
            else:
                self._log.debug('%s => open', self._state, exc_info=exc_info)
            self._state = 'open'
            self._last_change = now

    def _success(self):
        if self._state == 'half-open':
            self._state = 'closed'
            self._log.debug('half-open => closed')
