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

"""Functionality for managing errors when interacting with a remote
service.

The circuit breaker monitors the communication and in the case of a
high error rate may break the circuit and not allow further
communication for a short period.  After a while the breaker will let
through a single request to probe to see if the service feels better.
If not, it will open the circuit again.

A L{CircuitBreakerSet} can handle the state for multiple interactions
at the same time.  Use the C{context} method to pick which interaction
to track:

    try:
        with circuit_breaker.context('x'):
           # something that generates errors
        pass
    except CircuitOpenError:
        # the circuit was open so we did not even try to communicate
        # with the remote service.
        pass

"""

import logging
import timeit
from collections import deque

LOGGER = logging.getLogger('python-circuit')
LOGGER.addHandler(logging.NullHandler())

class CircuitOpenError(Exception):
    """The circuit breaker is open."""


class CircuitBreaker(object):
    """A single circuit with breaker logic."""

    def __init__(self, clock=timeit.default_timer, log=LOGGER, error_types=(),
                 maxfail=3, reset_timeout=10, time_unit=60):
        """Initialize a circuit breaker.

        @param clock: A callable that takes no arguments and return the current
            time in seconds.

        @param log: A L{logging.Logger} object that is used by the circuit breaker.
            Alternatively it can be a string specifying a descendant of L{LOGGER}.

        @param error_types: The exception types to be treated as errors by the
            circuit breaker.

        @param maxfail: The maximum number of allowed errors over the last
            C{time_unit}. If the breaker detects more errors than this, the
            circuit will open.

        @param reset_timeout: Number of seconds to have the circuit open before
            it moves into C{half-open}.

        @param time_unit: Time window (in seconds) for detecting errors.
        """
        self.clock = clock
        if isinstance(log, basestring):
            log = LOGGER.getChild(log)
        self.log = log
        self.error_types = tuple(error_types)
        self.maxfail = maxfail
        self.reset_timeout = reset_timeout
        self.time_unit = time_unit
        self.state = 'closed'
        self.last_change = None
        self.errors = deque([None] * maxfail)

    def error(self, err=None):
        """Update the circuit breaker with an error event."""
        now = self.clock()
        self.errors.append(now)
        earliest_error_time = self.errors.popleft()
        if earliest_error_time is not None:
            delta = now - earliest_error_time
            if delta < self.time_unit:
                self.state = 'open'
                self.last_change = now
                self.log.error('got error %r - opening circuit' % (err,))
                self.log.debug('error rate: %f errors per second' %
                               (float(self.maxfail) / (delta or 0.0001)))

    def test(self):
        """Check state of the circuit breaker.

        @raise CircuitOpenError: if the circuit is still open
        """
        if self.state == 'open':
            delta = self.clock() - self.last_change
            if delta < self.reset_timeout:
                raise CircuitOpenError()
            self.state = 'half-open'
            self.log.debug('half-open - letting one through')
        return self.state

    def success(self):
        if self.state == 'half-open':
            self.state = 'closed'
            self.log.info('closed circuit')

    def __enter__(self):
        """Context enter."""
        self.test()
        return self

    def __exit__(self, exc_type, exc_val, tb):
        """Context exit."""
        if exc_type is None:
            self.success()
        elif exc_type in self.error_types:
            self.error(exc_val)
        return False


class CircuitBreakerSet(object):
    """Controller for a set of circuit breakers.

    @ivar clock: A callable that takes no arguments and return the
        current time in seconds.

    @ivar log: A L{logging.Logger} object that is used for the circuit
        breakers.

    @ivar maxfail: The maximum number of allowed errors over the
        last minute.  If the breaker detects more errors than this, the
        circuit will open.

    @ivar reset_timeout: Number of seconds to have the circuit open
        before it moves into C{half-open}.
    """

    def __init__(self, clock=timeit.default_timer, log=LOGGER, maxfail=3,
                 reset_timeout=10, time_unit=60, factory=CircuitBreaker):
        self.clock = clock
        self.log = log
        self.maxfail = maxfail
        self.reset_timeout = reset_timeout
        self.time_unit = time_unit
        self.circuits = {}
        self.error_types = []
        self.factory = factory

    def handle_error(self, err_type):
        """Register error C{err_type} with the circuit breakers so
        that it will be handled as an error.
        """
        self.error_types.append(err_type)

    def handle_errors(self, err_types):
        """Register errors C{err_types} with the circuit breakers so
        that it will be handled as an error.
        """
        self.error_types.extend(err_types)

    def context(self, id):
        """Return a circuit breaker for the given ID."""
        if id not in self.circuits:
            self.circuits[id] = self.factory(self.clock, self.log.getChild(id),
                self.error_types, self.maxfail, self.reset_timeout,
                self.time_unit)
        return self.circuits[id]
