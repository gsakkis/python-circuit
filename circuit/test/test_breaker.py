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

"""Test cases for the circuit breaker."""

from mockito import mock
import unittest

from circuit import CircuitBreaker, CircuitOpenError


class Clock(object):
    now = 0.0

    def time(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


class CircuitBreakerTestCase(unittest.TestCase):
    """Test cases for the circuit breaker."""

    def setUp(self):
        self.clock = Clock()
        self.reset_timeout = 10
        self.breaker = CircuitBreaker(max_fail=2,
                                      time_unit=60,
                                      reset_timeout=self.reset_timeout,
                                      error_types=(IOError,),
                                      log=mock(),
                                      clock=self.clock.time)

    @property
    def error_count(self):
        return sum(1 for t in self.breaker.errors if t is not None)

    def test_passes_through_unhandled_errors(self):
        try:
            with self.breaker:
                raise RuntimeError("error")
        except RuntimeError:
            self.assertEquals(self.error_count, 0)
        else:
            self.assertTrue(False, "exception not raised")

    def test_passes_through_unhandled_errors_decorator(self):
        @self.breaker
        def test():
            raise RuntimeError("error")

        self.assertRaises(RuntimeError, test)
        self.assertEquals(self.error_count, 0)

    def test_catches_handled_errors(self):
        try:
            with self.breaker:
                raise IOError("error")
        except IOError:
            self.assertEquals(self.error_count, 1)
        else:
            self.assertTrue(False, "exception not raised")

    def test_catches_handled_errors_decorator(self):
        @self.breaker
        def test():
            raise IOError("error")

        self.assertRaises(IOError, test)
        self.assertEquals(self.error_count, 1)
        self.assertRaises(IOError, test)
        self.assertEquals(self.error_count, 2)

    def test_opens_breaker_on_errors(self):
        self.breaker._error()
        self.breaker._error()
        self.breaker._error()
        self.assertEquals(self.breaker.state, 'open')

    def test_allows_unfrequent_errors(self):
        for i in range(10):
            self.breaker._error()
            self.clock.advance(30)
        self.assertEquals(self.breaker.state, 'closed')

    def test_closes_breaker_on_successful_transaction(self):
        self.test_opens_breaker_on_errors()
        self.clock.advance(self.reset_timeout)
        with self.breaker:
            self.assertEquals(self.breaker.state, 'half-open')
        self.breaker._success()
        with self.breaker:
            self.assertEquals(self.breaker.state, 'closed')

    def test_raises_circuit_open_when_open(self):
        self.test_opens_breaker_on_errors()
        self.assertRaises(CircuitOpenError, self.breaker.__enter__)

    def test_context_exit_without_exception_resets_circuit(self):
        self.breaker.state = 'half-open'
        with self.breaker:
            pass
        self.assertEquals(self.breaker.state, 'closed')

    def test_context_exit_with_exception_opens_circuit(self):
        def test():
            with self.breaker:
                raise IOError("error")
        self.breaker.state = 'half-open'
        self.assertRaises(IOError, test)
        self.assertEquals(self.breaker.state, 'open')

    def test_context_exit_with_exception_marks_error(self):
        def test():
            with self.breaker:
                raise IOError("error")
        self.assertRaises(IOError, test)
        self.assertEquals(self.error_count, 1)

    def test_context_exit_with_exception_subclass(self):
        class SubIOError(IOError):
            pass
        def test():
            with self.breaker:
                raise SubIOError("error")
        self.assertRaises(SubIOError, test)
        self.assertEquals(self.error_count, 1)
