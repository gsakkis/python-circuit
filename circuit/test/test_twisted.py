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

from mockito import mock, when
import unittest

from twisted.internet import task, defer

from circuit import TwistedCircuitBreaker


class TwistedCircuitBreakerTestCase(unittest.TestCase):

    def setUp(self):
        self.circuit_breaker = TwistedCircuitBreaker(max_fail=3, time_unit=60,
                                                     log=mock(), clock=task.Clock())

    def test_context_exit_with_inline_callbacks_resets_circuit(self):
        @defer.inlineCallbacks
        def test():
            with self.circuit_breaker:
                self.circuit_breaker._state = 'half-open'
                yield defer.succeed(None)
                defer.returnValue(None)
        test()
        self.assertEquals(self.circuit_breaker._state, 'closed')
