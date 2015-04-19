import logging, random, time
from circuit import CircuitBreaker, CircuitOpenError

logging.basicConfig(level=logging.DEBUG, format="%(relativeCreated)s: %(message)s")

@CircuitBreaker(max_fail=8, max_error_rate=0.8, reset_timeout=0.5,
                error_types=(ValueError,))
def f(p_fail=0.4):
    if random.random() < p_fail:
        raise ValueError

sleep_time = 0.01

skipped = failed = passed = 0
for i in xrange(1000):
    try:
        f()
    except CircuitOpenError:
        skipped += 1
    except ValueError:
        failed += 1
    else:
        passed += 1
    time.sleep(sleep_time)
print "skipped: %d, failed: %d, passed: %d" % (skipped, failed, passed)
