#!/usr/bin/env python
#
# See LICENSE file for copying information
#

"""
    Space Monkey
    http://www.spacemonkey.com/

    The reactor we used for our Twisted processes. By default it prefers IO
    instead of running until all deferreds have been processed. This might not
    be the best for everyone.
"""

__copyright__ = "Copyright 2012 Space Monkey, Inc."


import os
import math
import thread
from collections import defaultdict
from heapq import heappush, heappop, heapify

from twisted.internet.epollreactor import EPollReactor
from twisted.internet.main import installReactor
from twisted.python import log

from monoclock import nano_count

# Instead of open sourcing our config system for this, just use environment
# variables. MAYBE SOMEDAY
CONF_ioreactor = "SPACE_DISABLE_IO_REACTOR" not in os.environ
CONF_profile = "SPACE_PROFILE_REACTOR" in os.environ


class Reactor(EPollReactor):

    def __init__(self, *args, **kwargs):
        EPollReactor.__init__(self, *args, **kwargs)
        self._thread_ident = None
        self.queue_contributions = defaultdict(lambda: [0, 0, 0, None, None])

    def profileData(self):
        if not CONF_profile:
            return
        data = []
        for key, value in self.queue_contributions.iteritems():
            lineno, funcname, filename = key
            count, time_sum, time_squared_sum, time_min, time_max = value
            avg = time_sum / count
            stddev = math.sqrt((time_squared_sum / count) - (avg ** 2))
            data.append(("%s:%s" % (filename, lineno), funcname,
                         count, time_sum, avg, stddev, time_min, time_max))
        data.sort(key=(lambda x: x[-2]), reverse=True)
        return data

    def runUntilCurrent(self):
        """Run all pending timed calls.
        """

        ioreactor = CONF_ioreactor
        should_profile = CONF_profile

        if not ioreactor:
            EPollReactor.runUntilCurrent(self)
            return

        if self.threadCallQueue:
            # Keep track of how many calls we actually make, as we're
            # making them, in case another call is added to the queue
            # while we're in this loop.
            count = 0
            total = len(self.threadCallQueue)
            for (f, a, kw) in self.threadCallQueue:
                try:
                    f(*a, **kw)
                except:  # pylint: disable=W0702
                    log.err()
                count += 1
                if count == total:
                    break
            del self.threadCallQueue[:count]
            if self.threadCallQueue:
                self.wakeUp()

        # insert new delayed calls now
        self._insertNewDelayedCalls()

        now = self.seconds()
        while self._pendingTimedCalls and (
                self._pendingTimedCalls[0].time <= now):
            call = heappop(self._pendingTimedCalls)
            if call.cancelled:
                self._cancellations -= 1
                break

            if call.delayed_time > 0:
                call.activate_delay()
                heappush(self._pendingTimedCalls, call)
                break

            if should_profile:
                try:
                    call_name = (call.func.func_code.co_firstlineno,
                                 call.func.__name__,
                                 call.func.func_code.co_filename)
                except AttributeError, e:
                    call_name = (0, str(e),
                                 "<missing_attribute: %s>" % type(call.func))
                contribution = self.queue_contributions[call_name]
                contribution[0] += 1
                start_time = nano_count()

            try:
                call.called = 1
                call.func(*call.args, **call.kw)
            except:  # pylint: disable=W0702
                log.deferr()
                if hasattr(call, "creator"):
                    e = "\n"
                    e += " C: previous exception occurred in " + \
                         "a DelayedCall created here:\n"
                    e += " C:"
                    e += "".join(call.creator).rstrip().replace("\n", "\n C:")
                    e += "\n"
                    log.msg(e)

            if should_profile:
                elapsed = (nano_count() - start_time) * 1e-9
                contribution[1] += elapsed
                contribution[2] += elapsed ** 2
                if contribution[3] is None:
                    contribution[3] = elapsed
                    contribution[4] = elapsed
                else:
                    contribution[3] += min(elapsed, contribution[3])
                    contribution[4] += max(elapsed, contribution[4])

            break

        if (self._cancellations > 50 and
             self._cancellations > len(self._pendingTimedCalls) >> 1):
            self._cancellations = 0
            self._pendingTimedCalls = [x for x in self._pendingTimedCalls
                                       if not x.cancelled]
            heapify(self._pendingTimedCalls)

        if self._justStopped:
            self._justStopped = False
            self.fireSystemEvent("shutdown")

    def get_ident(self):
        return self._thread_ident

    def run(self, *args, **kwargs):
        self._thread_ident = thread.get_ident()
        return EPollReactor.run(self, *args, **kwargs)


def install():
    p = Reactor()
    installReactor(p)


__all__ = ["Reactor", "install"]
