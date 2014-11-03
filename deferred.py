#!/usr/bin/env python
#
# See LICENSE file for copying information
#

"""
    Space Monkey Twisted helpers
    http://www.spacemonkey.com/

    This library provides some miscellaneous Twisted helper primitives
"""

__copyright__ = "Copyright 2012 Space Monkey, Inc."

import os

from twisted.internet import defer
from twisted.python.util import mergeFunctionMetadata


def _inlineCallbacksSuccess(result, current_state, generator, deferred):
    if current_state[0]:
        current_state[0] = False
        current_state[1] = result
    else:
        _inlineCallbacks(result, None, generator, deferred)


def _inlineCallbacksFailure(myfailure, current_state, generator, deferred):
    if current_state[0]:
        current_state[0] = False
        current_state[2] = myfailure
    else:
        _inlineCallbacks(None, myfailure, generator, deferred)


def _inlineCallbacks(result, myfailure, generator, deferred):
    current_state = [True, result, myfailure]

    while True:
        try:
            if current_state[2] is None:
                next_deferred = generator.send(current_state[1])
            else:
                next_deferred = current_state[2].throwExceptionIntoGenerator(
                        generator)
                current_state[2] = None
        except StopIteration:
            deferred.callback(None)
            return deferred
        except defer._DefGen_Return, e:  # pylint: disable=W0212
            deferred.callback(e.value)
            return deferred
        except:
            deferred.errback()
            return deferred

        args = (current_state, generator, deferred)
        try:
            next_deferred.addCallbacks(
                    _inlineCallbacksSuccess, _inlineCallbacksFailure,
                    callbackArgs=args, errbackArgs=args)
        except AttributeError:
            if hasattr(next_deferred, "addCallbacks"):
                raise
            # next_deferred was something without an addCallbacks.
            # just pass the value back in
            current_state[1] = next_deferred
            continue

        if current_state[0]:
            # the callbacks didn't run immediately
            current_state[0] = False
            return deferred

        # the callbacks ran immediately. reset waiting and loop
        current_state[0] = True


def fastInline(f):
    """This decorator is mostly equivalent to defer.inlineCallbacks except
    that speed is the primary priority. while inlineCallbacks checks a few
    different things to make sure you're doing it properly, this method simply
    assumes you are.

    changes:
     * this decorator no longer checks that you're wrapping a generator
     * this decorator no longer checks that _DefGen_Return is thrown from the
       decoratored generator only
     * the generator loop no longer double checks that you're yielding
       deferreds and simply assumes it, catching the exception if you aren't
     * the generator stops calling isinstance for logic - profiling reveals
       that isinstance consumes a nontrivial amount of computing power during
       heavy use

    You can force this method to behave exactly like defer.inlineCallbacks by
    providing the FORCE_STANDARD_INLINE_CALLBACKS environment variable
    """
    def unwind(*args, **kwargs):
        try:
            gen = f(*args, **kwargs)
        except defer._DefGen_Return:  # pylint: disable=W0212
            raise TypeError("defer.returnValue used from non-generator")
        return _inlineCallbacks(None, None, gen, defer.Deferred())
    return mergeFunctionMetadata(f, unwind)


if "FORCE_STANDARD_INLINE_CALLBACKS" in os.environ:
    fastInline = defer.inlineCallbacks
