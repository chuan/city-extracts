import os
import os.path
import sys
import time


def RateLimited(maxPerSecond):
    """Poor man's rate limiting decorator"""
    minInterval = 1.0 / float(maxPerSecond)

    def decorate(func):
        lastTimeCalled = [0.0]

        def rateLimitedFunction(*args, **kargs):
            elapsed = time.clock() - lastTimeCalled[0]
            leftToWait = minInterval - elapsed
            if leftToWait > 0:
                time.sleep(leftToWait)
            ret = func(*args, **kargs)
            lastTimeCalled[0] = time.clock()
            return ret
        return rateLimitedFunction
    return decorate


def check_dirs(dirs):
    """Check if the given dirs exist. Create the dir if it does not exist.
    Exit the program if anything else, e.g. file or link, is on the path."""
    for d in dirs:
        if (os.path.isdir(d)):
            pass
        elif (not os.path.exists(d)):
            os.makedirs(d)
        else:
            print(
                'Invalid data directory \'{0}\'.'.format(d),
                file=sys.stderr)
            sys.exit(1)
