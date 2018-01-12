import time
import resource

class Timer:
    def __init__(self):
        self._start = None
        self._end = None

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def start(self):
        self._start = time.time()

    def stop(self):
        self._end = time.time()

    @property
    def elapsed(self):
        if self._end is not None:
            return self._end - self._start
        else:
            return time.time() - self._start


class StopWatch():
    def __init__(self, name):
        self._segments = []
        self._name = name
        self._start = None
        self._segment_name = None

    def register_segment(self, segment_name, duration):
        self._segments.append((segment_name, duration))

    def register(self, name):
        self._start = time.time()
        self._segment_name = name
        return self

    def __enter__(self):
        if self._segment_name is None:
            raise ValueError('Should be used with register')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        segment = (self._segment_name, (time.time() - self._start))
        self._segments.append(segment)
        self._start = None
        self._segment_name = None

    def __str__(self):
        segstr = " ".join(["%s:%.5f" % (n, d) for n, d in self._segments])
        return "%s %s" % (self._name, segstr)



def get_rss():
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss

if __name__ == '__main__':
    sw = StopWatch("MyWatch")
    sw.register_segment("one", 1.234)
    with sw.register("two"):
        time.sleep(1.5)
    print(sw)
