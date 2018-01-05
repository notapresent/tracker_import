import time

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
