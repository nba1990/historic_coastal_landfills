# Universally applicable context manager supported computation measurement timer

import time


class MeasureTimer:
    """Helper time measuring utility class that supports context managers."""

    def __init__(self):
        self.start_time = 0
        self.end_time = 0
        self.elapsed_time = 0

    def __enter__(self):
        # This is preferred over time.time() as perf_counter is guaranteed to be monotonic and is not affected by
        # - system clock adjustments/time zone changes (such as DST)
        self.start_time = time.perf_counter()
        return self

    def __exit__(self, *args):
        self.end_time = time.perf_counter()
        self.elapsed_time = self.end_time - self.start_time
