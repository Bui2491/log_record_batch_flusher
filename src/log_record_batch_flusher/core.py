from __future__ import annotations

import logging
import threading
from typing import Callable, List, Optional


class BatchFlushHandler(logging.Handler):
    """A logging handler that accumulates records and flushes them in batches.

    Flushing is triggered by either (a) reaching ``capacity`` records, or
    (b) ``interval`` seconds elapsing since the last flush check. The latter is
    driven by an internal daemon thread that wakes on a condition variable so
    that ``close()`` can shut it down promptly without blocking for the full
    interval.

    The actual side-effect of a flush is delegated to a ``flush_func`` callable
    that receives the accumulated list of :class:`logging.LogRecord` objects.
    This keeps the handler free of any I/O opinion: the caller decides whether
    records go to a file, a queue, a socket, or a test double.

    The handler is safe for concurrent ``emit`` calls from multiple loggers.
    A single mutex guards the pending buffer and the flush trigger.
    """

    def __init__(
        self,
        flush_func: Callable[[List[logging.LogRecord]], None],
        capacity: int = 100,
        interval: float = 5.0,
        clock: Optional[Callable[[], float]] = None,
    ) -> None:
        """Initialize the handler.

        Args:
            flush_func: Called with the list of pending records on each flush.
                The list is a fresh copy each time; the handler clears its
                internal buffer before calling ``flush_func``, so the callable
                owns the list entirely. If ``flush_func`` raises, the records
                are still dropped (they have already been removed from the
                buffer) and the exception is allowed to propagate to the
                caller of ``flush`` / ``emit``. This is deliberate: swallowing
                flush errors silently would lose data with no signal.
            capacity: Maximum number of records to buffer before a forced
                flush. Must be at least 1.
            interval: Seconds between time-based flush checks. Must be
                positive. The timer thread wakes at least this often and
                flushes if any records are pending.
            clock: Monotonic clock callable returning seconds as a float.
                Defaults to :func:`time.monotonic`. Injected so tests can drive
                time-based behaviour deterministically.
        """
        if capacity < 1:
            raise ValueError("capacity must be at least 1")
        if interval <= 0:
            raise ValueError("interval must be positive")

        super().__init__()

        self._flush_func = flush_func
        self._capacity = capacity
        self._interval = interval
        self._clock = clock if clock is not None else _default_clock

        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._buffer: List[logging.LogRecord] = []
        self._last_flush = self._clock()
        self._closed = False

        self._thread = threading.Thread(
            target=self._timer_loop, name="BatchFlushHandler-timer", daemon=True
        )
        self._thread.start()

    def emit(self, record: logging.LogRecord) -> None:
        """Buffer a record, flushing if capacity is reached."""
        with self._lock:
            if self._closed:
                return
            self._buffer.append(record)
            if len(self._buffer) >= self._capacity:
                self._do_flush_locked()

    def flush(self) -> None:
        """Flush all pending records immediately."""
        with self._lock:
            if self._buffer:
                self._do_flush_locked()

    def _do_flush_locked(self) -> None:
        """Flush the buffer. Caller must hold ``_lock``.

        The buffer is cleared *before* calling ``flush_func`` so that if
        ``flush_func`` raises, we do not retry the same batch on the next
        flush — that could cause unbounded growth if the error is persistent.
        Dropping the batch on failure is the lesser evil.
        """
        batch = self._buffer
        self._buffer = []
        self._last_flush = self._clock()
        self._cond.notify_all()
        self._flush_func(batch)

    def _timer_loop(self) -> None:
        """Background loop that flushes on the time interval."""
        with self._lock:
            while not self._closed:
                now = self._clock()
                deadline = self._last_flush + self._interval
                timeout = deadline - now
                if timeout > 0:
                    self._cond.wait(timeout=timeout)
                    continue
                if self._buffer:
                    self._do_flush_locked()
                else:
                    # No pending records, but advance last_flush so we don't
                    # spin on a zero timeout every time the clock is past the
                    # deadline with an empty buffer.
                    self._last_flush = self._clock()

    def close(self) -> None:
        """Flush pending records and stop the timer thread."""
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._cond.notify_all()
            if self._buffer:
                self._do_flush_locked()
        self._thread.join(timeout=5.0)
        super().close()


def _default_clock() -> float:
    import time

    return time.monotonic()
