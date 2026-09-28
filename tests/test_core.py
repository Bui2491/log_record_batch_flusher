import logging
import threading
import unittest

from log_record_batch_flusher import BatchFlushHandler


class FakeClock:
    """A controllable monotonic clock for deterministic tests."""

    def __init__(self, start: float = 0.0):
        self._now = start
        self._lock = threading.Lock()

    def __call__(self) -> float:
        with self._lock:
            return self._now

    def advance(self, seconds: float) -> None:
        with self._lock:
            self._now += seconds


class CollectingFlush:
    """Records every batch passed to flush_func."""

    def __init__(self):
        self.batches = []

    def __call__(self, records):
        self.batches.append(list(records))


class TestBatchFlushHandler(unittest.TestCase):
    def _make_record(self, msg="hello", level=logging.INFO):
        return logging.LogRecord(
            name="test", level=level, pathname=__file__, lineno=1,
            msg=msg, args=None, exc_info=None,
        )

    def test_capacity_triggers_flush(self):
        sink = CollectingFlush()
        clock = FakeClock()
        handler = BatchFlushHandler(sink, capacity=3, interval=100.0, clock=clock)
        try:
            handler.emit(self._make_record("a"))
            handler.emit(self._make_record("b"))
            self.assertEqual(sink.batches, [])
            handler.emit(self._make_record("c"))
            self.assertEqual(len(sink.batches), 1)
            self.assertEqual([r.msg for r in sink.batches[0]], ["a", "b", "c"])
        finally:
            handler.close()

    def test_flush_drops_buffer_before_calling_func(self):
        recorded = {}

        def flush_func(records):
            recorded["count_at_call"] = len(records)
            recorded["buffer_after_clear"] = len(handler._buffer)

        clock = FakeClock()
        handler = BatchFlushHandler(flush_func, capacity=10, interval=100.0, clock=clock)
        try:
            handler.emit(self._make_record("x"))
            handler.flush()
            self.assertEqual(recorded["count_at_call"], 1)
            self.assertEqual(recorded["buffer_after_clear"], 0)
        finally:
            handler.close()

    def test_flush_with_empty_buffer_is_noop(self):
        sink = CollectingFlush()
        clock = FakeClock()
        handler = BatchFlushHandler(sink, capacity=10, interval=100.0, clock=clock)
        try:
            handler.flush()
            self.assertEqual(sink.batches, [])
        finally:
            handler.close()

    def test_close_flushes_pending(self):
        sink = CollectingFlush()
        clock = FakeClock()
        handler = BatchFlushHandler(sink, capacity=10, interval=100.0, clock=clock)
        handler.emit(self._make_record("pending"))
        handler.close()
        self.assertEqual(len(sink.batches), 1)
        self.assertEqual([r.msg for r in sink.batches[0]], ["pending"])

    def test_emit_after_close_is_silent_noop(self):
        sink = CollectingFlush()
        clock = FakeClock()
        handler = BatchFlushHandler(sink, capacity=10, interval=100.0, clock=clock)
        handler.close()
        # Should not raise and should not call flush_func.
        handler.emit(self._make_record("late"))
        self.assertEqual(sink.batches, [])

    def test_close_is_idempotent(self):
        sink = CollectingFlush()
        clock = FakeClock()
        handler = BatchFlushHandler(sink, capacity=10, interval=100.0, clock=clock)
        handler.close()
        handler.close()  # must not raise

    def test_invalid_capacity_raises(self):
        with self.assertRaises(ValueError):
            BatchFlushHandler(lambda r: None, capacity=0, interval=1.0, clock=FakeClock())

    def test_invalid_interval_raises(self):
        with self.assertRaises(ValueError):
            BatchFlushHandler(lambda r: None, capacity=1, interval=0.0, clock=FakeClock())

    def test_timer_flushes_on_interval(self):
        sink = CollectingFlush()
        clock = FakeClock()
        handler = BatchFlushHandler(sink, capacity=100, interval=5.0, clock=clock)
        try:
            handler.emit(self._make_record("timed"))
            self.assertEqual(sink.batches, [])
            clock.advance(5.0)
            # The timer thread is waiting on the condition variable with a
            # timeout derived from the fake clock. Wake it by nudging the
            # condition so it re-evaluates the deadline.
            with handler._cond:
                handler._cond.notify_all()
            # Give the thread a moment to react.
            for _ in range(200):
                if sink.batches:
                    break
                handler._thread.join(timeout=0.05)
            self.assertEqual(len(sink.batches), 1)
            self.assertEqual([r.msg for r in sink.batches[0]], ["timed"])
        finally:
            handler.close()

    def test_timer_does_not_flush_empty_buffer(self):
        sink = CollectingFlush()
        clock = FakeClock()
        handler = BatchFlushHandler(sink, capacity=100, interval=2.0, clock=clock)
        try:
            clock.advance(2.0)
            with handler._cond:
                handler._cond.notify_all()
            handler._thread.join(timeout=0.5)
            self.assertEqual(sink.batches, [])
        finally:
            handler.close()

    def test_concurrent_emit_is_thread_safe(self):
        sink = CollectingFlush()
        clock = FakeClock()
        handler = BatchFlushHandler(sink, capacity=500, interval=100.0, clock=clock)
        try:
            def worker(start, count):
                for i in range(start, start + count):
                    handler.emit(self._make_record(str(i)))

            threads = [
                threading.Thread(target=worker, args=(0, 100)),
                threading.Thread(target=worker, args=(100, 100)),
                threading.Thread(target=worker, args=(200, 100)),
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            handler.flush()
            total = sum(len(b) for b in sink.batches)
            self.assertEqual(total, 300)
            # No record should be duplicated or lost.
            all_msgs = []
            for b in sink.batches:
                all_msgs.extend(r.msg for r in b)
            self.assertEqual(sorted(all_msgs), sorted(str(i) for i in range(300)))
        finally:
            handler.close()

    def test_flush_func_exception_propagates_from_emit(self):
        clock = FakeClock()
        calls = []

        def boom(records):
            calls.append(len(records))
            raise RuntimeError("flush failed")

        handler = BatchFlushHandler(boom, capacity=2, interval=100.0, clock=clock)
        try:
            handler.emit(self._make_record("a"))
            with self.assertRaises(RuntimeError):
                handler.emit(self._make_record("b"))  # triggers capacity flush
            self.assertEqual(calls, [2])
            # Buffer was cleared despite the error, so a subsequent flush is empty.
            handler.flush()
            self.assertEqual(calls, [2])
        finally:
            handler.close()


if __name__ == "__main__":
    unittest.main()
