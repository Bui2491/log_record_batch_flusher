# Log Record Batch Flusher

A `logging.Handler` that accumulates `LogRecord` instances and flushes them in batches, triggered either by a record-count capacity or a time interval. The flush side-effect is delegated to a caller-supplied function, so the handler has no I/O opinion of its own.

## Usage

```python
import logging
from log_record_batch_flusher import BatchFlushHandler

def write_batch(records):
    # records is a list[logging.LogRecord]; do whatever you want with it
    with open("app.log", "a") as f:
        for r in records:
            f.write(r.getMessage() + "\n")

handler = BatchFlushHandler(write_batch, capacity=100, interval=5.0)

logger = logging.getLogger("demo")
logger.setLevel(logging.INFO)
logger.addHandler(handler)

logger.info("buffered until 100 records or 5 seconds")
```

## Why

Every `emit` call to a stock `StreamHandler` or `FileHandler` performs a write. Under high log volume that I/O dominates cost. Batching amortizes it: one write per N records, or one per interval, whichever comes first.

The trade-off is latency and a window for data loss. Records sit in memory until a flush triggers. If the process crashes between flushes, buffered records are lost. `close()` flushes pending records, but ungraceful exits do not call `close()`. Use this handler when throughput matters more than per-record durability.

## Edge cases

- If the supplied `flush_func` raises, the current batch is dropped (it has already been removed from the buffer) and the exception propagates to the caller of `emit` or `flush`. This is deliberate: silently swallowing flush errors would lose data with no signal. If you need resilience, wrap your `flush_func` in a try/except.
- `emit` after `close()` is a silent no-op.
- The time-based flush is driven by a daemon thread that wakes on a condition variable. `close()` signals the thread and joins it, so shutdown does not block for the full interval.
- The `clock` constructor argument (defaulting to `time.monotonic`) exists so tests can advance time deterministically instead of sleeping.

## Exported names

- `BatchFlushHandler(flush_func, capacity=100, interval=5.0, clock=None)` — the handler class.

`flush_func` receives `list[logging.LogRecord]`. `capacity` must be ≥ 1. `interval` must be > 0.
