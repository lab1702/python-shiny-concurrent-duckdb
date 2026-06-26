# DuckDB + Shiny for Python — queued aggregations demo

Python analogue of an R/Shiny app that runs DuckDB queries via `{mirai}`. The
mapping:

| R/Shiny stack        | This demo                                            |
|----------------------|------------------------------------------------------|
| Shiny (R)            | **Shiny for Python** (reactive UI, same model)       |
| duckdb               | **duckdb** (same engine, `.cursor()` per worker)     |
| mirai daemons        | **`@reactive.extended_task` + `ThreadPoolExecutor`** |

Queries run *off* the reactive graph in a shared thread pool. Invocations
**queue** (a single `ExtendedTask` never runs itself concurrently), and the
session stays responsive throughout.

## Run

```bash
pip install shiny duckdb pandas matplotlib
shiny run --reload app.py
# open http://127.0.0.1:8000
```

## What to try

1. **Watch the clock** (top-left). It ticks every second even while a query
   runs — proof the UI isn't blocked.
2. **Change filters / group-by, click "Run aggregation."** With the latency
   slider at ~2s you'll see the button enter its *Processing…* state while the
   clock keeps ticking; results, chart, and SQL update on completion.
3. **Click "Queue 5 at once."** Five invocations fire instantly but drain
   **one at a time** (FIFO). Watch *Submitted* jump to 5 and *Completed* climb
   1→5 while the clock never freezes — this is the queuing behaviour mirai gives
   you in the R app.

## Tuning / production notes

- `max_workers` on the pool = how many queries run at once before others queue
  (the daemon-count knob from mirai).
- Swap `ThreadPoolExecutor` → `ProcessPoolExecutor` for crash isolation or
  CPU-bound post-processing. `run_aggregation` is already module-level, which
  `ProcessPoolExecutor` requires.
- The `time.sleep(latency)` in `run_aggregation` is **only** to make the async
  behaviour visible on a tiny dataset — delete it for real use.
- For mirai's heavier features (bounded queues / backpressure, distribution
  across machines, a real dispatcher), step up to Dask `distributed` or Ray.
