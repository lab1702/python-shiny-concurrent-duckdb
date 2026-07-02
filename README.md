# DuckDB — queued aggregations demo (Shiny & Streamlit)

Python analogue of an R/Shiny app that runs DuckDB queries via `{mirai}`. Two
frontends, same behaviour:

- **`app.py`** — Shiny for Python
- **`st-app.py`** — Streamlit

The mapping:

| R/Shiny stack | `app.py` (Shiny)                                     | `st-app.py` (Streamlit)                         |
|---------------|------------------------------------------------------|-------------------------------------------------|
| Shiny (R)     | **Shiny for Python** (reactive UI)                   | **Streamlit** (rerun-based UI)                  |
| duckdb        | **duckdb** — one shared DB, `.cursor()` per worker   | **duckdb** — same, via `@st.cache_resource`     |
| mirai daemons | **`@reactive.extended_task` + `ThreadPoolExecutor`** | **`Future` + `ThreadPoolExecutor`**, polled by a `st.fragment(run_every=1)` |

Queries run *off* the reactive graph in a shared thread pool. Invocations
**queue** (`max_workers=3`), and the session stays responsive throughout.

Shiny queues invocations for you (a single `ExtendedTask` never runs itself
concurrently). Streamlit has no such primitive, so `st-app.py` submits Futures
to the shared pool and polls them in a 1-second fragment. One honest
difference: Streamlit shows only the *latest completed* result, whereas Shiny
auto-renders the in-progress task state.

## Run

```bash
# Shiny
pip install shiny duckdb pandas matplotlib
shiny run --reload app.py
# open http://127.0.0.1:8000

# Streamlit
pip install streamlit duckdb pandas matplotlib
streamlit run st-app.py
# open http://localhost:8501
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
