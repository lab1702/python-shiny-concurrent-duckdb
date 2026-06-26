"""
DuckDB + Shiny for Python: queued aggregations that keep the UI responsive.

This is the Python analogue of an R/Shiny app that uses {mirai} to run DuckDB
queries in background processes. Here the moving parts are:

  * Shiny for Python  -> the reactive UI (the Shiny-for-R port)
  * duckdb            -> the same embedded analytical engine
  * @reactive.extended_task + ThreadPoolExecutor
                      -> the mirai analogue: work runs OFF the reactive graph,
                         invocations QUEUE, and the session stays responsive

Why a thread pool (not processes)? DuckDB releases the GIL during query
execution and its cursors are thread-safe, so threads give real parallelism for
DB work at a fraction of the overhead of mirai's separate-process model. Swap
ThreadPoolExecutor -> ProcessPoolExecutor (and nothing else) if you need crash
isolation or CPU-bound post-processing.

Run with:  shiny run --reload app.py
"""

import asyncio
import concurrent.futures
import datetime
import time

import duckdb
import pandas as pd
from shiny import App, reactive, render, ui

# ---------------------------------------------------------------------------
# Module-level shared resources (created ONCE, shared across all sessions).
# This is the equivalent of mirai's persistent daemon pool: a fixed set of
# workers that every session's queries are dispatched to.
# ---------------------------------------------------------------------------

# A single in-memory DuckDB database. Worker threads each call .cursor() on it
# to get a thread-local connection that shares the same data (the Python
# analogue of `everywhere(con <<- dbConnect(...))` in mirai).
_con = duckdb.connect()

_con.execute(
    """
    CREATE TABLE transactions AS
    SELECT
        i AS id,
        (DATE '2021-01-01' + (random() * 1460)::INTEGER) AS txn_date,
        (['North', 'South', 'East', 'West', 'Central'])[1 + (random() * 5)::INTEGER]      AS region,
        (['Electronics', 'Apparel', 'Home', 'Grocery', 'Toys', 'Sports'])[1 + (random() * 6)::INTEGER] AS category,
        (['Consumer', 'SMB', 'Enterprise'])[1 + (random() * 3)::INTEGER]                   AS segment,
        round((random() * 500 + 5)::DECIMAL(10, 2), 2)  AS amount,
        (1 + (random() * 10)::INTEGER)                  AS quantity
    FROM range(3000000) t(i);
    """
)

# 3 workers => at most 3 queries run at once; the rest queue. Tune to taste.
pool = concurrent.futures.ThreadPoolExecutor(max_workers=3)

REGIONS = ["North", "South", "East", "West", "Central"]
CATEGORIES = ["Electronics", "Apparel", "Home", "Grocery", "Toys", "Sports"]
SEGMENTS = ["Consumer", "SMB", "Enterprise"]

GROUP_EXPR = {
    "region": "region",
    "category": "category",
    "segment": "segment",
    "year": "CAST(EXTRACT(year FROM txn_date) AS INTEGER)",
    "month": "strftime(txn_date, '%Y-%m')",
}
AGG_EXPR = {
    "sum_amount": "SUM(amount)",
    "avg_amount": "AVG(amount)",
    "count": "COUNT(*)",
    "sum_qty": "SUM(quantity)",
}
AGG_LABEL = {
    "sum_amount": "Total amount",
    "avg_amount": "Average amount",
    "count": "Transaction count",
    "sum_qty": "Total quantity",
}


def _quote_in(values):
    return ", ".join("'" + v.replace("'", "''") + "'" for v in values)


def build_sql(regions, categories, segments, year_lo, year_hi, min_amount, groups, agg):
    """Turn the current filter / group-by selections into a single SQL string."""
    where = []
    # An empty multi-select means "nothing selected" -> return no rows.
    where.append(f"region IN ({_quote_in(regions)})" if regions else "1=0")
    where.append(f"category IN ({_quote_in(categories)})" if categories else "1=0")
    where.append(f"segment IN ({_quote_in(segments)})" if segments else "1=0")
    where.append(f"EXTRACT(year FROM txn_date) BETWEEN {int(year_lo)} AND {int(year_hi)}")
    where.append(f"amount >= {float(min_amount)}")
    where_sql = " AND ".join(where)

    agg_sql = AGG_EXPR.get(agg, "SUM(amount)")
    group_cols = [g for g in groups if g in GROUP_EXPR]

    if group_cols:
        select_cols = ", ".join(f"{GROUP_EXPR[g]} AS {g}" for g in group_cols)
        sql = (
            f"SELECT {select_cols}, {agg_sql} AS metric "
            f"FROM transactions WHERE {where_sql} "
            f"GROUP BY ALL ORDER BY metric DESC LIMIT 500"
        )
    else:
        sql = f"SELECT {agg_sql} AS metric FROM transactions WHERE {where_sql}"
    return sql, group_cols


def run_aggregation(params):
    """Runs in a WORKER THREAD. No Shiny / reactive access allowed in here."""
    start = time.perf_counter()
    cur = _con.cursor()  # thread-local connection over the shared database

    # Pedagogical only: real heaviness comes from data size / query complexity.
    # This makes the async + queuing behaviour visible with a small dataset.
    if params["latency"] > 0:
        time.sleep(params["latency"])

    df = cur.execute(params["sql"]).df()
    elapsed = time.perf_counter() - start
    return {
        "df": df,
        "group_cols": params["group_cols"],
        "rows": len(df),
        "elapsed": elapsed,
        "label": params["label"],
        "sql": params["sql"],
        "finished": datetime.datetime.now().strftime("%H:%M:%S"),
    }


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------

app_ui = ui.page_sidebar(
    ui.sidebar(
        ui.input_checkbox_group("regions", "Region", REGIONS, selected=REGIONS),
        ui.input_checkbox_group("categories", "Category", CATEGORIES, selected=CATEGORIES),
        ui.input_checkbox_group("segments", "Segment", SEGMENTS, selected=SEGMENTS),
        ui.input_slider("years", "Year range", min=2021, max=2024, value=(2021, 2024), sep=""),
        ui.input_numeric("min_amount", "Min amount ($)", value=0, min=0, max=505, step=10),
        ui.hr(),
        ui.input_checkbox_group(
            "groups",
            "Group by",
            {"region": "Region", "category": "Category", "segment": "Segment",
             "year": "Year", "month": "Month"},
            selected=["region", "category"],
        ),
        ui.input_select("agg", "Metric", AGG_LABEL),
        ui.hr(),
        ui.input_slider("latency", "Simulated query latency (s)", min=0, max=5, value=2, step=0.5),
        ui.input_task_button("run", "Run aggregation"),
        ui.input_action_button("stress", "Queue 5 at once"),
        width=320,
    ),
    ui.layout_columns(
        ui.value_box("Clock (proves responsiveness)", ui.output_text("clock")),
        ui.value_box("Submitted", ui.output_text("n_submitted")),
        ui.value_box("Completed", ui.output_text("n_completed")),
        ui.value_box("In flight / queued", ui.output_text("n_inflight")),
        col_widths=[3, 3, 3, 3],
    ),
    ui.layout_columns(
        ui.card(ui.card_header("Results (latest completed query)"), ui.output_data_frame("results")),
        ui.card(ui.card_header("Top groups"), ui.output_plot("chart")),
        col_widths=[7, 5],
    ),
    ui.layout_columns(
        ui.card(ui.card_header("Query log (FIFO completion order)"), ui.output_data_frame("query_log")),
        ui.card(ui.card_header("Last SQL sent to DuckDB"), ui.output_code("last_sql")),
        col_widths=[6, 6],
    ),
    title="DuckDB aggregations, queued off the reactive graph",
    fillable=False,
)


# ---------------------------------------------------------------------------
# Server
# ---------------------------------------------------------------------------

def server(input, output, session):
    submitted = reactive.value(0)
    completed = reactive.value(0)
    log = reactive.value([])

    # The mirai analogue: an off-graph task backed by the thread pool. It cannot
    # read reactive inputs, so everything it needs is passed in as `params`.
    @ui.bind_task_button(button_id="run")
    @reactive.extended_task
    async def agg_task(params):
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(pool, run_aggregation, params)

    def snapshot(label):
        sql, group_cols = build_sql(
            input.regions(), input.categories(), input.segments(),
            input.years()[0], input.years()[1], input.min_amount(),
            input.groups(), input.agg(),
        )
        return {"sql": sql, "group_cols": group_cols,
                "latency": float(input.latency()), "label": label}

    # One query on click. input_task_button blocks re-clicks while a run is in
    # progress, so this can't accidentally double-fire.
    @reactive.effect
    @reactive.event(input.run)
    def _on_run():
        n = submitted() + 1
        submitted.set(n)
        agg_task(snapshot(f"query #{n}"))

    # Fire 5 invocations back-to-back to demonstrate FIFO queuing: a single
    # ExtendedTask won't run itself concurrently, so these drain one at a time
    # while the clock above keeps ticking.
    @reactive.effect
    @reactive.event(input.stress)
    def _on_stress():
        base = submitted()
        params = snapshot("")
        for k in range(1, 6):
            p = dict(params, label=f"queued #{base + k}")
            agg_task(p)
        submitted.set(base + 5)

    # Bring completed results back INTO the reactive graph. result() raises a
    # silent exception until the task succeeds, so this effect only does real
    # work on completion. isolate() the log read so writing it doesn't re-fire us.
    @reactive.effect
    def _collect():
        res = agg_task.result()
        with reactive.isolate():
            completed.set(completed() + 1)
            entry = {"query": res["label"], "rows": res["rows"],
                     "seconds": round(res["elapsed"], 2), "finished": res["finished"]}
            log.set(([entry] + log())[:15])

    @render.text
    def clock():
        reactive.invalidate_later(1)
        return datetime.datetime.now().strftime("%H:%M:%S")

    @render.text
    def n_submitted():
        return str(submitted())

    @render.text
    def n_completed():
        return str(completed())

    @render.text
    def n_inflight():
        return str(submitted() - completed())

    @render.data_frame
    def results():
        df = agg_task.result()["df"]  # in-progress state shown automatically
        return render.DataGrid(df, height="340px", filters=True)

    @render.plot
    def chart():
        import matplotlib.pyplot as plt

        res = agg_task.result()
        df, group_cols = res["df"], res["group_cols"]
        fig, ax = plt.subplots()
        if df.empty:
            ax.text(0.5, 0.5, "No rows match the filters", ha="center", va="center")
            ax.axis("off")
            return fig

        top = df.head(15).iloc[::-1]
        if group_cols:
            labels = top[group_cols].astype(str).agg(" / ".join, axis=1)
        else:
            labels = ["Total"]
        ax.barh(range(len(top)), top["metric"], color="#4c78a8")
        ax.set_yticks(range(len(top)))
        ax.set_yticklabels(labels, fontsize=8)
        ax.set_xlabel(AGG_LABEL.get(input.agg(), "metric"))
        fig.tight_layout()
        return fig

    @render.data_frame
    def query_log():
        rows = log()
        if not rows:
            return render.DataGrid(pd.DataFrame(columns=["query", "rows", "seconds", "finished"]))
        return render.DataGrid(pd.DataFrame(rows), height="300px")

    @render.code
    def last_sql():
        try:
            return agg_task.result()["sql"]
        except Exception:
            return "Run a query to see the SQL."


app = App(app_ui, server)
app.on_shutdown(pool.shutdown)
