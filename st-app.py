"""
DuckDB + Streamlit: queued aggregations that keep the UI responsive.

This is the Streamlit port of app.py (Shiny for Python). Same idea, different
reactivity model:

  * Streamlit             -> reruns the whole script on interaction; each
                             browser session gets its own ScriptRunner thread
  * duckdb                -> the same embedded analytical engine
  * @st.cache_resource    -> the "persistent daemon pool": ONE DuckDB database
                             and ONE ThreadPoolExecutor shared across every
                             session (mirai's daemon-pool analogue)
  * Future + st.fragment(run_every=1)
                          -> the off-graph queue: work runs in worker threads,
                             submissions QUEUE (3 workers), and a 1s fragment
                             polls them so the clock keeps ticking and the app
                             stays responsive while queries drain

Shiny's @reactive.extended_task queues invocations for you. Streamlit has no
such primitive, so we submit Futures to the shared pool ourselves and poll
them in a fragment. The pool's max_workers=3 is what makes submissions queue.

Run with:  streamlit run st-app.py
"""

import concurrent.futures
import datetime
import time

import duckdb
import pandas as pd
import streamlit as st

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
GROUP_LABEL = {
    "region": "Region", "category": "Category", "segment": "Segment",
    "year": "Year", "month": "Month",
}


# ---------------------------------------------------------------------------
# Shared resources: created ONCE, shared across all sessions. @st.cache_resource
# is Streamlit's process-wide singleton -> the mirai persistent-pool analogue.
# ---------------------------------------------------------------------------

@st.cache_resource
def get_resources():
    con = duckdb.connect()
    con.execute(
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
    return con, pool


CON, POOL = get_resources()


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
    """Runs in a WORKER THREAD. No Streamlit access allowed in here."""
    start = time.perf_counter()
    cur = CON.cursor()  # thread-local connection over the shared database

    # Pedagogical only: makes the async + queuing behaviour visible.
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
# Per-session state (each browser session has its own).
# ---------------------------------------------------------------------------

st.set_page_config(page_title="DuckDB queued aggregations (Streamlit)", layout="wide")

ss = st.session_state
ss.setdefault("pending", [])       # list of Futures not yet collected
ss.setdefault("submitted", 0)
ss.setdefault("completed", 0)
ss.setdefault("log", [])
ss.setdefault("last_result", None)


def snapshot(label):
    sql, group_cols = build_sql(
        ss.regions, ss.categories, ss.segments,
        ss.years[0], ss.years[1], ss.min_amount,
        ss.groups, ss.agg,
    )
    return {"sql": sql, "group_cols": group_cols,
            "latency": float(ss.latency), "label": label}


def submit(label):
    ss.pending.append(POOL.submit(run_aggregation, snapshot(label)))
    ss.submitted += 1


def on_run():
    submit(f"query #{ss.submitted + 1}")


def on_stress():
    # Fire 5 back-to-back to demonstrate FIFO queuing: 3 workers means at most
    # 3 run at once while the rest wait, and the clock keeps ticking meanwhile.
    base = ss.submitted
    for k in range(1, 6):
        submit(f"queued #{base + k}")


def collect_finished():
    """Move any completed Futures into the log. Called from the polling fragment."""
    still_pending = []
    for fut in ss.pending:
        if not fut.done():
            still_pending.append(fut)
            continue
        res = fut.result()  # worker already finished; won't block
        ss.completed += 1
        ss.last_result = res
        entry = {"query": res["label"], "rows": res["rows"],
                 "seconds": round(res["elapsed"], 2), "finished": res["finished"]}
        ss.log = ([entry] + ss.log)[:15]
    ss.pending = still_pending


# ---------------------------------------------------------------------------
# Sidebar (inputs). Interacting reruns the script; buttons submit off-graph work.
# ---------------------------------------------------------------------------

with st.sidebar:
    st.multiselect("Region", REGIONS, default=REGIONS, key="regions")
    st.multiselect("Category", CATEGORIES, default=CATEGORIES, key="categories")
    st.multiselect("Segment", SEGMENTS, default=SEGMENTS, key="segments")
    st.slider("Year range", 2021, 2024, (2021, 2024), key="years")
    st.number_input("Min amount ($)", 0, 505, 0, step=10, key="min_amount")
    st.divider()
    st.multiselect(
        "Group by", list(GROUP_LABEL), default=["region", "category"],
        format_func=GROUP_LABEL.get, key="groups",
    )
    st.selectbox("Metric", list(AGG_LABEL), format_func=AGG_LABEL.get, key="agg")
    st.divider()
    st.slider("Simulated query latency (s)", 0.0, 5.0, 2.0, step=0.5, key="latency")
    st.button("Run aggregation", type="primary", on_click=on_run, use_container_width=True)
    st.button("Queue 5 at once", on_click=on_stress, use_container_width=True)


# ---------------------------------------------------------------------------
# Live panel: reruns every 1s so the clock ticks and Futures drain visibly
# while the rest of the page stays interactive. This is the responsiveness proof.
# ---------------------------------------------------------------------------

st.title("DuckDB aggregations, queued off the reactive graph")


@st.fragment(run_every=1)
def live_panel():
    collect_finished()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Clock (proves responsiveness)", datetime.datetime.now().strftime("%H:%M:%S"))
    c2.metric("Submitted", ss.submitted)
    c3.metric("Completed", ss.completed)
    c4.metric("In flight / queued", len(ss.pending))

    res = ss.last_result
    left, right = st.columns([7, 5])

    with left:
        st.subheader("Results (latest completed query)")
        if res is None:
            st.info("Run a query to see results.")
        else:
            st.dataframe(res["df"], height=340, use_container_width=True)

    with right:
        st.subheader("Top groups")
        if res is not None:
            st.pyplot(_chart(res))

    lo, ro = st.columns(2)
    with lo:
        st.subheader("Query log (FIFO completion order)")
        if ss.log:
            st.dataframe(pd.DataFrame(ss.log), height=300, use_container_width=True)
        else:
            st.caption("No completed queries yet.")
    with ro:
        st.subheader("Last SQL sent to DuckDB")
        st.code(res["sql"] if res else "Run a query to see the SQL.", language="sql")


def _chart(res):
    import matplotlib.pyplot as plt

    df, group_cols = res["df"], res["group_cols"]
    fig, ax = plt.subplots()
    if df.empty:
        ax.text(0.5, 0.5, "No rows match the filters", ha="center", va="center")
        ax.axis("off")
        return fig

    top = df.head(15).iloc[::-1]
    labels = top[group_cols].astype(str).agg(" / ".join, axis=1) if group_cols else ["Total"]
    ax.barh(range(len(top)), top["metric"], color="#4c78a8")
    ax.set_yticks(range(len(top)))
    ax.set_yticklabels(labels, fontsize=8)
    ax.set_xlabel(AGG_LABEL.get(ss.agg, "metric"))
    fig.tight_layout()
    return fig


live_panel()
