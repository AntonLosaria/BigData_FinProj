"""
Weather Streaming Dashboard
Pipeline : WeatherAPI -> Kafka (producer) -> Spark Structured Streaming -> MongoDB
Dashboard: Kafka consumer (live view) + MongoDB (history view) + aggregation + export

Expected Kafka message (JSON). Field aliases are handled in normalize_record():
{
  "timestamp": "2026-10-25T14:30:00",   # or "localtime" / "last_updated"
  "location": "Quezon City",            # or "city"
  "country": "Philippines",
  "temp_c": 31.2, "feelslike_c": 36.0, "humidity": 70,
  "pressure_mb": 1008, "wind_kph": 12.4, "precip_mm": 0.0,
  "uv": 7, "cloud": 40, "condition": "Partly cloudy"
}

Run:  streamlit run dashboard.py
"""

import io
import json
import random
from datetime import datetime, timedelta

import pandas as pd
import plotly.express as px
import streamlit as st
from kafka import KafkaConsumer
from streamlit_autorefresh import st_autorefresh
import os
from dotenv import load_dotenv

load_dotenv()

try:
    from pymongo import MongoClient
except ImportError:  # pragma: no cover
    MongoClient = None

# ----------------------------------------------------------------------------
# Page config & styling
# ----------------------------------------------------------------------------
st.set_page_config(
    page_title="Weather Streaming Dashboard",
    page_icon="🌦️",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
    .block-container {padding-top: 1.5rem;}
    div[data-testid="stMetric"] {
        background: rgba(120,140,180,0.10);
        border: 1px solid rgba(120,140,180,0.25);
        border-radius: 12px; padding: 12px 16px;
    }
    .loc-card {
        border-radius: 14px; padding: 14px 18px; margin-bottom: 8px;
        background: linear-gradient(135deg, rgba(56,128,255,0.18), rgba(120,200,255,0.10));
        border: 1px solid rgba(100,150,255,0.30);
    }
    .loc-card h4 {margin: 0 0 4px 0;}
    .loc-card .big {font-size: 2rem; font-weight: 700;}
    </style>
    """,
    unsafe_allow_html=True,
)

METRICS = {
    "temp_c": ("Temperature", "°C"),
    "feelslike_c": ("Feels Like", "°C"),
    "humidity": ("Humidity", "%"),
    "pressure_mb": ("Pressure", "mb"),
    "wind_kph": ("Wind Speed", "km/h"),
    "precip_mm": ("Precipitation", "mm"),
    "uv": ("UV Index", ""),
    "cloud": ("Cloud Cover", "%"),
}

ALIASES = {
    "temp_c": ["temp_c", "temperature", "temp"],
    "feelslike_c": ["feelslike_c", "feels_like", "feelslike"],
    "humidity": ["humidity"],
    "pressure_mb": ["pressure_mb", "pressure"],
    "wind_kph": ["wind_kph", "wind_speed", "wind"],
    "precip_mm": ["precip_mm", "precipitation"],
    "uv": ["uv", "uv_index"],
    "cloud": ["cloud", "cloud_cover"],
}

METRIC_COL = {n: c for c, names in ALIASES.items() for n in names}

MAX_LIVE_BUFFER = 500


# ----------------------------------------------------------------------------
# Data helpers
# ----------------------------------------------------------------------------
def normalize_record(rec: dict) -> dict:
    """Map a raw Kafka/Mongo record onto the dashboard schema."""
    out = {
        "timestamp": rec.get("timestamp") or rec.get("localtime") or rec.get("last_updated"),
        "location": rec.get("location") or rec.get("city") or rec.get("sensor_id") or "Unknown",
        "country": rec.get("country", ""),
        "condition": rec.get("condition") or rec.get("weather") or "",
    }
    for col, names in ALIASES.items():
        for n in names:
            if n in rec and rec[n] is not None:
                out[col] = rec[n]
                break
    mt, val = rec.get("metric_type"), rec.get("value")
    if mt is not None and val is not None:
        col = METRIC_COL.get(str(mt).lower())
        if col:
            out[col] = val
    return out

def merge_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """One-metric-per-row data -> one row per location and minute."""
    cols = [m for m in METRICS if m in df.columns]
    if not cols or df[cols].notna().sum(axis=1).max() > 1:
        return df
    df = df.copy()
    df["timestamp"] = df["timestamp"].dt.floor("min")
    agg = {c: "first" for c in cols + ["country", "condition"]}
    return df.groupby(["timestamp", "location"], as_index=False).agg(agg)

def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    """Parse timestamps (naive), coerce numerics, sort."""
    if df.empty:
        return df
    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True).dt.tz_localize(None)
    df = df.dropna(subset=["timestamp"])
    for col in METRICS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    for col in ("location", "country", "condition"):
        if col not in df.columns:
            df[col] = ""
    df = merge_metrics(df)
    return df.sort_values("timestamp").reset_index(drop=True)


def available_metrics(df: pd.DataFrame):
    return [m for m in METRICS if m in df.columns and df[m].notna().any()]


def metric_label(m: str) -> str:
    name, unit = METRICS[m]
    return f"{name} ({unit})" if unit else name


def generate_demo_records(n=1):
    """Synthetic weather data, used only when Demo Mode is switched on."""
    cities = ["Quezon City", "Cebu City", "Davao City"]
    base = {"Quezon City": 31, "Cebu City": 30, "Davao City": 29}
    now = datetime.utcnow()
    rows = []
    for _ in range(n):
        for c in cities:
            t = base[c] + random.uniform(-2, 2)
            rows.append({
                "timestamp": now.isoformat(), "location": c, "country": "Philippines",
                "temp_c": round(t, 1), "feelslike_c": round(t + random.uniform(2, 5), 1),
                "humidity": random.randint(60, 90), "pressure_mb": random.randint(1004, 1012),
                "wind_kph": round(random.uniform(3, 20), 1), "precip_mm": round(random.choice([0, 0, 0.2, 1.1]), 1),
                "uv": random.randint(1, 10), "cloud": random.randint(10, 90),
                "condition": random.choice(["Sunny", "Partly cloudy", "Light rain", "Cloudy"]),
            })
    return rows


# ----------------------------------------------------------------------------
# Kafka (live) & MongoDB (history)
# ----------------------------------------------------------------------------
@st.cache_resource(show_spinner=False)
def get_consumer(broker: str, topic: str):
    """One long-lived consumer, shared across reruns (not recreated every 15 s)."""
    return KafkaConsumer(
        topic,
        bootstrap_servers=[broker],
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="weather-dashboard-live",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        consumer_timeout_ms=3000,
    )


@st.cache_resource(show_spinner=False)
def get_mongo(uri: str):
    if MongoClient is None:
        raise RuntimeError("pymongo is not installed (pip install pymongo)")
    client = MongoClient(uri, serverSelectionTimeoutMS=4000)
    client.admin.command("ping")
    return client


def fetch_recent_from_mongo(cfg, limit=100):
    try:
        coll = get_mongo(cfg["mongo_uri"])[cfg["mongo_db"]][cfg["mongo_collection"]]
        docs = list(coll.find({}, {"_id": 0}).sort("_id", -1).limit(limit))
        return [normalize_record(d) for d in reversed(docs)]
    except Exception:
        return []


def update_live_buffer(cfg):
    """Poll Kafka once and append to the session buffer. Returns (new_count, error)."""
    buf = st.session_state.setdefault("live_buffer", [])

    if cfg["demo_mode"]:
        new = generate_demo_records(1)
        buf.extend(new)
        del buf[:-MAX_LIVE_BUFFER]
        return len(new), None

    # Warm start: pre-fill the chart from MongoDB so it isn't empty on first load
    if not buf and not st.session_state.get("prefilled"):
        buf.extend(fetch_recent_from_mongo(cfg))
        st.session_state["prefilled"] = True

    try:
        consumer = get_consumer(cfg["kafka_broker"], cfg["kafka_topic"])
        packs = consumer.poll(timeout_ms=3000, max_records=200)
        new = 0
        for msgs in packs.values():
            for m in msgs:
                if isinstance(m.value, dict):
                    buf.append(normalize_record(m.value))
                    new += 1
        del buf[:-MAX_LIVE_BUFFER]
        st.session_state["last_kafka_msg"] = datetime.now() if new else st.session_state.get("last_kafka_msg")
        return new, None
    except Exception as e:
        get_consumer.clear()  # force reconnect next refresh
        return 0, str(e)


@st.cache_data(ttl=15, show_spinner=False)
def fetch_history(uri, db, collection, locations):
    """All stored readings (optionally per location). Date filtering is done in pandas
    so it works whether Spark stored timestamps as strings or BSON dates."""
    coll = get_mongo(uri)[db][collection]
    query = {"$or": [{"location": {"$in": list(locations)}}, {"city": {"$in": list(locations)}}]} if locations else {}
    docs = list(coll.find(query, {"_id": 0}).limit(200_000))
    return clean_df(pd.DataFrame([normalize_record(d) for d in docs]))


@st.cache_data(ttl=60, show_spinner=False)
def fetch_locations(uri, db, collection):
    coll = get_mongo(uri)[db][collection]
    locs = set(coll.distinct("location")) | set(coll.distinct("city"))
    return sorted(x for x in locs if x)


# ----------------------------------------------------------------------------
# Aggregation
# ----------------------------------------------------------------------------
FREQ = {"raw": None, "hourly": "h", "daily": "D", "weekly": "W"}


def aggregate(df: pd.DataFrame, agg: str, metrics):
    """Mean per location & period (raw returns the data untouched)."""
    if agg == "raw" or df.empty or not metrics:
        return df
    out = (
        df.set_index("timestamp")
        .groupby("location")[metrics]
        .resample(FREQ[agg])
        .mean()
        .round(2)
        .dropna(how="all")
        .reset_index()
    )
    return out


def summary_table(df: pd.DataFrame, metrics):
    """Count / avg / min / max per location & metric."""
    rows = []
    for loc, g in df.groupby("location"):
        for m in metrics:
            s = g[m].dropna()
            if s.empty:
                continue
            rows.append({
                "Location": loc, "Metric": metric_label(m), "Records": len(s),
                "Average": round(s.mean(), 2), "Min": round(s.min(), 2),
                "Max": round(s.max(), 2), "Std Dev": round(s.std(), 2) if len(s) > 1 else 0.0,
            })
    return pd.DataFrame(rows)


# ----------------------------------------------------------------------------
# Export
# ----------------------------------------------------------------------------
def to_excel_bytes(data: pd.DataFrame, summary: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as xw:
        data.to_excel(xw, sheet_name="Data", index=False)
        if not summary.empty:
            summary.to_excel(xw, sheet_name="Summary", index=False)
        for ws in xw.book.worksheets:
            ws.freeze_panes = "A2"
            for col in ws.columns:
                width = max(len(str(c.value)) if c.value is not None else 0 for c in col[:200])
                ws.column_dimensions[col[0].column_letter].width = min(max(width + 2, 10), 40)
    return buf.getvalue()


def fig_to_png(fig, w=1100, h=500):
    try:
        return fig.to_image(format="png", width=w, height=h, scale=2)  # needs kaleido
    except Exception:
        return None


def to_pdf_bytes(data: pd.DataFrame, summary: pd.DataFrame, title: str, chart_png=None) -> bytes:
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4, landscape
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.lib.units import cm
    from reportlab.platypus import Image, PageBreak, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

    def make_table(frame, max_rows):
        frame = frame.head(max_rows).copy()
        for c in frame.columns:
            if pd.api.types.is_datetime64_any_dtype(frame[c]):
                frame[c] = frame[c].dt.strftime("%Y-%m-%d %H:%M")
        rows = [list(map(str, frame.columns))] + frame.astype(str).values.tolist()
        t = Table(rows, repeatRows=1)
        t.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1f4e9c")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("FONTSIZE", (0, 0), (-1, -1), 7),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#eef3fb")]),
            ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
        ]))
        return t

    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=landscape(A4), leftMargin=1.2 * cm, rightMargin=1.2 * cm,
                            topMargin=1.2 * cm, bottomMargin=1.2 * cm)
    st_ = getSampleStyleSheet()
    story = [
        Paragraph(title, st_["Title"]),
        Paragraph(f"Generated {datetime.now():%Y-%m-%d %H:%M:%S} &nbsp;|&nbsp; Records: {len(data):,}", st_["Normal"]),
        Spacer(1, 0.4 * cm),
    ]
    if chart_png:
        story += [Image(io.BytesIO(chart_png), width=24 * cm, height=10.9 * cm), Spacer(1, 0.3 * cm)]
    if not summary.empty:
        story += [Paragraph("Summary Statistics", st_["Heading2"]), make_table(summary, 40), PageBreak()]
    story += [Paragraph("Data (first 150 rows)", st_["Heading2"]), make_table(data, 150)]
    doc.build(story)
    return buf.getvalue()


# ----------------------------------------------------------------------------
# Sidebar
# ----------------------------------------------------------------------------
def setup_sidebar():
    st.sidebar.title("🌦️ Dashboard Controls")

    st.sidebar.subheader("Kafka (Live)")
    kafka_broker = st.sidebar.text_input("Kafka Broker", value="localhost:9092")
    kafka_topic = st.sidebar.text_input("Kafka Topic", value="streaming-data")

    st.sidebar.subheader("MongoDB (History)")
    mongo_uri = st.sidebar.text_input("Mongo URI", value=os.getenv("MONGO_URI", ""))
    mongo_db = st.sidebar.text_input("Database", value="bigdata")
    mongo_collection = st.sidebar.text_input("Collection", value="readings")

    demo_mode = st.sidebar.checkbox("Demo mode (synthetic data, no Kafka)", value=False,
                                    help="For testing the UI before the pipeline is running.")

    st.sidebar.subheader("Refresh")
    auto = st.sidebar.checkbox("Enable Auto Refresh", value=True)
    interval = st.sidebar.slider("Interval (seconds)", 5, 60, 15) if auto else 15

    if st.sidebar.button("🔄 Manual Refresh", use_container_width=True):
        fetch_history.clear()
        st.rerun()

    if st.sidebar.button("🗑️ Clear Live Buffer", use_container_width=True):
        st.session_state["live_buffer"] = []
        st.session_state["prefilled"] = False

    return {
        "kafka_broker": kafka_broker, "kafka_topic": kafka_topic,
        "mongo_uri": mongo_uri, "mongo_db": mongo_db, "mongo_collection": mongo_collection,
        "demo_mode": demo_mode, "auto_refresh": auto, "refresh_interval": interval,
    }


# ----------------------------------------------------------------------------
# Live view
# ----------------------------------------------------------------------------
def display_real_time_view(cfg):
    st.header("📈 Live Weather Stream")

    with st.spinner("Polling Kafka..."):
        new, err = update_live_buffer(cfg)

    st.session_state["last_refresh"] = datetime.now()
    status_cols = st.columns([2, 2, 3])
    status_cols[0].info(f"**Auto-refresh:** {'🟢 On' if cfg['auto_refresh'] else '🔴 Off'} · every {cfg['refresh_interval']}s")
    status_cols[1].info(f"**New messages:** {new}")
    if err:
        status_cols[2].error(f"Kafka error: {err[:120]}")
    elif cfg["demo_mode"]:
        status_cols[2].warning("Demo mode: showing synthetic data")
    else:
        last = st.session_state.get("last_kafka_msg")
        age = (datetime.now() - last).total_seconds() if last else None
        icon = "🟢" if age is not None and age < 60 else "🟡"
        status_cols[2].success(f"{icon} Last Kafka message: {last:%H:%M:%S}" if last
                               else "🟡 Connected, waiting for new messages...")

    df = clean_df(pd.DataFrame(st.session_state.get("live_buffer", [])))
    if df.empty:
        st.warning("No live data yet. Make sure the producer is running and the topic name matches.")
        return

    # Location cards (latest reading each)
    latest = df.sort_values("timestamp").groupby("location").tail(1)
    cols = st.columns(min(len(latest), 4) or 1)
    for i, (_, r) in enumerate(latest.iterrows()):
        with cols[i % len(cols)]:
            temp = f"{r['temp_c']:.1f}°C" if "temp_c" in r and pd.notna(r.get("temp_c")) else "—"
            hum = f"{r['humidity']:.0f}%" if "humidity" in r and pd.notna(r.get("humidity")) else "—"
            wind = f"{r['wind_kph']:.1f} km/h" if "wind_kph" in r and pd.notna(r.get("wind_kph")) else "—"
            st.markdown(
                f"""<div class="loc-card"><h4>📍 {r['location']}</h4>
                <div class="big">{temp}</div>
                <div>{r['condition'] or ''}</div>
                <div>💧 {hum} &nbsp; 💨 {wind}</div>
                <small>{r['timestamp']:%Y-%m-%d %H:%M:%S}</small></div>""",
                unsafe_allow_html=True,
            )

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Records in Buffer", len(df))
    k2.metric("Locations", df["location"].nunique())
    if "temp_c" in df:
        k3.metric("Avg Temperature", f"{df['temp_c'].mean():.1f} °C")
        k4.metric("Max Temperature", f"{df['temp_c'].max():.1f} °C")

    mets = available_metrics(df)
    c1, c2 = st.columns([1, 3])
    metric = c1.selectbox("Metric", mets, format_func=metric_label, key="live_metric")
    window = c1.selectbox("Window", ["Last 50", "Last 100", "Last 250", "All buffered"], index=1)
    sub = df if window == "All buffered" else df.groupby("location").tail(int(window.split()[1]))
    fig = px.line(sub, x="timestamp", y=metric, color="location", markers=True,
                  labels={metric: metric_label(metric), "timestamp": "Time"}, template="plotly_white",
                  title=f"Live {METRICS[metric][0]}")
    fig.update_layout(hovermode="x unified", legend_title_text="Location", margin=dict(t=50, b=10))
    c2.plotly_chart(fig, use_container_width=True)

    if "condition" in df and df["condition"].astype(bool).any():
        d1, d2 = st.columns(2)
        cond = latest["condition"].value_counts().reset_index()
        cond.columns = ["Condition", "Locations"]
        d1.plotly_chart(px.pie(cond, names="Condition", values="Locations", hole=0.45,
                               title="Current Conditions"), use_container_width=True)
        d2.plotly_chart(px.bar(latest, x="location", y=metric, color="location",
                               title=f"Latest {METRICS[metric][0]} by Location",
                               labels={"location": "Location", metric: metric_label(metric)}),
                        use_container_width=True)

    with st.expander("📋 Raw live data"):
        st.dataframe(df.sort_values("timestamp", ascending=False), use_container_width=True, height=300)


# ----------------------------------------------------------------------------
# History view
# ----------------------------------------------------------------------------
def display_historical_view(cfg):
    st.header("📊 Historical Data (MongoDB)")

    try:
        all_locs = fetch_locations(cfg["mongo_uri"], cfg["mongo_db"], cfg["mongo_collection"])
    except Exception as e:
        st.error(f"Cannot connect to MongoDB: {e}")
        return

    f1, f2, f3, f4 = st.columns([2, 2, 2, 2])
    today = datetime.now().date()
    dates = f1.date_input("Date range", value=(today - timedelta(days=7), today))
    sel_locs = f2.multiselect("Locations", all_locs, default=all_locs)
    agg = f3.selectbox("Aggregation", list(FREQ), index=0)
    page_size = f4.selectbox("Rows per page", [25, 50, 100, 500], index=1)

    try:
        df = fetch_history(cfg["mongo_uri"], cfg["mongo_db"], cfg["mongo_collection"], tuple(sel_locs))
    except Exception as e:
        st.error(f"MongoDB query failed: {e}")
        return

    if df.empty:
        st.warning("No historical data found. Check the collection name and that Spark is writing to MongoDB.")
        return

    if isinstance(dates, (list, tuple)) and len(dates) == 2:
        start, end = dates
        df = df[(df["timestamp"] >= pd.Timestamp(start)) &
                (df["timestamp"] < pd.Timestamp(end) + pd.Timedelta(days=1))]
    if df.empty:
        st.warning("No records in the selected date range.")
        return

    mets = available_metrics(df)
    chosen = st.multiselect("Metrics to analyze", mets, default=mets[:3], format_func=metric_label)
    if not chosen:
        st.info("Select at least one metric.")
        return

    view = aggregate(df, agg, chosen)
    summ = summary_table(df, chosen)

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total Records", f"{len(df):,}")
    m2.metric("Locations", df["location"].nunique())
    m3.metric("From", f"{df['timestamp'].min():%Y-%m-%d}")
    m4.metric("To", f"{df['timestamp'].max():%Y-%m-%d}")

    t_trend, t_agg, t_table = st.tabs(["📈 Trends", "🧮 Aggregation Summary", "📋 All Records"])

    trend_metric = None
    trend_fig = None
    with t_trend:
        trend_metric = st.selectbox("Chart metric", chosen, format_func=metric_label, key="hist_metric")
        trend_fig = px.line(view, x="timestamp", y=trend_metric, color="location", template="plotly_white",
                            title=f"{METRICS[trend_metric][0]} – {agg.capitalize()}",
                            labels={trend_metric: metric_label(trend_metric), "timestamp": "Time"})
        trend_fig.update_layout(hovermode="x unified")
        st.plotly_chart(trend_fig, use_container_width=True)
        if len(df["location"].unique()) > 1:
            st.plotly_chart(px.box(df, x="location", y=trend_metric, color="location",
                                   title=f"{METRICS[trend_metric][0]} distribution by location",
                                   template="plotly_white"), use_container_width=True)

    with t_agg:
        st.caption("Computed over the filtered records (count, average, min, max, std dev).")
        st.dataframe(summ, use_container_width=True, hide_index=True)
        if len(chosen) > 1:
            corr = df[chosen].corr().round(2)
            st.plotly_chart(px.imshow(corr, text_auto=True, color_continuous_scale="RdBu_r", zmin=-1, zmax=1,
                                      title="Metric correlation"), use_container_width=True)

    with t_table:
        shown_cols = [c for c in view.columns if c not in ("country",)] if agg == "raw" else list(view.columns)
        table = view[shown_cols].sort_values("timestamp", ascending=False).reset_index(drop=True)
        pages = max(1, -(-len(table) // page_size))
        p1, p2 = st.columns([1, 5])
        page = p1.number_input("Page", 1, pages, 1)
        p2.caption(f"Page {page} of {pages} · {len(table):,} rows (every collected record is available here)")
        st.dataframe(table.iloc[(page - 1) * page_size: page * page_size],
                     use_container_width=True, hide_index=True)

    # ---- Export ----------------------------------------------------------
    st.subheader("⬇️ Export")
    export_df = view.copy()
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    e1, e2, e3, e4 = st.columns(4)
    e1.download_button("CSV", export_df.to_csv(index=False).encode(), f"weather_{stamp}.csv",
                       "text/csv", use_container_width=True)
    try:
        e2.download_button("Excel (.xlsx)", to_excel_bytes(export_df, summ), f"weather_{stamp}.xlsx",
                           "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                           use_container_width=True)
    except Exception as e:
        e2.error(f"Excel export unavailable: {e} (pip install openpyxl)")

    png = fig_to_png(trend_fig) if trend_fig is not None else None
    try:
        e3.download_button("PDF report", to_pdf_bytes(export_df, summ, "Weather Data Report", png),
                           f"weather_report_{stamp}.pdf", "application/pdf", use_container_width=True)
    except Exception as e:
        e3.error(f"PDF export unavailable: {e} (pip install reportlab)")

    if png:
        e4.download_button("Chart (PNG)", png, f"weather_chart_{stamp}.png", "image/png", use_container_width=True)
    else:
        e4.button("Chart (PNG)", disabled=True, use_container_width=True,
                  help="Install kaleido: pip install kaleido")


# ----------------------------------------------------------------------------
# About
# ----------------------------------------------------------------------------
def display_about():
    st.header("ℹ️ About this Dashboard")
    st.markdown(
        """
**Pipeline:** WeatherAPI → Kafka Producer → Kafka topic → Spark Structured Streaming → MongoDB

| Stage | Role |
|---|---|
| WeatherAPI | REST data source (current conditions per city) |
| Kafka | Decouples the producer from consumers; buffers the stream |
| Spark Structured Streaming | Parses/cleans JSON and writes micro-batches to MongoDB |
| MongoDB | Stores every reading as a JSON document (flexible schema, easy to query) |
| Streamlit | **Live tab** reads Kafka directly; **History tab** reads MongoDB |

**Dashboard significance:** one place to monitor current weather, compare locations, review
all collected records, aggregate trends (hourly/daily/weekly), and export reports.
        """
    )


# ----------------------------------------------------------------------------
def main():
    st.title("🌦️ Weather Streaming Dashboard")
    cfg = setup_sidebar()

    if cfg["auto_refresh"]:
        st_autorefresh(interval=cfg["refresh_interval"] * 1000, key="auto_refresh")

    last = st.session_state.get("last_refresh")
    st.sidebar.markdown("---")
    st.sidebar.metric("Last Refresh", last.strftime("%H:%M:%S") if last else "—")

    tab1, tab2, tab3 = st.tabs(["📈 Live Stream", "📊 History & Export", "ℹ️ About"])
    with tab1:
        display_real_time_view(cfg)
    with tab2:
        display_historical_view(cfg)
    with tab3:
        display_about()


if __name__ == "__main__":
    main()