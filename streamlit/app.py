import os
import time
from datetime import datetime

import boto3
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine, text


st.set_page_config(page_title="Stock Pipeline Monitor", layout="wide")
st.title("Stock Pipeline Monitor")
st.caption("Tracks Airflow DAG state, MinIO lake activity, and Postgres gold metrics.")


def get_db_url() -> str:
    host = os.getenv("POSTGRES_HOST", "postgres")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "airflow")
    user = os.getenv("POSTGRES_USER", "airflow")
    password = os.getenv("POSTGRES_PASSWORD", "airflow")
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"


@st.cache_resource
def get_engine():
    return create_engine(get_db_url(), pool_pre_ping=True)


@st.cache_resource
def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.getenv("S3_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.getenv("S3_SECRET_KEY", "minioadmin"),
        region_name=os.getenv("S3_REGION", "us-east-1"),
    )


def query_df(sql: str) -> pd.DataFrame:
    with get_engine().connect() as conn:
        return pd.read_sql(text(sql), conn)


def query_df_params(sql: str, params: dict) -> pd.DataFrame:
    with get_engine().connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)


def query_scalar(sql: str):
    with get_engine().connect() as conn:
        result = conn.execute(text(sql)).scalar()
    return result


def table_exists(schema: str, table: str) -> bool:
    exists_sql = """
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = :schema_name
      AND table_name = :table_name
    LIMIT 1
    """
    with get_engine().connect() as conn:
        row = conn.execute(
            text(exists_sql),
            {"schema_name": schema, "table_name": table},
        ).first()
    return row is not None


def minio_prefix_snapshot(bucket: str, prefix: str, max_keys: int = 200):
    response = get_s3_client().list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=max_keys)
    contents = response.get("Contents", [])
    count = len(contents)
    latest = None
    latest_key = None
    if contents:
        latest_obj = max(contents, key=lambda x: x["LastModified"])
        latest = latest_obj["LastModified"]
        latest_key = latest_obj["Key"]
    return count, latest, latest_key, contents


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out = out.sort_values("event_ts").reset_index(drop=True)

    out["sma_5"] = out["close"].rolling(5).mean()
    out["sma_20"] = out["close"].rolling(20).mean()
    out["ema_20"] = out["close"].ewm(span=20, adjust=False).mean()
    out["ema_50"] = out["close"].ewm(span=50, adjust=False).mean()

    delta = out["close"].diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.rolling(14).mean()
    avg_loss = loss.rolling(14).mean()
    rs = avg_gain / avg_loss.replace(0, pd.NA)
    out["rsi_14"] = 100 - (100 / (1 + rs))

    typical_price = (out["high"] + out["low"] + out["close"]) / 3.0
    vol = out["volume"].fillna(0.0)
    cumulative_vol = vol.cumsum()
    out["vwap"] = (typical_price * vol).cumsum() / cumulative_vol.replace(0, pd.NA)

    bb_basis = out["close"].rolling(20).mean()
    bb_std = out["close"].rolling(20).std()
    out["bb_upper"] = bb_basis + (2 * bb_std)
    out["bb_lower"] = bb_basis - (2 * bb_std)

    ema_12 = out["close"].ewm(span=12, adjust=False).mean()
    ema_26 = out["close"].ewm(span=26, adjust=False).mean()
    out["macd"] = ema_12 - ema_26
    out["macd_signal"] = out["macd"].ewm(span=9, adjust=False).mean()
    out["macd_hist"] = out["macd"] - out["macd_signal"]

    high_low = out["high"] - out["low"]
    high_prev_close = (out["high"] - out["close"].shift(1)).abs()
    low_prev_close = (out["low"] - out["close"].shift(1)).abs()
    tr = pd.concat([high_low, high_prev_close, low_prev_close], axis=1).max(axis=1)
    out["atr_14"] = tr.rolling(14).mean()

    plus_dm = out["high"].diff()
    minus_dm = -out["low"].diff()
    plus_dm = plus_dm.where((plus_dm > minus_dm) & (plus_dm > 0), 0.0)
    minus_dm = minus_dm.where((minus_dm > plus_dm) & (minus_dm > 0), 0.0)

    plus_di = 100 * (plus_dm.rolling(14).mean() / out["atr_14"])
    minus_di = 100 * (minus_dm.rolling(14).mean() / out["atr_14"])
    dx = 100 * ((plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, pd.NA))
    out["adx_14"] = dx.rolling(14).mean()

    lowest_low_14 = out["low"].rolling(14).min()
    highest_high_14 = out["high"].rolling(14).max()
    out["stoch_k"] = 100 * ((out["close"] - lowest_low_14) / (highest_high_14 - lowest_low_14).replace(0, pd.NA))
    out["stoch_d"] = out["stoch_k"].rolling(3).mean()

    sma_tp = typical_price.rolling(20).mean()
    mean_dev = typical_price.rolling(20).apply(lambda x: np.mean(np.abs(x - np.mean(x))), raw=True)
    out["cci_20"] = (typical_price - sma_tp) / (0.015 * mean_dev.replace(0, pd.NA))

    out["stddev_20"] = out["close"].rolling(20).std()

    direction = out["close"].diff().fillna(0.0)
    signed_volume = direction.apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0)) * out["volume"].fillna(0.0)
    out["obv"] = signed_volume.cumsum()

    out["support_20"] = out["low"].rolling(20).min()
    out["resistance_20"] = out["high"].rolling(20).max()

    out["return_pct"] = out["close"].pct_change() * 100.0
    return out


def apply_time_axis(fig, compress_gaps: bool, bucket: str):
    if not compress_gaps:
        return

    rangebreaks = [dict(bounds=["sat", "mon"])]
    if bucket in ["Raw", "1H"]:
        # Market hours are roughly 09:30-16:00 ET; data is stored in UTC.
        rangebreaks.append(dict(pattern="hour", bounds=[21, 14.5]))

    fig.update_xaxes(rangebreaks=rangebreaks)


with st.sidebar:
    st.subheader("Refresh")
    auto_refresh = st.toggle("Auto refresh", value=True)
    refresh_sec = st.slider("Every (seconds)", min_value=15, max_value=120, value=30, step=5)
    st.caption(f"Last refresh: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


tab_monitor, tab_sql = st.tabs(["Pipeline Monitor", "Postgres Explorer"])

with tab_monitor:
    gold_ready = table_exists("public", "stock_bars_gold")
    c1, c2, c3 = st.columns(3)
    if gold_ready:
        try:
            gold_rows = query_scalar("SELECT COUNT(*) FROM public.stock_bars_gold")
        except Exception:
            gold_rows = None
        try:
            distinct_symbols = query_scalar("SELECT COUNT(DISTINCT symbol) FROM public.stock_bars_gold")
        except Exception:
            distinct_symbols = None
        try:
            max_event_ts = query_scalar("SELECT MAX(event_ts) FROM public.stock_bars_gold")
        except Exception:
            max_event_ts = None
    else:
        gold_rows = None
        distinct_symbols = None
        max_event_ts = None

    c1.metric("Gold Rows", f"{gold_rows:,}" if gold_rows is not None else "N/A")
    c2.metric("Tracked Symbols", f"{distinct_symbols:,}" if distinct_symbols is not None else "N/A")
    c3.metric("Latest Gold Timestamp", str(max_event_ts) if max_event_ts else "N/A")

    st.subheader("Airflow DAG Run Status")
    try:
        dag_runs = query_df(
            """
            SELECT dag_id, run_id, state, start_date, end_date
            FROM dag_run
            WHERE dag_id IN ('update_symbols_dag', 'historical_backfill_dag', 'intraday_pipeline_dag')
            ORDER BY start_date DESC
            LIMIT 30
            """
        )
        st.dataframe(dag_runs, use_container_width=True, hide_index=True)
    except Exception as exc:
        st.error(f"Unable to query dag_run table: {exc}")

    st.subheader("MinIO Lake Activity")
    try:
        bronze_count, bronze_latest, bronze_key, bronze_objects = minio_prefix_snapshot(
            "datalake", "bronze/stock_bars/"
        )
        silver_count, silver_latest, silver_key, silver_objects = minio_prefix_snapshot(
            "datalake", "silver/stock_bars_clean/"
        )

        b1, b2 = st.columns(2)
        b1.metric("Bronze Objects (sampled)", bronze_count)
        b2.metric("Silver Objects (sampled)", silver_count)

        c1, c2 = st.columns(2)
        c1.write(f"Bronze latest object: `{bronze_key}`" if bronze_key else "Bronze latest object: N/A")
        c1.write(f"Bronze latest modified: `{bronze_latest}`" if bronze_latest else "Bronze latest modified: N/A")
        c2.write(f"Silver latest object: `{silver_key}`" if silver_key else "Silver latest object: N/A")
        c2.write(f"Silver latest modified: `{silver_latest}`" if silver_latest else "Silver latest modified: N/A")
    except Exception as exc:
        st.error(f"Unable to query MinIO: {exc}")

    st.subheader("Gold Price Trend")
    if not gold_ready:
        st.info(
            "Gold table `public.stock_bars_gold` does not exist yet. "
            "Run `historical_backfill_dag` through `spark_to_gold` first."
        )
    else:
        try:
            symbols_df = query_df(
                """
                SELECT DISTINCT symbol
                FROM public.stock_bars_gold
                ORDER BY symbol
                """
            )
            if symbols_df.empty:
                st.info("No rows in public.stock_bars_gold yet.")
            else:
                c1, c2, c3 = st.columns(3)
                selected_symbol = c1.selectbox("Symbol", symbols_df["symbol"].tolist())
                timeframe = c2.selectbox(
                    "Timeframe",
                    ["1D", "5D", "1M", "3M", "6M", "1Y", "All"],
                    index=4,
                )
                chart_type = c3.selectbox("Chart Type", ["Line", "Bar", "Candlestick"], index=0)

                c4, c5, c6 = st.columns(3)
                bucket = c4.selectbox("Bucket", ["Raw", "1H", "1D", "1W"], index=0)
                indicators = c5.multiselect(
                    "Indicators",
                    [
                        "SMA(5)",
                        "SMA(20)",
                        "EMA(20)",
                        "EMA(50)",
                        "MACD",
                        "ADX(14)",
                        "RSI(14)",
                        "Stochastic",
                        "CCI(20)",
                        "Bollinger Bands",
                        "ATR(14)",
                        "StdDev(20)",
                        "OBV",
                        "VWAP",
                        "Support/Resistance",
                    ],
                    default=["SMA(20)", "RSI(14)", "VWAP", "Support/Resistance"],
                )
                compress_gaps = c6.toggle("Compress Market Gaps", value=True)

                trend_df = query_df_params(
                    """
                    SELECT event_ts, open, high, low, close, volume
                    FROM public.stock_bars_gold
                    WHERE symbol = :symbol
                    ORDER BY event_ts
                    LIMIT 50000
                    """,
                    {"symbol": selected_symbol},
                )
                if not trend_df.empty:
                    trend_df["event_ts"] = pd.to_datetime(trend_df["event_ts"], utc=True)

                    if timeframe != "All":
                        now = pd.Timestamp.now(tz="UTC")
                        lookback_map = {
                            "1D": pd.Timedelta(days=1),
                            "5D": pd.Timedelta(days=5),
                            "1M": pd.Timedelta(days=30),
                            "3M": pd.Timedelta(days=90),
                            "6M": pd.Timedelta(days=180),
                            "1Y": pd.Timedelta(days=365),
                        }
                        cutoff = now - lookback_map[timeframe]
                        trend_df = trend_df[trend_df["event_ts"] >= cutoff]

                    trend_df = trend_df.sort_values("event_ts")
                    if bucket != "Raw":
                        trend_df = (
                            trend_df.set_index("event_ts")
                            .resample(bucket)
                            .agg(
                                {
                                    "open": "first",
                                    "high": "max",
                                    "low": "min",
                                    "close": "last",
                                    "volume": "sum",
                                }
                            )
                            .dropna(subset=["open", "high", "low", "close"])
                            .reset_index()
                        )

                    trend_df = add_indicators(trend_df)
                    latest = trend_df.iloc[-1]
                    prev_close = trend_df["close"].iloc[-2] if len(trend_df) > 1 else pd.NA
                    pct_change = (
                        ((latest["close"] - prev_close) / prev_close) * 100.0
                        if pd.notna(prev_close) and prev_close != 0
                        else pd.NA
                    )
                    vwap_delta = (
                        ((latest["close"] - latest["vwap"]) / latest["vwap"]) * 100.0
                        if pd.notna(latest["vwap"]) and latest["vwap"] != 0
                        else pd.NA
                    )

                    k1, k2, k3, k4, k5 = st.columns(5)
                    k1.metric("Last Close", f"{latest['close']:.2f}")
                    k2.metric("Change %", f"{pct_change:.2f}%" if pd.notna(pct_change) else "N/A")
                    k3.metric("Last Volume", f"{int(latest['volume']):,}" if pd.notna(latest["volume"]) else "N/A")
                    k4.metric("RSI(14)", f"{latest['rsi_14']:.2f}" if pd.notna(latest["rsi_14"]) else "N/A")
                    k5.metric("VWAP Delta", f"{vwap_delta:.2f}%" if pd.notna(vwap_delta) else "N/A")

                    if chart_type == "Candlestick":
                        fig = go.Figure(
                            data=[
                                go.Candlestick(
                                    x=trend_df["event_ts"],
                                    open=trend_df["open"],
                                    high=trend_df["high"],
                                    low=trend_df["low"],
                                    close=trend_df["close"],
                                    name="OHLC",
                                )
                            ]
                        )
                        if "SMA(5)" in indicators:
                            fig.add_trace(
                                go.Scatter(
                                    x=trend_df["event_ts"],
                                    y=trend_df["sma_5"],
                                    mode="lines",
                                    name="SMA(5)",
                                )
                            )
                    elif chart_type == "Bar":
                        fig = px.bar(
                            trend_df,
                            x="event_ts",
                            y="close",
                            title=f"{selected_symbol} close ({timeframe}, {bucket})",
                        )
                    else:
                        fig = px.line(
                            trend_df,
                            x="event_ts",
                            y=["close"],
                            title=f"{selected_symbol} close ({timeframe}, {bucket})",
                        )

                    if "SMA(5)" in indicators:
                        fig.add_trace(go.Scatter(x=trend_df["event_ts"], y=trend_df["sma_5"], mode="lines", name="SMA(5)"))
                    if "SMA(20)" in indicators:
                        fig.add_trace(go.Scatter(x=trend_df["event_ts"], y=trend_df["sma_20"], mode="lines", name="SMA(20)"))
                    if "EMA(20)" in indicators:
                        fig.add_trace(go.Scatter(x=trend_df["event_ts"], y=trend_df["ema_20"], mode="lines", name="EMA(20)"))
                    if "EMA(50)" in indicators:
                        fig.add_trace(go.Scatter(x=trend_df["event_ts"], y=trend_df["ema_50"], mode="lines", name="EMA(50)"))
                    if "VWAP" in indicators:
                        fig.add_trace(go.Scatter(x=trend_df["event_ts"], y=trend_df["vwap"], mode="lines", name="VWAP"))
                    if "Bollinger Bands" in indicators:
                        fig.add_trace(go.Scatter(x=trend_df["event_ts"], y=trend_df["bb_upper"], mode="lines", name="BB Upper"))
                        fig.add_trace(go.Scatter(x=trend_df["event_ts"], y=trend_df["bb_lower"], mode="lines", name="BB Lower"))
                    if "Support/Resistance" in indicators:
                        fig.add_trace(
                            go.Scatter(x=trend_df["event_ts"], y=trend_df["support_20"], mode="lines", name="Support(20)")
                        )
                        fig.add_trace(
                            go.Scatter(
                                x=trend_df["event_ts"], y=trend_df["resistance_20"], mode="lines", name="Resistance(20)"
                            )
                        )

                    fig.update_layout(xaxis_title="Time", yaxis_title="Price")
                    apply_time_axis(fig, compress_gaps=compress_gaps, bucket=bucket)
                    st.plotly_chart(fig, use_container_width=True)

                    if "RSI(14)" in indicators:
                        rsi_fig = go.Figure()
                        rsi_fig.add_trace(go.Scatter(x=trend_df["event_ts"], y=trend_df["rsi_14"], mode="lines", name="RSI(14)"))
                        rsi_fig.add_hline(y=70, line_dash="dash")
                        rsi_fig.add_hline(y=30, line_dash="dash")
                        rsi_fig.update_layout(title="RSI(14)", yaxis_title="RSI", xaxis_title="Time")
                        apply_time_axis(rsi_fig, compress_gaps=compress_gaps, bucket=bucket)
                        st.plotly_chart(rsi_fig, use_container_width=True)

                    if "Stochastic" in indicators:
                        stoch_fig = go.Figure()
                        stoch_fig.add_trace(go.Scatter(x=trend_df["event_ts"], y=trend_df["stoch_k"], mode="lines", name="%K"))
                        stoch_fig.add_trace(go.Scatter(x=trend_df["event_ts"], y=trend_df["stoch_d"], mode="lines", name="%D"))
                        stoch_fig.add_hline(y=80, line_dash="dash")
                        stoch_fig.add_hline(y=20, line_dash="dash")
                        stoch_fig.update_layout(title="Stochastic Oscillator", yaxis_title="Value", xaxis_title="Time")
                        apply_time_axis(stoch_fig, compress_gaps=compress_gaps, bucket=bucket)
                        st.plotly_chart(stoch_fig, use_container_width=True)

                    if "CCI(20)" in indicators:
                        cci_fig = go.Figure()
                        cci_fig.add_trace(go.Scatter(x=trend_df["event_ts"], y=trend_df["cci_20"], mode="lines", name="CCI(20)"))
                        cci_fig.add_hline(y=100, line_dash="dash")
                        cci_fig.add_hline(y=-100, line_dash="dash")
                        cci_fig.update_layout(title="CCI(20)", yaxis_title="Value", xaxis_title="Time")
                        apply_time_axis(cci_fig, compress_gaps=compress_gaps, bucket=bucket)
                        st.plotly_chart(cci_fig, use_container_width=True)

                    if "MACD" in indicators:
                        macd_fig = go.Figure()
                        macd_fig.add_trace(go.Scatter(x=trend_df["event_ts"], y=trend_df["macd"], mode="lines", name="MACD"))
                        macd_fig.add_trace(
                            go.Scatter(x=trend_df["event_ts"], y=trend_df["macd_signal"], mode="lines", name="Signal")
                        )
                        macd_fig.add_trace(go.Bar(x=trend_df["event_ts"], y=trend_df["macd_hist"], name="Histogram"))
                        macd_fig.update_layout(title="MACD", yaxis_title="Value", xaxis_title="Time")
                        apply_time_axis(macd_fig, compress_gaps=compress_gaps, bucket=bucket)
                        st.plotly_chart(macd_fig, use_container_width=True)

                    if "ADX(14)" in indicators:
                        adx_fig = go.Figure()
                        adx_fig.add_trace(go.Scatter(x=trend_df["event_ts"], y=trend_df["adx_14"], mode="lines", name="ADX(14)"))
                        adx_fig.add_hline(y=25, line_dash="dash")
                        adx_fig.update_layout(title="ADX(14) - Trend Strength", yaxis_title="ADX", xaxis_title="Time")
                        apply_time_axis(adx_fig, compress_gaps=compress_gaps, bucket=bucket)
                        st.plotly_chart(adx_fig, use_container_width=True)

                    if "ATR(14)" in indicators:
                        atr_fig = go.Figure()
                        atr_fig.add_trace(go.Scatter(x=trend_df["event_ts"], y=trend_df["atr_14"], mode="lines", name="ATR(14)"))
                        atr_fig.update_layout(title="ATR(14)", yaxis_title="ATR", xaxis_title="Time")
                        apply_time_axis(atr_fig, compress_gaps=compress_gaps, bucket=bucket)
                        st.plotly_chart(atr_fig, use_container_width=True)

                    if "StdDev(20)" in indicators:
                        std_fig = go.Figure()
                        std_fig.add_trace(
                            go.Scatter(x=trend_df["event_ts"], y=trend_df["stddev_20"], mode="lines", name="StdDev(20)")
                        )
                        std_fig.update_layout(title="Standard Deviation(20)", yaxis_title="StdDev", xaxis_title="Time")
                        apply_time_axis(std_fig, compress_gaps=compress_gaps, bucket=bucket)
                        st.plotly_chart(std_fig, use_container_width=True)

                    if "OBV" in indicators:
                        obv_fig = go.Figure()
                        obv_fig.add_trace(go.Scatter(x=trend_df["event_ts"], y=trend_df["obv"], mode="lines", name="OBV"))
                        obv_fig.update_layout(title="On-Balance Volume (OBV)", yaxis_title="OBV", xaxis_title="Time")
                        apply_time_axis(obv_fig, compress_gaps=compress_gaps, bucket=bucket)
                        st.plotly_chart(obv_fig, use_container_width=True)

                    st.dataframe(trend_df.tail(50), use_container_width=True, hide_index=True)
                else:
                    st.info("No data found for the selected symbol/timeframe.")
        except Exception as exc:
            st.error(f"Unable to render gold trend: {exc}")

with tab_sql:
    st.subheader("Postgres Explorer")
    sql_default = "SELECT table_schema, table_name FROM information_schema.tables ORDER BY 1, 2 LIMIT 100;"
    sql = st.text_area("SQL query", value=sql_default, height=140)
    run = st.button("Run Query")
    if run:
        try:
            df = query_df(sql)
            st.success(f"Rows returned: {len(df)}")
            st.dataframe(df, use_container_width=True)
        except Exception as exc:
            st.error(f"Query failed: {exc}")


if auto_refresh:
    time.sleep(refresh_sec)
    st.rerun()
