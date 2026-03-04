"""
Fetch stock bars from yfinance and publish them to Kafka.
"""
import argparse
import json
from datetime import datetime, timezone

import yfinance as yf
from kafka import KafkaProducer


def scalar(v):
    if hasattr(v, "iloc"):
        return v.iloc[0]
    return v


def to_iso_dt(v):
    if hasattr(v, "to_pydatetime"):
        return v.to_pydatetime().isoformat()
    if hasattr(v, "isoformat"):
        return v.isoformat()
    return str(v)


def fetch_bars(symbol: str, period: str, interval: str) -> list[dict]:
    df = yf.download(
        tickers=symbol,
        period=period,
        interval=interval,
        progress=False,
        auto_adjust=False,
    )
    if df.empty:
        return []

    df = df.reset_index()
    time_col = "Datetime" if "Datetime" in df.columns else "Date"
    emitted_at = datetime.now(timezone.utc).isoformat()
    rows = []

    for _, row in df.iterrows():
        raw_time = row[time_col]
        if hasattr(raw_time, "iloc"):
            raw_time = raw_time.iloc[0]
        event_time = to_iso_dt(raw_time)

        rows.append(
            {
                "symbol": symbol,
                "event_time": event_time,
                "open": float(scalar(row["Open"])),
                "high": float(scalar(row["High"])),
                "low": float(scalar(row["Low"])),
                "close": float(scalar(row["Close"])),
                "adj_close": float(scalar(row["Adj Close"])),
                "volume": int(scalar(row["Volume"])),
                "source": "yfinance",
                "emitted_at_utc": emitted_at,
            }
        )
    return rows


def publish_events(
    bootstrap_servers: str,
    topic: str,
    symbols: list[str],
    period: str,
    interval: str,
) -> int:
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )
    sent = 0
    try:
        for symbol in symbols:
            for event in fetch_bars(symbol=symbol, period=period, interval=interval):
                producer.send(topic, key=symbol, value=event)
                sent += 1
        producer.flush()
    finally:
        producer.close()
    return sent


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest yfinance bars to Kafka")
    parser.add_argument("--bootstrap-servers", default="kafka:9092")
    parser.add_argument("--topic", default="stock_bars_raw")
    parser.add_argument("--symbols", default="AAPL,MSFT,GOOGL,AMZN,TSLA")
    parser.add_argument("--period", default="5d")
    parser.add_argument("--interval", default="1h")
    args = parser.parse_args()

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    total = publish_events(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        symbols=symbols,
        period=args.period,
        interval=args.interval,
    )
    print(f"Published {total} yfinance bar events to topic '{args.topic}'")
