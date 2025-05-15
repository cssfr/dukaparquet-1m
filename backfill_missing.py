
#!/usr/bin/env python3

import subprocess
from datetime import datetime, timedelta, date
from pathlib import Path
import yaml
import pandas as pd
import json

OUTPUT_DIR = Path("ohlcv_1m")
DOWNLOAD_DIR = Path("download")
SYMBOLS_FILE = Path("symbols.yaml")

SYMBOLS = yaml.safe_load(SYMBOLS_FILE.read_text())

def convert_to_parquet(input_csv_path: str, output_parquet_path: str, symbol: str):
    df = pd.read_csv(input_csv_path)
    df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y-%m-%d %H:%M', utc=True)
    df['unix_time'] = df['timestamp'].astype('int64') // 10**9
    if 'volume' in df.columns:
        df['volume'] = df['volume'].astype('float64')
    df.insert(0, 'symbol', symbol)
    cols = df.columns.tolist()
    cols.insert(cols.index('timestamp') + 1, cols.pop(cols.index('unix_time')))
    df = df[cols]
    df.to_parquet(output_parquet_path, index=False)

def run_dukascopy(symbol_id: str, date_str: str):
    cmd = [
        "npx", "dukascopy-node",
        "-i", symbol_id,
        "-from", date_str,
        "-to", (datetime.fromisoformat(date_str) + timedelta(days=1)).strftime("%Y-%m-%d"),
        "-t", "m1",
        "-f", "csv",
        '--date-format \"YYYY-MM-DD HH:mm\"',
        "-v",
        "-fl",
    ]
    subprocess.run(" ".join(cmd), check=True, shell=True)

def list_parquet_dates_remote(symbol_key: str):
    # List remote objects and parse dates
    proc = subprocess.run(
        ["mc", "ls", "--json", f"myminio/dukascopy-node/ohlcv_1m/symbol={symbol_key}/"],
        capture_output=True, text=True, check=True
    )
    dates = []
    for line in proc.stdout.splitlines():
        obj = json.loads(line)
        key = obj.get("key", "")
        if "date=" in key and key.endswith(".parquet"):
            part = key.split("date=")[1].split(".")[0]
            try:
                dates.append(datetime.strptime(part, "%Y-%m-%d").date())
            except ValueError:
                pass
    return dates

def ingest_symbol_backfill(symbol_key: str, earliest_required: date, earliest_available: date):
    meta = SYMBOLS[symbol_key]
    dukas_id = meta["id"]
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

    current = earliest_required
    while current < earliest_available:
        date_str = current.strftime("%Y-%m-%d")
        next_day_str = (current + timedelta(days=1)).strftime("%Y-%m-%d")
        parquet_path = OUTPUT_DIR / f"symbol={symbol_key}" / f"date={date_str}.parquet"

        if parquet_path.exists():
            current += timedelta(days=1)
            continue

        try:
            run_dukascopy(dukas_id, date_str)
            csv_name = f"{dukas_id}-m1-bid-{date_str}-{next_day_str}.csv"
            csv_path = DOWNLOAD_DIR / csv_name
            if csv_path.exists():
                parquet_path.parent.mkdir(parents=True, exist_ok=True)
                convert_to_parquet(str(csv_path), str(parquet_path), symbol_key)
                print(f"[{symbol_key}] ✔ backfilled {date_str}")
            else:
                print(f"[{symbol_key}] ❌ CSV not found for {date_str}")
        except Exception as e:
            print(f"[{symbol_key}] ❌ Error on {date_str}: {e}")
        current += timedelta(days=1)

def main():
    for symbol in SYMBOLS:
        earliest_required = datetime.strptime(SYMBOLS[symbol]["earliest_date"], "%Y-%m-%d").date()
        existing_dates = list_parquet_dates_remote(symbol)
        if not existing_dates:
            print(f"[{symbol}] No existing parquet; skipping backfill.")
            continue
        earliest_available = min(existing_dates)
        if earliest_required < earliest_available:
            print(f"[{symbol}] Backfilling {earliest_required} to {earliest_available - timedelta(days=1)}")
            ingest_symbol_backfill(symbol, earliest_required, earliest_available)
        else:
            print(f"[{symbol}] Already has full history; nothing to backfill.")

if __name__ == "__main__":
    main()
