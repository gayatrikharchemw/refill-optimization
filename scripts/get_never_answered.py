"""
Read a CSV from S3 with a 'number' column, look up pickup rates for each number
across 2025 and 2026, and save the result sorted by pickup rate ascending.

Usage:
    uv run scripts/get_never_answered.py config.yml --input s3://bucket/path/numbers.csv --output results.csv
"""

import argparse
import logging
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from s3path import S3Path

from utils.config_utils import Config
from utils.file_utils import clean_ani, read_s3_csv, save_df_to_csv
from utils.nice_utils import get_pickup_rate_by_destination

logger = logging.getLogger(__name__)

YEAR_2025_START = datetime(2025, 1, 1, tzinfo=ZoneInfo("UTC"))
YEAR_2025_END   = datetime(2025, 12, 31, 23, 59, 59, tzinfo=ZoneInfo("UTC"))
YEAR_2026_START = datetime(2026, 1, 1, tzinfo=ZoneInfo("UTC"))


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] - %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser()
    parser.add_argument("config", type=Path, help="Path to config file")
    parser.add_argument("--input", required=True, help="S3 path to CSV with a 'number' column")
    parser.add_argument("--output", type=Path, required=True, help="Local path to save results CSV")
    args = parser.parse_args()

    input_df = read_s3_csv(S3Path(args.input))

    if "number" not in input_df.columns:
        raise ValueError(f"Input file must have a 'number' column. Found: {list(input_df.columns)}")

    input_df["number_normalized"] = input_df["number"].dropna().astype(str).str.strip().apply(clean_ani)
    destinations = input_df["number_normalized"].dropna().unique().tolist()
    logger.info(f"Checking {len(destinations)} phone numbers")

    config = Config(args.config)
    engine = config.get_outreach_db_engine()

    now = datetime.now(tz=ZoneInfo("UTC"))

    rates_2025 = get_pickup_rate_by_destination(engine, YEAR_2025_START, YEAR_2025_END, year=2025)
    rates_2025["destination"] = rates_2025["destination"].astype(str).apply(clean_ani)
    rates_2025 = rates_2025[rates_2025["destination"].isin(destinations)][
        ["destination", "total_calls", "answered_calls", "pickup_rate"]
    ].rename(columns={
        "total_calls": "total_calls_2025",
        "answered_calls": "answered_calls_2025",
        "pickup_rate": "pickup_rate_2025",
    })

    rates_2026 = get_pickup_rate_by_destination(engine, YEAR_2026_START, now, year=2026)
    rates_2026["destination"] = rates_2026["destination"].astype(str).apply(clean_ani)
    rates_2026 = rates_2026[rates_2026["destination"].isin(destinations)][
        ["destination", "total_calls", "answered_calls", "pickup_rate"]
    ].rename(columns={
        "total_calls": "total_calls_2026",
        "answered_calls": "answered_calls_2026",
        "pickup_rate": "pickup_rate_2026",
    })

    result = (
        input_df
        .merge(rates_2025, left_on="number_normalized", right_on="destination", how="left")
        .drop(columns=["destination"])
        .merge(rates_2026, left_on="number_normalized", right_on="destination", how="left")
        .drop(columns=["destination"])
        .sort_values("pickup_rate_2026", ascending=True)
        .reset_index(drop=True)
    )

    save_df_to_csv(result, args.output)
    logger.info(f"Saved {len(result)} rows to {args.output}")
