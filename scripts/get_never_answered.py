"""
Find phone numbers dialed in both 2025 and 2026 that are consistently not answering,
and check whether they called us back on the inbound skill.

Outputs a CSV sorted by total combined outbound attempts (descending), so the worst
offenders — most wasted dials, never answered, never called back — are at the top.
Saved to paths.optimization in the config.

Usage:
    uv run scripts/get_never_answered.py config/humana_refill_etl_prod_config.yml
    uv run scripts/get_never_answered.py config/humana_refill_etl_prod_config.yml --min-attempts 5
"""

import argparse
import logging
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from utils.config_utils import HumanaRefillConfig
from utils.file_utils import clean_ani
from utils.nice_utils import get_inbound_callbacks_by_number, get_outbound_stats_by_number

logger = logging.getLogger(__name__)

OUTBOUND_SKILLS_2025 = ["Refill-Humana-MANUAL-OB", "Refill-Humana-OB", "Refill-Humana-AL"]
INBOUND_SKILLS_2025  = ["Refill-Humana-IB", "Refill-Humana-VM"]

OUTBOUND_SKILLS_2026 = ["Refill-Humana-ENG-MN", "Refill-Humana-ENG-OB", "Refill-Humana-SPA-OB"]
INBOUND_SKILLS_2026  = ["Refill-Humana-ENG-IB", "Refill-Humana-ENG-VM", "Refill-Humana-SPA-IB"]

YEAR_2025_START = datetime(2025, 1, 1, tzinfo=ZoneInfo("UTC"))
YEAR_2025_END   = datetime(2025, 12, 31, 23, 59, 59, tzinfo=ZoneInfo("UTC"))
YEAR_2026_START = datetime(2026, 1, 1, tzinfo=ZoneInfo("UTC"))


def _save(df, path):
    """Save DataFrame to CSV, supporting both local paths and S3."""
    csv_bytes = df.fillna("").to_csv(index=False, lineterminator="\n").encode()
    if not hasattr(path, "write_bytes"):
        path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(csv_bytes)


def _normalize(df, col="member_phone"):
    df[col] = df[col].astype(str).str.strip().apply(clean_ani)
    return df.dropna(subset=[col])


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] - %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser()
    parser.add_argument("config_path", type=Path, help="Path to config file")
    parser.add_argument("--min-attempts", type=int, default=3, help="Minimum 2026 outbound attempts to include (default: 3)")
    args = parser.parse_args()

    config = HumanaRefillConfig(config_file=args.config_path)
    engine = config.get_outreach_db_engine()
    now = datetime.now(tz=ZoneInfo("UTC"))

    # --- outbound stats ---
    logger.info("Fetching 2026 outbound stats...")
    ob_2026 = _normalize(get_outbound_stats_by_number(engine, YEAR_2026_START, now, OUTBOUND_SKILLS_2026, year=2026))
    ob_2026 = ob_2026.rename(columns={
        "total_calls": "attempts_2026",
        "answered_calls": "pickups_2026",
        "pickup_rate": "pickup_rate_2026",
    })

    logger.info("Fetching 2025 outbound stats...")
    ob_2025 = _normalize(get_outbound_stats_by_number(engine, YEAR_2025_START, YEAR_2025_END, OUTBOUND_SKILLS_2025, year=2025))
    ob_2025 = ob_2025.rename(columns={
        "total_calls": "attempts_2025",
        "answered_calls": "pickups_2025",
        "pickup_rate": "pickup_rate_2025",
    })

    # inner join — only numbers seen in both years
    logger.info("Joining 2025 and 2026 outbound data...")
    result = ob_2026.merge(ob_2025, on="member_phone", how="inner")
    logger.info(f"  {len(result):,} numbers appeared in both years")

    # apply min-attempts filter on 2026
    result = result[result["attempts_2026"] >= args.min_attempts].reset_index(drop=True)
    logger.info(f"  {len(result):,} numbers with >= {args.min_attempts} attempts in 2026")

    # --- inbound callbacks ---
    logger.info("Fetching 2026 inbound callbacks...")
    ib_2026 = _normalize(get_inbound_callbacks_by_number(engine, YEAR_2026_START, now, INBOUND_SKILLS_2026, year=2026))
    ib_2026 = ib_2026.rename(columns={"inbound_calls": "inbound_callbacks_2026"})

    logger.info("Fetching 2025 inbound callbacks...")
    ib_2025 = _normalize(get_inbound_callbacks_by_number(engine, YEAR_2025_START, YEAR_2025_END, INBOUND_SKILLS_2025, year=2025))
    ib_2025 = ib_2025.rename(columns={"inbound_calls": "inbound_callbacks_2025"})

    result = (
        result
        .merge(ib_2026, on="member_phone", how="left")
        .merge(ib_2025, on="member_phone", how="left")
    )
    result["inbound_callbacks_2026"] = result["inbound_callbacks_2026"].fillna(0).astype(int)
    result["inbound_callbacks_2025"] = result["inbound_callbacks_2025"].fillna(0).astype(int)
    result["ever_called_back"] = (result["inbound_callbacks_2026"] + result["inbound_callbacks_2025"]) > 0

    # --- final columns and sort ---
    result["total_attempts_combined"] = result["attempts_2026"] + result["attempts_2025"]

    result = (
        result[[
            "member_phone",
            "attempts_2026", "pickups_2026", "pickup_rate_2026",
            "attempts_2025", "pickups_2025", "pickup_rate_2025",
            "total_attempts_combined",
            "inbound_callbacks_2026", "inbound_callbacks_2025",
            "ever_called_back",
        ]]
        .sort_values(["ever_called_back", "total_attempts_combined"], ascending=[True, False])
        .reset_index(drop=True)
    )

    today = now.strftime("%Y-%m-%d")
    output_path = config["paths"]["optimization"] / f"never_answered_{today}.csv"
    _save(result, output_path)
    logger.info(f"Saved {len(result):,} rows to {output_path}")
