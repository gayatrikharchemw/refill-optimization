"""
Find phone numbers that are consistently not answering outbound calls, and check
whether they called us back on the inbound skill.

By default, only includes numbers seen in both 2025 and 2026 (inner join).
Use --2026-only to report on all 2026 numbers regardless of 2025 history.

Saved to paths.optimization in the config.

Usage:
    uv run scripts/get_never_answered.py config/humana_refill_etl_prod_config.yml
    uv run scripts/get_never_answered.py config/humana_refill_etl_prod_config.yml --min-attempts 5
    uv run scripts/get_never_answered.py config/humana_refill_etl_prod_config.yml --2026-only
    uv run scripts/get_never_answered.py config/humana_refill_etl_prod_config.yml --2026-only --min-attempts 5
"""

import argparse
import logging
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from utils.config_utils import HumanaRefillConfig
from utils.file_utils import clean_ani
from utils.nice_utils import get_inbound_callbacks_by_number, get_outbound_stats_both_years, get_outbound_stats_by_number

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
    parser.add_argument("--2026-only", dest="only_2026", action="store_true", help="Report on 2026 numbers only, without requiring 2025 history")
    parser.add_argument("--never-answered", dest="never_answered_only", action="store_true", help="Only include numbers with 0 pickups in 2026 (answered_calls = 0)")
    parser.add_argument("--date", type=str, default=None, help="Treat this date as 'today' (YYYY-MM-DD). Defaults to now.")
    args = parser.parse_args()

    config = HumanaRefillConfig(config_file=args.config_path)
    engine = config.get_outreach_db_engine()
    if args.date:
        parsed = datetime.strptime(args.date, "%Y-%m-%d")
        now = datetime(parsed.year, parsed.month, parsed.day, 23, 59, 59, tzinfo=ZoneInfo("UTC"))
    else:
        now = datetime.now(tz=ZoneInfo("UTC"))

    if args.only_2026:
        # --- 2026 only ---
        logger.info("Fetching 2026 outbound stats...")
        result = _normalize(get_outbound_stats_by_number(
            engine, YEAR_2026_START, now, OUTBOUND_SKILLS_2026, year=2026,
            min_attempts=args.min_attempts, never_answered_only=args.never_answered_only,
        ))
        result = result.rename(columns={
            "total_calls": "attempts_2026",
            "attempt_days": "attempt_days_2026",
            "answered_calls": "pickups_2026",
            "pickup_rate": "pickup_rate_2026",
        })
        logger.info(f"  {len(result):,} numbers with >= {args.min_attempts} attempts in 2026")

        matched_numbers = result["member_phone"].tolist()

        logger.info("Fetching 2026 inbound callbacks...")
        ib_2026 = _normalize(get_inbound_callbacks_by_number(
            engine, YEAR_2026_START, now, INBOUND_SKILLS_2026, year=2026, numbers=matched_numbers,
        ))
        ib_2026 = ib_2026.rename(columns={"inbound_calls": "inbound_callbacks_2026"})

        result = result.merge(ib_2026, on="member_phone", how="left")
        result["inbound_callbacks_2026"] = result["inbound_callbacks_2026"].fillna(0).astype(int)
        result["ever_called_back"] = result["inbound_callbacks_2026"] > 0

        result = (
            result[[
                "member_phone",
                "attempts_2026", "attempt_days_2026", "pickups_2026", "pickup_rate_2026",
                "inbound_callbacks_2026",
                "ever_called_back",
            ]]
            .sort_values(["ever_called_back", "attempts_2026"], ascending=[True, False])
            .reset_index(drop=True)
        )
        filename = f"never_answered_2026_only_{now.strftime('%Y-%m-%d')}.csv"

    else:
        # --- both years (inner join) ---
        logger.info("Fetching outbound stats for numbers present in both 2025 and 2026...")
        result = _normalize(get_outbound_stats_both_years(
            engine,
            start_2026=YEAR_2026_START, end_2026=now,
            start_2025=YEAR_2025_START, end_2025=YEAR_2025_END,
            skills_2026=OUTBOUND_SKILLS_2026,
            skills_2025=OUTBOUND_SKILLS_2025,
            min_attempts=args.min_attempts,
            never_answered_only=args.never_answered_only,
        ))
        logger.info(f"  {len(result):,} numbers with >= {args.min_attempts} attempts in 2026 and seen in 2025")

        matched_numbers = result["member_phone"].tolist()

        logger.info("Fetching 2026 inbound callbacks...")
        ib_2026 = _normalize(get_inbound_callbacks_by_number(
            engine, YEAR_2026_START, now, INBOUND_SKILLS_2026, year=2026, numbers=matched_numbers,
        ))
        ib_2026 = ib_2026.rename(columns={"inbound_calls": "inbound_callbacks_2026"})

        logger.info("Fetching 2025 inbound callbacks...")
        ib_2025 = _normalize(get_inbound_callbacks_by_number(
            engine, YEAR_2025_START, YEAR_2025_END, INBOUND_SKILLS_2025, year=2025, numbers=matched_numbers,
        ))
        ib_2025 = ib_2025.rename(columns={"inbound_calls": "inbound_callbacks_2025"})

        result = (
            result
            .merge(ib_2026, on="member_phone", how="left")
            .merge(ib_2025, on="member_phone", how="left")
        )
        result["inbound_callbacks_2026"] = result["inbound_callbacks_2026"].fillna(0).astype(int)
        result["inbound_callbacks_2025"] = result["inbound_callbacks_2025"].fillna(0).astype(int)
        result["ever_called_back"] = (result["inbound_callbacks_2026"] + result["inbound_callbacks_2025"]) > 0
        result["total_attempts_combined"] = result["attempts_2026"] + result["attempts_2025"]

        result = (
            result[[
                "member_phone",
                "attempts_2026", "attempt_days_2026", "pickups_2026", "pickup_rate_2026",
                "attempts_2025", "attempt_days_2025", "pickups_2025", "pickup_rate_2025",
                "total_attempts_combined",
                "inbound_callbacks_2026", "inbound_callbacks_2025",
                "ever_called_back",
            ]]
            .sort_values(["ever_called_back", "total_attempts_combined"], ascending=[True, False])
            .reset_index(drop=True)
        )
        filename = f"never_answered_both_years_{now.strftime('%Y-%m-%d')}.csv"

    output_path = config["paths"]["optimization"] / filename
    _save(result, output_path)
    logger.info(f"Saved {len(result):,} rows to {output_path}")
