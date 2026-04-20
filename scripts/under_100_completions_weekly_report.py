"""
Weekly report of agents with fewer than 100 completed calls.

Usage:
    uv run scripts/under_100_completions_weekly_report.py config/humana_refill_etl_prod_config.yml
    uv run scripts/under_100_completions_weekly_report.py config/humana_refill_etl_prod_config.yml --date 2026-04-14
"""
import argparse
import datetime
import logging
from pathlib import Path

from utils.config_utils import HumanaRefillConfig
from utils.date_util import get_current_pst_time
from utils.file_utils import save_df_to_csv
from utils.email_utils import send_under_100_completions_alert
from utils.reporting import daily
from utils.reporting.refill_summary import build_under_100_report


logger = logging.getLogger(__name__)


def main(config: HumanaRefillConfig, as_of_date: datetime.date = None):
    now = get_current_pst_time()
    if as_of_date is None:
        as_of_date = now.date()

    ytd_start = now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)

    logger.info("Downloading Humana Refill Report YTD...")
    refill_report = daily.download_cmapp_report(
        cmapp_client=config.get_cmapp_client(),
        report_category="Humana Refill Report",
        account="Humana - Refill Reminder",
        start_date=ytd_start,
        end_date=now,
    )
    logger.info(f"  Downloaded {len(refill_report):,} rows")

    report = build_under_100_report(refill_report, as_of_date=as_of_date)
    logger.info(f"  {len(report)} agents with fewer than 100 completions this week")

    week_start = as_of_date - datetime.timedelta(days=as_of_date.weekday())
    week_str = week_start.strftime("%Y-%m-%d")
    today = now.strftime("%Y-%m-%d")
    reports_dir = config["paths"]["reports"] / today
    output_path = reports_dir / f"under_100_completions_week_{week_str}.csv"
    save_df_to_csv(report, output_path)
    logger.info(f"Saved report to {output_path}")

    week_end = week_start + datetime.timedelta(days=6)
    date_range = f"{week_start.strftime('%m/%d/%Y')} - {week_end.strftime('%m/%d/%Y')}"
    send_under_100_completions_alert(report, config.config, date_range=date_range)
    logger.info("Sent under-100 completions email")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] - %(name)s: %(message)s"
    )

    parser = argparse.ArgumentParser(description="Weekly report of agents with < 100 completed calls.")
    parser.add_argument("config_path", help="Path to config file")
    parser.add_argument("--date", default=None, help="As-of date (YYYY-MM-DD). Defaults to today (PST).")
    args = parser.parse_args()

    as_of_date = datetime.date.fromisoformat(args.date) if args.date else None

    main(HumanaRefillConfig(config_file=Path(args.config_path)), as_of_date=as_of_date)
