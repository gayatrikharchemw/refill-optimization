"""
Usage:
    uv run scripts/refill_reminder_result_summary.py config/humana_refill_etl_prod_config.yml
    uv run scripts/refill_reminder_result_summary.py config/humana_refill_etl_prod_config.yml --etl-dir s3://humana-prod-data/2026/Refill/daily_refill_etl/
    uv run scripts/refill_reminder_result_summary.py config/humana_refill_etl_prod_config.yml --date 2026-03-28 --period weekly
    uv run scripts/refill_reminder_result_summary.py config/humana_refill_etl_prod_config.yml --period monthly
"""
import argparse
import datetime
import logging
from pathlib import Path

from s3path import S3Path

from utils.config_utils import HumanaRefillConfig
from utils.date_util import get_current_pst_time
from utils.file_utils import save_df_to_csv
from utils.reporting import daily
from utils.email_utils import send_agent_performance_alert, send_metric_alerts
from utils.reporting.refill_summary import build_agent_report, build_reports


logger = logging.getLogger(__name__)


def _resolve_path(path_str: str):
    if path_str.startswith("s3://"):
        return S3Path("/" + path_str[5:])
    return Path(path_str)


def main(config: HumanaRefillConfig, etl_dir: str = None, as_of_date: datetime.date = None, period: str = "weekly"):
    now = get_current_pst_time()
    if as_of_date is None:
        as_of_date = now.date()
    ytd_start = now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)

    refill_report = daily.download_cmapp_report(
        cmapp_client=config.get_cmapp_client(),
        report_category="Humana Refill Report",
        account="Humana - Refill Reminder",
        start_date=ytd_start,
        end_date=now,
    )

    transformed_claims_files = []
    base_dir = _resolve_path(etl_dir) if etl_dir else config["paths"].get("etl_dir")
    if base_dir:
        logger.info(f"Scanning {base_dir} for transformed_claims files...")
        transformed_claims_files = list(base_dir.glob("**/*_transformed_claims.csv"))
        logger.info(f"  Found {len(transformed_claims_files)} files")

    result_summary_df, decline_reason_counts, submission_result_counts = build_reports(refill_report, transformed_claims_files)
    agent_report = build_agent_report(refill_report, as_of_date=as_of_date, period=period)

    today = now.strftime("%Y-%m-%d")
    reports_dir = config["paths"]["reports"] / today
    agent_date_str = as_of_date.strftime("%Y-%m-%d")

    save_df_to_csv(result_summary_df, reports_dir / f"ytd_refill_result_summary_{today}.csv")
    save_df_to_csv(decline_reason_counts, reports_dir / f"ytd_decline_reason_counts_{today}.csv")
    save_df_to_csv(submission_result_counts, reports_dir / f"ytd_refill_submission_result_counts_{today}.csv")
    save_df_to_csv(agent_report, reports_dir / f"{period}_agent_refill_submission_rate_{agent_date_str}.csv")

    send_metric_alerts(submission_result_counts, result_summary_df, decline_reason_counts, config.config, as_of_date=as_of_date)
    if period == "daily":
        agent_date_range = as_of_date.strftime("%m/%d/%Y")
    elif period == "weekly":
        week_start = as_of_date - datetime.timedelta(days=as_of_date.weekday())
        week_end = week_start + datetime.timedelta(days=6)
        agent_date_range = f"{week_start.strftime('%m/%d/%Y')} - {week_end.strftime('%m/%d/%Y')}"
    else:  # monthly
        agent_date_range = as_of_date.strftime("%B %Y")

    send_agent_performance_alert(agent_report, config.config, date_range=agent_date_range)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] - %(name)s: %(message)s"
    )

    parser = argparse.ArgumentParser()
    parser.add_argument("config_path", help="Path to config file")
    parser.add_argument("--etl-dir", default="s3://humana-prod-data/2026/Refill/daily_refill_etl/", help="Base ETL dir (local or s3://) to scan for transformed_claims files")
    parser.add_argument("--date", default=None, help="As-of date for the agent report (YYYY-MM-DD). Defaults to today (PST).")
    parser.add_argument("--period", default="weekly", choices=["daily", "weekly", "monthly"], help="Time window for the agent performance report (default: weekly)")
    args = parser.parse_args()

    as_of_date = datetime.date.fromisoformat(args.date) if args.date else None

    main(
        HumanaRefillConfig(config_file=Path(args.config_path)),
        etl_dir=args.etl_dir,
        as_of_date=as_of_date,
        period=args.period,
    )
