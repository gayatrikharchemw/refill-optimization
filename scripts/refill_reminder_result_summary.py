"""
Usage:
    uv run scripts/refill_reminder_result_summary.py config/humana_refill_etl_prod_config.yml
    uv run scripts/refill_reminder_result_summary.py config/humana_refill_etl_prod_config.yml --etl-dir s3://humana-prod-data/2026/Refill/daily_refill_etl/
"""
import argparse
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


def main(config: HumanaRefillConfig, etl_dir: str = None):
    now = get_current_pst_time()
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
    agent_report = build_agent_report(refill_report)

    today = now.strftime("%Y-%m-%d")
    reports_dir = config["paths"]["reports"] / today

    save_df_to_csv(result_summary_df, reports_dir / f"ytd_refill_result_summary_{today}.csv")
    save_df_to_csv(decline_reason_counts, reports_dir / f"ytd_decline_reason_counts_{today}.csv")
    save_df_to_csv(submission_result_counts, reports_dir / f"ytd_refill_submission_result_counts_{today}.csv")
    save_df_to_csv(agent_report, reports_dir / f"ytd_agent_refill_submission_rate_{today}.csv")
    breakpoint()
    send_metric_alerts(submission_result_counts, result_summary_df, decline_reason_counts, config.config)
    send_agent_performance_alert(agent_report, config.config)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] - %(name)s: %(message)s"
    )

    parser = argparse.ArgumentParser()
    parser.add_argument("config_path", help="Path to config file")
    parser.add_argument("--etl-dir", default=None, help="Base ETL dir (local or s3://) to scan for transformed_claims files")
    args = parser.parse_args()

    main(HumanaRefillConfig(config_file=Path(args.config_path)), etl_dir=args.etl_dir)
