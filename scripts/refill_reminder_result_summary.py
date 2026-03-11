import argparse
import logging
from pathlib import Path

from utils.config_utils import HumanaRefillConfig
from utils.date_util import get_current_pst_time
from utils.file_utils import save_df_to_csv
from utils.reporting import daily
from utils.reporting.refill_summary import build_agent_report, build_reports


logger = logging.getLogger(__name__)


def main(config: HumanaRefillConfig):
    now = get_current_pst_time()
    ytd_start = now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)

    refill_report = daily.download_cmapp_report(
        cmapp_client=config.get_cmapp_client(),
        report_category="Humana Refill Report",
        account="Humana - Refill Reminder",
        start_date=ytd_start,
        end_date=now,
    )

    result_summary_df, decline_reason_counts, submission_result_counts = build_reports(refill_report)
    agent_report = build_agent_report(refill_report)

    today = now.strftime("%Y-%m-%d")
    reports_dir = config["paths"]["reports"] / today

    save_df_to_csv(result_summary_df, reports_dir / f"ytd_refill_result_summary_{today}.csv")
    save_df_to_csv(decline_reason_counts, reports_dir / f"ytd_decline_reason_counts_{today}.csv")
    save_df_to_csv(submission_result_counts, reports_dir / f"ytd_refill_submission_result_counts_{today}.csv")
    save_df_to_csv(agent_report, reports_dir / f"ytd_agent_refill_submission_rate_{today}.csv")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] - %(name)s: %(message)s"
    )

    parser = argparse.ArgumentParser()
    parser.add_argument("config_path", help="Path to config file")
    args = parser.parse_args()

    main(HumanaRefillConfig(config_file=Path(args.config_path)))
