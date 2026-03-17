"""
Usage:
    uv run scripts/pharmacy_request_report.py config/humana_refill_etl_prod_config.yml
"""
import argparse
import logging
from pathlib import Path

import pandas as pd

from utils.config_utils import HumanaRefillConfig
from utils.date_util import get_current_pst_time
from utils.file_utils import save_df_to_csv
from utils.reporting import daily


logger = logging.getLogger(__name__)

AI_EMAIL = "humana_noncmr_clinician@medwatchers.com"


def build_pharmacy_request_report(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["Pharmacy Request Completion Date"] = pd.to_datetime(
        df["Pharmacy Request Completion Date"], errors="coerce"
    )

    march = df[df["Pharmacy Request Completion Date"] >= pd.Timestamp(2026, 3, 1)].copy()

    march["Completion By"] = march["Pharmacy Request Completion By Email"].apply(
        lambda x: "AI Calls" if str(x).strip().lower() == AI_EMAIL.lower() else "Clerks"
    )

    result_col = "Pharmacy Request Result"
    march[result_col] = march[result_col].fillna("").str.strip()

    result_values = sorted(march[result_col].unique())

    rows = []
    for label in ["AI Calls", "Clerks"]:
        group = march[march["Completion By"] == label]
        total = len(group)
        row = {"Completion By": label, "Total Completed (Since March 1, 2026)": total}
        for val in result_values:
            row[val if val else "(Blank)"] = (group[result_col] == val).sum()
        rows.append(row)

    return pd.DataFrame(rows)


def build_clerk_weekly_report(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["Pharmacy Request Completion Date"] = pd.to_datetime(
        df["Pharmacy Request Completion Date"], errors="coerce"
    )

    clerks = df[
        (df["Pharmacy Request Completion Date"] >= pd.Timestamp(2026, 3, 1)) &
        (df["Pharmacy Request Completion By Email"].str.strip().str.lower() != AI_EMAIL.lower())
    ].copy()

    clerks["Week"] = clerks["Pharmacy Request Completion Date"].dt.to_period("W").dt.start_time.dt.date
    clerks["Date Range"] = clerks["Week"].apply(
        lambda w: f"{w.strftime('%m/%d/%Y')} - {(w + pd.Timedelta(days=6)).strftime('%m/%d/%Y')}"
    )

    result_col = "Pharmacy Request Result"
    clerks[result_col] = clerks[result_col].fillna("").str.strip()
    result_values = sorted(clerks[result_col].unique())

    rows = []
    for (email, week), group in clerks.groupby(["Pharmacy Request Completion By Email", "Week"]):
        date_range = group["Date Range"].iloc[0]
        row = {
            "Clerk Email": email,
            "Date Range": date_range,
            "Total Completed": len(group),
        }
        for val in result_values:
            row[val if val else "(Blank)"] = (group[result_col] == val).sum()
        rows.append(row)

    return pd.DataFrame(rows).sort_values(["Clerk Email", "Date Range"]).reset_index(drop=True)


def main(config: HumanaRefillConfig):
    now = get_current_pst_time()
    ytd_start = now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)

    logger.info("Downloading Refill Pharmacy Request Report YTD...")
    pharmacy_report = daily.download_cmapp_report(
        cmapp_client=config.get_cmapp_client(),
        report_category="Refill Pharmacy Request Report",
        account="Humana - Refill Reminder",
        start_date=ytd_start,
        end_date=now,
    )
    logger.info(f"  Downloaded {len(pharmacy_report):,} rows")

    report = build_pharmacy_request_report(pharmacy_report)
    clerk_weekly_report = build_clerk_weekly_report(pharmacy_report)

    today = now.strftime("%Y-%m-%d")
    reports_dir = config["paths"]["reports"] / today
    save_df_to_csv(report, reports_dir / f"pharmacy_request_march_summary_{today}.csv")
    save_df_to_csv(clerk_weekly_report, reports_dir / f"pharmacy_request_clerk_weekly_{today}.csv")
    logger.info(f"Saved reports to {reports_dir}")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] - %(name)s: %(message)s"
    )

    parser = argparse.ArgumentParser()
    parser.add_argument("config_path", help="Path to config file")
    args = parser.parse_args()

    main(HumanaRefillConfig(config_file=Path(args.config_path)))
