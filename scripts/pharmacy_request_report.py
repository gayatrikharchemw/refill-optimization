"""
Usage:
    uv run scripts/pharmacy_request_report.py config/humana_refill_etl_prod_config.yml
    uv run scripts/pharmacy_request_report.py config/humana_refill_etl_prod_config.yml --date 2026-03-20
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

    filtered = df[df["Pharmacy Request Completion Date"] >= pd.Timestamp(2026, 3, 1)].copy()
    filtered["Is AI"] = filtered["Pharmacy Request Completion By Email"].str.strip().str.lower() == AI_EMAIL.lower()

    result_col = "Pharmacy Request Result"
    filtered[result_col] = filtered[result_col].fillna("").str.strip()
    result_values = sorted(filtered[result_col].unique())

    ai_df = filtered[filtered["Is AI"]]
    agent_df = filtered[~filtered["Is AI"]]

    ai_total = len(ai_df)
    agent_total = len(agent_df)
    grand_total = len(filtered)

    rows = []
    for val in result_values:
        label = val if val else "(Blank)"
        count = (filtered[result_col] == val).sum()
        ai_count = (ai_df[result_col] == val).sum()
        agent_count = (agent_df[result_col] == val).sum()
        rows.append({
            "Row Labels": label,
            "Count of dispo": count,
            "Total percentage": f"{round(count / grand_total * 100, 1)}%" if grand_total > 0 else "0%",
            "AI": ai_count,
            "AI %": f"{round(ai_count / ai_total * 100, 1)}%" if ai_total > 0 else "0%",
            "Agent": agent_count,
            "Agent %": f"{round(agent_count / agent_total * 100, 1)}%" if agent_total > 0 else "0%",
        })

    rows.append({
        "Row Labels": "Grand Total",
        "Count of dispo": grand_total,
        "Total percentage": "",
        "AI": ai_total,
        "AI %": "",
        "Agent": agent_total,
        "Agent %": "",
    })

    return pd.DataFrame(rows)


def build_clerk_ytd_report(df: pd.DataFrame, start_date: pd.Timestamp, end_date: pd.Timestamp) -> pd.DataFrame:
    df = df.copy()

    df["Pharmacy Request Completion Date"] = pd.to_datetime(
        df["Pharmacy Request Completion Date"], errors="coerce"
    )

    clerks = df[
        (df["Pharmacy Request Completion Date"] >= start_date) &
        (df["Pharmacy Request Completion Date"] <= end_date) &
        (df["Pharmacy Request Completion By Email"].str.strip().str.lower() != AI_EMAIL.lower())
    ].copy()

    result_col = "Pharmacy Request Result"
    clerks[result_col] = clerks[result_col].fillna("").str.strip()
    result_values = sorted(clerks[result_col].unique())

    rows = []
    for email, group in clerks.groupby("Pharmacy Request Completion By Email"):
        total = len(group)
        row = {
            "Clerk Email": email,
            "Total Completed": total,
        }
        for val in result_values:
            count = (group[result_col] == val).sum()
            label = val if val else "(Blank)"
            row[label] = count
            row[f"% {label}"] = round(count / total * 100, 1) if total > 0 else 0
        rows.append(row)

    return pd.DataFrame(rows).sort_values("Total Completed", ascending=False).reset_index(drop=True)


def main(config: HumanaRefillConfig, date: str = None):
    now = get_current_pst_time()

    if date:
        target = pd.Timestamp(date)
    else:
        target = (now - pd.Timedelta(days=1)).normalize()

    clerk_start = target.replace(hour=0, minute=0, second=0, microsecond=0)
    clerk_end = target.replace(hour=23, minute=59, second=59, microsecond=999999)

    ytd_start = now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)

    logger.info(f"Downloading Refill Pharmacy Request Report YTD...")
    pharmacy_report = daily.download_cmapp_report(
        cmapp_client=config.get_cmapp_client(),
        report_category="Refill Pharmacy Request Report",
        account="Humana - Refill Reminder",
        start_date=ytd_start,
        end_date=now,
    )
    logger.info(f"  Downloaded {len(pharmacy_report):,} rows")

    report = build_pharmacy_request_report(pharmacy_report)
    clerk_report = build_clerk_ytd_report(pharmacy_report, clerk_start, clerk_end)

    today = now.strftime("%Y-%m-%d")
    clerk_date_label = target.strftime("%Y-%m-%d")
    reports_dir = config["paths"]["reports"] / today
    save_df_to_csv(report, reports_dir / f"pharmacy_request_march_summary_{today}.csv")
    save_df_to_csv(clerk_report, reports_dir / f"pharmacy_request_clerk_{clerk_date_label}.csv")
    logger.info(f"Saved reports to {reports_dir}")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] - %(name)s: %(message)s"
    )

    parser = argparse.ArgumentParser()
    parser.add_argument("config_path", help="Path to config file")
    parser.add_argument("--date", default=None, help="Date for clerk report (YYYY-MM-DD). Defaults to yesterday.")
    args = parser.parse_args()

    main(HumanaRefillConfig(config_file=Path(args.config_path)), date=args.date)
