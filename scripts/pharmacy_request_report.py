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


def _pharmacy_summary_rows(filtered: pd.DataFrame, result_values: list) -> list:
    """Build summary rows (overall or per-week) for AI vs agent comparison."""
    result_col = "Pharmacy Request Result"
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
            "Total %": f"{round(count / grand_total * 100, 1)}%" if grand_total > 0 else "0%",
            "AI": ai_count,
            "AI %": f"{round(ai_count / ai_total * 100, 1)}%" if ai_total > 0 else "0%",
            "Agent": agent_count,
            "Agent %": f"{round(agent_count / agent_total * 100, 1)}%" if agent_total > 0 else "0%",
        })
    rows.append({
        "Row Labels": "Grand Total",
        "Count of dispo": grand_total,
        "Total %": "",
        "AI": ai_total,
        "AI %": "",
        "Agent": agent_total,
        "Agent %": "",
    })
    return rows


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

    return pd.DataFrame(_pharmacy_summary_rows(filtered, result_values))


def build_pharmacy_request_weekly_report(df: pd.DataFrame) -> pd.DataFrame:
    """Week-by-week AI vs agent breakdown for pharmacy requests since March 1, 2026."""
    df = df.copy()

    df["Pharmacy Request Completion Date"] = pd.to_datetime(
        df["Pharmacy Request Completion Date"], errors="coerce"
    )

    filtered = df[df["Pharmacy Request Completion Date"] >= pd.Timestamp(2026, 3, 1)].copy()
    filtered["Is AI"] = filtered["Pharmacy Request Completion By Email"].str.strip().str.lower() == AI_EMAIL.lower()

    result_col = "Pharmacy Request Result"
    filtered[result_col] = filtered[result_col].fillna("").str.strip()
    result_values = sorted(filtered[result_col].unique())

    filtered["Week Start"] = (
        filtered["Pharmacy Request Completion Date"]
        .dt.to_period("W")
        .dt.start_time
        .dt.date
    )
    filtered["Week"] = filtered["Week Start"].apply(
        lambda w: f"{w.strftime('%m/%d/%Y')} - {(w + pd.Timedelta(days=6)).strftime('%m/%d/%Y')}"
    )

    all_rows = []
    for week_label, week_df in filtered.groupby("Week", sort=True):
        for row in _pharmacy_summary_rows(week_df, result_values):
            all_rows.append({"Week": week_label, **row})

    return pd.DataFrame(all_rows)


def build_clerk_daily_report(df: pd.DataFrame, target_date: pd.Timestamp) -> pd.DataFrame:
    df = df.copy()

    df["Pharmacy Request Completion Date"] = pd.to_datetime(
        df["Pharmacy Request Completion Date"], errors="coerce"
    )

    clerks = df[
        (df["Pharmacy Request Completion Date"].dt.normalize() == target_date) &
        (df["Pharmacy Request Completion By Email"].str.strip().str.lower() != AI_EMAIL.lower())
    ].copy()

    result_col = "Pharmacy Request Result"
    clerks[result_col] = clerks[result_col].fillna("").str.strip()
    result_values = sorted(clerks[result_col].unique())

    REFILL_SUBMITTED = "Refill Submitted"
    OUT_OF_REFILL = "Out of Refill - Refill Request"
    COMBINED_COL = "Refill Submitted + Out of Refill"

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

        combined_count = (group[result_col].isin([REFILL_SUBMITTED, OUT_OF_REFILL])).sum()
        row[COMBINED_COL] = combined_count
        rows.append(row)

    result_df = pd.DataFrame(rows)
    if result_df.empty or COMBINED_COL not in result_df.columns:
        return result_df
    return result_df.sort_values(COMBINED_COL, ascending=False).reset_index(drop=True)


def main(config: HumanaRefillConfig, date: str = None):
    now = get_current_pst_time()

    if date:
        target = pd.Timestamp(date)
    else:
        yesterday = now - pd.Timedelta(days=1)
        target = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

    clerk_date = pd.Timestamp(target.date())

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
    weekly_report = build_pharmacy_request_weekly_report(pharmacy_report)
    clerk_report = build_clerk_daily_report(pharmacy_report, clerk_date)

    today = now.strftime("%Y-%m-%d")
    clerk_date_label = target.strftime("%Y-%m-%d")
    reports_dir = config["paths"]["reports"] / today
    save_df_to_csv(report, reports_dir / f"pharmacy_request_march_summary_{today}.csv")
    save_df_to_csv(weekly_report, reports_dir / f"pharmacy_request_weekly_ai_vs_agent_{today}.csv")
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
