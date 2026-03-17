"""
Generates YTD member-level counts from daily_refill_etl S3 folder for Sankey diagram.

Globs all *_initial_scrub_file.csv and *_original_refill_data_scrubbed.csv files
across date subfolders. Counts at MEMBER level (unique idcard_mbr_id).

Usage:
    python scripts/generate_ytd_sankey_counts.py \
        --etl-dir s3://humana-prod-data/2026/Refill/daily_refill_etl/ \
        --config config/humana_refill_etl_prod_config.yml

Output: Prints Mermaid sankey-beta diagram with real YTD counts.
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytz
from s3path import S3Path

from humana_refill_etl.config_utils import HumanaRefillConfig
from humana_refill_etl.reporting import daily


# Disposition groupings from fields.DISPOSITION_MAP
REACHED_DISPOS = {
    "Refill Submitted",
    "Reminded - Refilled on own",
    "Provider Changed Dose",
    "Refill Request",
    "Pharmacy Change",
    "Already Ordered Refill",
    "Member Declined",
    "Member Does Not Want Refill",
    "Member No Longer With Plan",
    "Provider Discontinued Medicine",
    "Member Stopped Medication",
    "Provider Changed Medication",
    "Member Deceased",
}

INTERMEDIATE_DISPOS = {
    "No Answer",
    "Left Voicemail",
    "Answering Machine",
    "Member Requests Call Back",
    "Member Unavailable",
}

FAILED_DISPOS = {
    "Operator Intercept",
    "Caller Disconnected",
    "Hardware Timeout",
    "Busy",
    "Null",
    "Others-Misc",
    "Call Queued For later",
    "Invalid Number",
    "Wrong Number",
    "Member Requests Do Not Call",
}

# CMAPP report disposition sub-groups for Reached members
POSITIVE_OUTCOME_DISPOS = {
    "Refill Submitted",
    "Reminded - Refilled on own",
    "Already Ordered Refill",
    "Refill Request",
    "Pharmacy Change",
}

MEMBER_CLOSED_DISPOS = {
    "Member Declined",
    "Member Does Not Want Refill",
    "Member Stopped Medication",
    "Member No Longer With Plan",
    "Provider Discontinued Medicine",
    "Provider Changed Dose",
    "Provider Changed Medication",
    "Member Deceased",
}


def resolve_path(path_str: str):
    """
    s3path expects the format S3Path("/bucket/key/path/")
    e.g. s3://humana-prod-data/2026/Refill/daily_refill_etl/
      -> S3Path("/humana-prod-data/2026/Refill/daily_refill_etl/")
    """
    if path_str.startswith("s3://"):
        # Strip scheme, keep /bucket/key/... format that s3path expects
        return S3Path("/" + path_str[5:])
    return Path(path_str)


def glob_all_files_with_suffix(base_dir, suffix: str) -> list:
    """Globs all files matching *{suffix} across all date subfolders."""
    print(f"  Scanning {base_dir} for *{suffix} ...", file=sys.stderr)
    return list(base_dir.glob(f"**/*{suffix}"))


def read_csv_from_path(path) -> pd.DataFrame:
    if isinstance(path, S3Path):
        import io
        return pd.read_csv(io.BytesIO(path.read_bytes()), dtype="string")
    return pd.read_csv(path, dtype="string")


def load_and_concat(paths: list) -> pd.DataFrame:
    dfs = []
    for p in paths:
        try:
            df = read_csv_from_path(p)
            dfs.append(df)
        except Exception as e:
            print(f"  Warning: could not read {p}: {e}", file=sys.stderr)
    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)


def get_scrub_counts(initial_scrub_files: list) -> dict:
    """Returns member-level counts from initial_scrub_file. De-dupes by idcard_mbr_id per day, then sums."""
    if not initial_scrub_files:
        return {"total_scrubbed": 0, "by_reason": {}}

    reason_priority = {
        "Do not call": 1,
        "Missing data": 2,
        "Invalid number": 3,
        "Max attempt reached": 4,
    }

    from collections import defaultdict
    days: dict = defaultdict(list)
    for path in initial_scrub_files:
        days[path.parent.name].append(path)

    total = 0
    by_reason: dict = {}
    for day_files in days.values():
        df = load_and_concat(day_files)
        if df.empty or "idcard_mbr_id" not in df.columns:
            continue
        df["priority"] = df["scrub_reason"].map(reason_priority).fillna(99)
        deduped = df.sort_values("priority").drop_duplicates(subset="idcard_mbr_id", keep="first")
        total += len(deduped)
        for reason, count in deduped["scrub_reason"].value_counts().items():
            by_reason[reason] = by_reason.get(reason, 0) + count

    return {"total_scrubbed": total, "by_reason": by_reason}


def get_transformed_claims_count(transformed_claims_files: list) -> int:
    """Counts unique Document Keys per day then sums across days."""
    if not transformed_claims_files:
        return 0

    from collections import defaultdict
    days: dict = defaultdict(list)
    for path in transformed_claims_files:
        days[path.parent.name].append(path)

    total = 0
    for day_files in days.values():
        df = load_and_concat(day_files)
        if df.empty:
            continue
        if "Document Key" in df.columns:
            total += df["Document Key"].nunique()
        else:
            print(
                "  Warning: no Document Key column found in transformed_claims files, using row count",
                file=sys.stderr,
            )
            total += len(df)

    return total


def get_eligible_member_count(scrubbed_original_files: list) -> int:
    """Counts unique members per day then sums across days."""
    if not scrubbed_original_files:
        return 0

    from collections import defaultdict
    days: dict = defaultdict(list)
    for path in scrubbed_original_files:
        days[path.parent.name].append(path)

    total = 0
    for day_files in days.values():
        df = load_and_concat(day_files)
        if df.empty:
            continue
        if "idcard_mbr_id" in df.columns:
            total += df["idcard_mbr_id"].nunique()
        elif "RecipientMemberCardId" in df.columns:
            total += df["RecipientMemberCardId"].nunique()
        else:
            print(
                "  Warning: no member ID column found in scrubbed original files, using row count",
                file=sys.stderr,
            )
            total += len(df)

    return total


def get_disposition_counts(df: pd.DataFrame) -> dict | None:
    """
    Parses CMAPP report DataFrame for YTD disposition counts at member level.

    Expected columns (from CMAPP 'Humana Refill Reminder Report'):
      - Member ID, Disposition, Refill Reminder Result, Refill Submission Result, Denial Reason

    Returns member-level disposition summary or None if df is empty.
    """
    if df is None or df.empty:
        return None

    df = df.fillna("").astype(str)

    case_completed_count = None
    case_not_completed_count = None

    # Determine format
    if "call_status_1" in df.columns and "idcard_mbr_id" in df.columns:
        # Refill team file format - already pivoted, one row per member contact
        member_id_col = "idcard_mbr_id"
        dispo_cols = ["call_status_1", "call_status_2", "call_status_3"]

        # Get best disposition per member (Reached > Intermediate > Failed)
        def best_dispo(row):
            statuses = [row[c] for c in dispo_cols if row[c] and row[c].strip()]
            for s in statuses:
                if s in REACHED_DISPOS:
                    return ("Reached", s)
            for s in statuses:
                if s in INTERMEDIATE_DISPOS:
                    return ("Not Reached", s)
            for s in statuses:
                if s in FAILED_DISPOS:
                    return ("Call Failed", s)
            return ("Unknown", statuses[0] if statuses else "None")

        df[["dispo_group", "specific_dispo"]] = df.apply(
            best_dispo, axis=1, result_type="expand"
        )

    elif "Disposition" in df.columns and "Member ID" in df.columns:
        # CMAPP report format - one row per lead/claim
        member_id_col = "Member ID"

        # Case-level completed/not-completed (before member dedup, preserves multi-row per case)
        case_id_col = next(
            (c for c in ["Case ID", "case_id", "CaseID"] if c in df.columns),
            member_id_col,
        )
        completed_mask = df["Disposition"].str.strip().str.lower() == "completed"
        completed_cases = set(df.loc[completed_mask, case_id_col].dropna().unique())
        all_cases = set(df[case_id_col].dropna().unique())
        case_completed_count = len(completed_cases)
        case_not_completed_count = len(all_cases) - case_completed_count

        # Accepted/Declined from "Refill Reminder Result"; breakdown from "Refill Submission Result"
        refill_accepted_count = None
        refill_declined_count = None
        refill_submission_counts = None
        if "Refill Reminder Result" in df.columns:
            rrr = df["Refill Reminder Result"].str.strip().str.lower()
            refill_accepted_count = int((rrr == "yes").sum())
            refill_declined_count = int((rrr == "no").sum())
        if "Refill Submission Result" in df.columns:
            refill_submission_counts = (
                df["Refill Submission Result"]
                .str.strip()
                .replace("", pd.NA)
                .dropna()
                .value_counts()
                .to_dict()
            )
        denial_reason_counts = None
        if "Denial Reason" in df.columns:
            denial_reason_counts = (
                df["Denial Reason"]
                .str.strip()
                .replace("", pd.NA)
                .dropna()
                .value_counts()
                .to_dict()
            )

        def categorize(d):
            if d in REACHED_DISPOS:
                return "Reached"
            if d in INTERMEDIATE_DISPOS:
                return "Not Reached"
            if d in FAILED_DISPOS:
                return "Call Failed"
            return "Unknown"

        df["dispo_group"] = df["Disposition"].apply(categorize)
        df["specific_dispo"] = df["Disposition"]

        # Best disposition per member
        priority_map = {"Reached": 1, "Not Reached": 2, "Call Failed": 3, "Unknown": 4}
        df["priority"] = df["dispo_group"].map(priority_map)
        df = df.sort_values("priority")
        df = df.drop_duplicates(subset=member_id_col, keep="first")

    else:
        print(
            f"  Warning: unrecognized disposition report format. Columns: {list(df.columns[:10])}",
            file=sys.stderr,
        )
        return None

    total_called = df[member_id_col].nunique()
    group_counts = df.groupby("dispo_group")[member_id_col].nunique().to_dict()
    specific_counts = df.groupby("specific_dispo")[member_id_col].nunique().to_dict()

    return {
        "total_called": total_called,
        "by_group": group_counts,
        "by_specific": specific_counts,
        "case_completed": case_completed_count,
        "case_not_completed": case_not_completed_count,
        "refill_accepted": refill_accepted_count,
        "refill_declined": refill_declined_count,
        "refill_submission_counts": refill_submission_counts,
        "denial_reason_counts": denial_reason_counts,
    }


def build_sankey(
    total_from_humana: int,
    scrub_counts: dict,
    eligible: int,
    disposition_counts: dict | None,
) -> str:
    scrubbed = scrub_counts["total_scrubbed"]
    by_reason = scrub_counts["by_reason"]

    lines = [
        "---",
        "config:",
        "  sankey:",
        "    showValues: true",
        "---",
        "sankey-beta",
        "",
        "%% YTD Humana Refill Concierge Funnel",
        "%% Source,Target,Value",
        "",
        "%% --- SCRUBBING STAGE ---",
        f"Total Members from Humana,Scrubbed Before Outreach,{scrubbed}",
        f"Total Members from Humana,Eligible for Outreach,{eligible}",
    ]

    # Scrub reason breakdown
    if by_reason:
        lines.append("")
        lines.append("%% Scrub reasons")
        for reason, count in sorted(by_reason.items(), key=lambda x: -x[1]):
            safe_reason = reason.replace(",", " /")
            lines.append(f"Scrubbed Before Outreach,{safe_reason},{count}")

    if disposition_counts:
        called = disposition_counts["total_called"]
        not_called = max(0, eligible - called)
        by_group = disposition_counts["by_group"]
        by_specific = disposition_counts["by_specific"]
        case_completed = disposition_counts.get("case_completed")
        case_not_completed = disposition_counts.get("case_not_completed")
        refill_accepted = disposition_counts.get("refill_accepted")
        refill_declined = disposition_counts.get("refill_declined")
        refill_submission_counts = disposition_counts.get("refill_submission_counts")
        denial_reason_counts = disposition_counts.get("denial_reason_counts")

        reached = by_group.get("Reached", 0)
        not_reached = by_group.get("Not Reached", 0)
        call_failed = by_group.get("Call Failed", 0)

        lines += [
            "",
            "%% --- OUTREACH STAGE ---",
            f"Eligible for Outreach,Called,{called}",
            f"Eligible for Outreach,Not Yet Called,{not_called}",
            "",
            "%% Call outcomes",
            f"Called,Reached Member,{reached}",
            f"Called,Not Reached - Voicemail/No Answer,{not_reached}",
            f"Called,Call Failed,{call_failed}",
        ]

        if case_completed is not None and case_not_completed is not None:
            lines += [
                "",
                "%% --- DISPOSITION COMPLETION (case-level) ---",
                f"Eligible for Outreach,Disposition Completed,{case_completed}",
                f"Eligible for Outreach,Disposition Not Completed,{case_not_completed}",
            ]
            if refill_accepted is not None or refill_declined is not None or refill_submission_counts:
                lines.append("")
                lines.append("%% Accepted / Declined")
                if refill_accepted is not None:
                    lines.append(f"Disposition Completed,Accepted,{refill_accepted}")
                if refill_declined is not None:
                    lines.append(f"Disposition Completed,Declined,{refill_declined}")
                if refill_submission_counts:
                    lines.append("")
                    lines.append("%% Refill Submission Result breakdown (from Accepted)")
                    for result, count in sorted(
                        refill_submission_counts.items(), key=lambda x: -x[1]
                    ):
                        safe_result = result.replace(",", " /")
                        lines.append(f"Accepted,{safe_result},{count}")
                if denial_reason_counts:
                    lines.append("")
                    lines.append("%% Denial Reason breakdown (from Declined)")
                    for reason, count in sorted(
                        denial_reason_counts.items(), key=lambda x: -x[1]
                    ):
                        safe_reason = reason.replace(",", " /")
                        lines.append(f"Declined,{safe_reason},{count}")

        # Specific reached dispositions
        positive = {
            k: v for k, v in by_specific.items() if k in POSITIVE_OUTCOME_DISPOS and v > 0
        }
        closed = {
            k: v for k, v in by_specific.items() if k in MEMBER_CLOSED_DISPOS and v > 0
        }

        if positive or closed:
            lines.append("")
            lines.append("%% Reached breakdown")
            for dispo, count in sorted(positive.items(), key=lambda x: -x[1]):
                lines.append(f"Reached Member,{dispo},{count}")
            if closed:
                total_closed = sum(closed.values())
                lines.append(f"Reached Member,Closed - No Action Needed,{total_closed}")
    else:
        lines += [
            "",
            "%% --- OUTREACH STAGE (add after downloading CMAPP report) ---",
            f"%% Eligible for Outreach,Called,???",
            f"%% Eligible for Outreach,Not Yet Called,???",
            f"%% Called,Reached Member,???",
            f"%% Called,Not Reached - Voicemail/No Answer,???",
            f"%% Called,Call Failed,???",
            f"%% Reached Member,Refill Submitted / Reminded,???",
            f"%% Reached Member,Member Declined / Closed,???",
        ]

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Generate YTD Sankey counts")
    parser.add_argument(
        "--etl-dir",
        required=True,
        help="Base daily_etl_dir path, e.g. s3://humana-prod-data/2026/Refill/daily_refill_etl/",
    )
    parser.add_argument(
        "--config",
        required=True,
        help="Path to config file (used to download CMAPP Humana Refill Report YTD)",
    )
    args = parser.parse_args()

    base_dir = resolve_path(args.etl_dir)

    print("Scanning for initial_scrub_file files...", file=sys.stderr)
    initial_scrub_files = glob_all_files_with_suffix(base_dir, "_initial_scrub_file.csv")
    print(f"  Found {len(initial_scrub_files)} files", file=sys.stderr)

    print("Scanning for original_refill_data_scrubbed files...", file=sys.stderr)
    scrubbed_original_files = glob_all_files_with_suffix(
        base_dir, "_original_refill_data_scrubbed.csv"
    )
    print(f"  Found {len(scrubbed_original_files)} files", file=sys.stderr)

    print("Computing scrub counts (member level)...", file=sys.stderr)
    scrub_counts = get_scrub_counts(initial_scrub_files)

    print("Scanning for transformed_claims files...", file=sys.stderr)
    transformed_claims_files = glob_all_files_with_suffix(base_dir, "_transformed_claims.csv")
    print(f"  Found {len(transformed_claims_files)} files", file=sys.stderr)

    print("Computing eligible member count (member level)...", file=sys.stderr)
    eligible = get_eligible_member_count(scrubbed_original_files)

    print("Computing transformed claims count (member level)...", file=sys.stderr)
    transformed_claims_count = get_transformed_claims_count(transformed_claims_files)

    total_from_humana = scrub_counts["total_scrubbed"] + eligible

    print(f"\n=== YTD Counts (Member Level) ===", file=sys.stderr)
    print(f"  Total from Humana:  {total_from_humana:,}", file=sys.stderr)
    print(f"  Scrubbed:           {scrub_counts['total_scrubbed']:,}", file=sys.stderr)
    for reason, count in sorted(
        scrub_counts["by_reason"].items(), key=lambda x: -x[1]
    ):
        print(f"    {reason}: {count:,}", file=sys.stderr)
    print(f"  Eligible:           {eligible:,}", file=sys.stderr)
    print(f"  Transformed Claims: {transformed_claims_count:,}", file=sys.stderr)

    print("\nDownloading Humana Refill Report from CMAPP (YTD)...", file=sys.stderr)
    config = HumanaRefillConfig(config_file=Path(args.config))
    cmapp_client = config.get_cmapp_client()
    pst = pytz.timezone("America/Los_Angeles")
    now = datetime.now(tz=pst)
    start_date = now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    end_date = now.replace(hour=22, minute=0, second=0, microsecond=0)
    report_df = daily.download_cmapp_report(
        cmapp_client=cmapp_client,
        report_category="Humana Refill Report",
        account="Humana - Refill Reminder",
        start_date=start_date,
        end_date=end_date,
    )
    print(f"  Downloaded {len(report_df):,} rows", file=sys.stderr)

    disposition_counts = None
    if report_df is not None and not report_df.empty:
        print("\nComputing disposition counts...", file=sys.stderr)
        disposition_counts = get_disposition_counts(report_df)
        if disposition_counts:
            print(f"  Total called:  {disposition_counts['total_called']:,}", file=sys.stderr)
            for group, count in disposition_counts["by_group"].items():
                print(f"    {group}: {count:,}", file=sys.stderr)

    print("\n=== Mermaid Sankey Diagram ===\n")
    # sankey = build_sankey(
    #     total_from_humana=total_from_humana,
    #     scrub_counts=scrub_counts,
    #     eligible=eligible,
    #     disposition_counts=disposition_counts,
    # )
    # print(sankey)


if __name__ == "__main__":
    main()
