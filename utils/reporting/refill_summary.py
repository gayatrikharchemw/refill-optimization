import io
import sys
from collections import defaultdict
from pathlib import Path

import pandas as pd
from s3path import S3Path


def _read_csv_from_path(path) -> pd.DataFrame:
    if isinstance(path, S3Path):
        return pd.read_csv(io.BytesIO(path.read_bytes()), dtype="string")
    return pd.read_csv(path, dtype="string")


def _load_and_concat(paths: list) -> pd.DataFrame:
    dfs = []
    for p in paths:
        try:
            dfs.append(_read_csv_from_path(p))
        except Exception as e:
            print(f"  Warning: could not read {p}: {e}", file=sys.stderr)
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


def get_week_totals_from_transformed_claims(transformed_claims_files: list) -> dict:
    """Groups transformed claims files by day, counts unique Document Keys per day, sums per week."""
    if not transformed_claims_files:
        return {}

    days: dict = defaultdict(list)
    for path in transformed_claims_files:
        days[path.parent.name].append(path)

    week_totals: dict = defaultdict(int)
    for day_str, day_files in days.items():
        try:
            day_date = pd.to_datetime(day_str).date()
        except Exception:
            continue
        week_start = day_date - pd.Timedelta(days=day_date.weekday())
        df = _load_and_concat(day_files)
        if df.empty:
            continue
        if "Document Key" in df.columns:
            week_totals[week_start] += df["Document Key"].nunique()
        else:
            print("  Warning: no Document Key column in transformed_claims files", file=sys.stderr)

    return dict(week_totals)


def _add_total_and_pct(df, skip_pct_cols=None):
    skip_pct_cols = set(skip_pct_cols or [])
    count_cols = [c for c in df.columns if c != "Date Range" and c not in skip_pct_cols]
    df["Total"] = df[count_cols].apply(pd.to_numeric, errors="coerce").sum(axis=1)

    pct_cols = {}
    for col in count_cols:
        numeric = pd.to_numeric(df[col], errors="coerce")
        pct_cols[f"% {col}"] = (numeric / df["Total"] * 100).round(1)

    return pd.concat([df, pd.DataFrame(pct_cols, index=df.index)], axis=1)


def _week_summary(df):
    accepted = (df["Refill Reminder Result"] == "Yes").sum()
    declined = (df["Refill Reminder Result"] == "No").sum()
    return pd.Series({"Accepted": accepted, "Declined": declined})


def _submission_week_summary(df):
    counts = df["Refill Submission Result"].value_counts()

    refilled_on_own = counts.get("Reminded - Refilled on own", 0)
    no_post_completion = counts.get("No Post Completion Workflow", 0)
    denominator = counts.get("Refill Submitted", 0) + counts.get("Out of Refill - Refill Request", 0)
    ratio = round((refilled_on_own + no_post_completion) / denominator, 2) if denominator > 0 else None

    result = counts.to_dict()
    result["(Refilled on Own + No Post Completion) : (Refill Submitted + Out of Refill)"] = ratio
    return pd.Series(result)


def build_reports(refill_report: pd.DataFrame, transformed_claims_files: list = None):
    refill_report = refill_report.copy()
    refill_report["Interaction Date"] = pd.to_datetime(refill_report["Interaction Date"])
    refill_report["Week"] = refill_report["Interaction Date"].dt.to_period("W").dt.start_time.dt.date
    refill_report["Date Range"] = refill_report["Week"].apply(
        lambda w: f"{w.strftime('%m/%d/%Y')} - {(w + pd.Timedelta(days=6)).strftime('%m/%d/%Y')}"
    )

    week_to_date_range = refill_report.drop_duplicates("Week").set_index("Week")["Date Range"]

    result_summary_df = refill_report.groupby("Week").apply(_week_summary, include_groups=False).reset_index()
    result_summary_df.insert(1, "Date Range", result_summary_df["Week"].map(week_to_date_range))

    week_totals = get_week_totals_from_transformed_claims(transformed_claims_files or [])
    result_summary_df["Total"] = result_summary_df["Week"].map(week_totals).fillna(0).astype(int)
    result_summary_df["Not Completed"] = (result_summary_df["Total"] - result_summary_df["Accepted"] - result_summary_df["Declined"]).clip(lower=0)

    result_summary_df = result_summary_df.drop(columns=["Week"])
    for col in ["Accepted", "Declined", "Not Completed"]:
        result_summary_df[f"% {col}"] = (result_summary_df[col] / result_summary_df["Total"] * 100).round(1)
    result_summary_df["% Completed"] = (result_summary_df["% Accepted"] + result_summary_df["% Declined"]).round(1)

    decline_reason_counts = (
        refill_report.loc[refill_report["Refill Reminder Result"] == "No"]
        .groupby("Week")["Denial Reason"]
        .value_counts()
        .unstack(fill_value=0)
        .reset_index()
    )
    decline_reason_counts.insert(1, "Date Range", decline_reason_counts["Week"].map(week_to_date_range))
    decline_reason_counts = _add_total_and_pct(decline_reason_counts.drop(columns=["Week"]))
    decline_reason_counts = decline_reason_counts.rename(columns={"Total": "Total Declined"})

    accepted_df = refill_report.loc[refill_report["Refill Reminder Result"] == "Yes"].copy()
    accepted_df["Refill Submission Result"] = accepted_df["Refill Submission Result"].fillna("No Post Completion Workflow")
    accepted_df.loc[accepted_df["Refill Submission Result"].str.strip() == "", "Refill Submission Result"] = "No Post Completion Workflow"

    submission_result_counts = (
        accepted_df
        .groupby("Week")["Refill Submission Result"]
        .value_counts()
        .unstack(fill_value=0)
        .reset_index()
    )
    ratio_key = "(Refilled on Own + No Post Completion) : (Refill Submitted + Out of Refill)"

    submission_result_counts.insert(1, "Date Range", submission_result_counts["Week"].map(week_to_date_range))
    refilled_on_own = submission_result_counts.get("Reminded - Refilled on own", 0)
    no_post = submission_result_counts.get("No Post Completion Workflow", 0)
    denom = submission_result_counts.get("Refill Submitted", 0) + submission_result_counts.get("Out of Refill - Refill Request", 0)
    submission_result_counts[ratio_key] = ((refilled_on_own + no_post) / denom).where(denom > 0).round(2)
    submission_result_counts = _add_total_and_pct(submission_result_counts.drop(columns=["Week"]), skip_pct_cols=[ratio_key])
    submission_result_counts = submission_result_counts.rename(columns={"Total": "Total Accepted"})

    return result_summary_df, decline_reason_counts, submission_result_counts


def agent_summary(df):
        total = len(df)
        accepted = (df["Refill Reminder Result"] == "Yes").sum()
        declined = (df["Refill Reminder Result"] == "No").sum()
        refilled_on_own = (
            (df["Refill Reminder Result"] == "Yes") &
            (df["Refill Submission Result"] == "Reminded - Refilled on own")
        ).sum()
        refill_submitted = (
            (df["Refill Reminder Result"] == "Yes") &
            (df["Refill Submission Result"] == "Refill Submitted")
        ).sum()
        does_not_want_refill = (
            (df["Refill Reminder Result"] == "No") &
            (df["Denial Reason"] == "Member Does Not Want Refill")
        ).sum()

        return pd.Series({
            "Total Completed Calls": total,
            "Refill Accepted": accepted,
            "% Refill Accepted": round(accepted / total * 100, 1) if total > 0 else None,
            "Refill Declined": declined,
            "% Refill Declined": round(declined / total * 100, 1) if total > 0 else None,
            "Member Does Not Want Refill": does_not_want_refill,
            "% Member Does Not Want Refill": round(does_not_want_refill / total * 100, 1) if total > 0 else None,
            "Reminded - Refilled on Own": refilled_on_own,
            "% Reminded - Refilled on Own (of Total)": round(refilled_on_own / total * 100, 1) if total > 0 else None,
            "% Reminded - Refilled on Own (of Accepted)": round(refilled_on_own / accepted * 100, 1) if accepted > 0 else None,
            "% Refill Submitted (of Accepted)": round(refill_submitted / accepted * 100, 1) if accepted > 0 else None,
        })

MIN_COMPLETED_CASES = 100
ACCEPTED_RATE_THRESHOLD = 84.0


def build_under_100_report(
    refill_report: pd.DataFrame,
    as_of_date=None,
) -> pd.DataFrame:
    """Build weekly report of agents with fewer than 100 completed calls."""
    import datetime

    completed = refill_report.loc[refill_report["Disposition"] == "Completed"].copy()
    completed["Interaction Date"] = pd.to_datetime(completed["Interaction Date"]).dt.date

    if as_of_date is None:
        as_of_date = datetime.date.today()

    week_start = as_of_date - datetime.timedelta(days=as_of_date.weekday())
    week_end = week_start + datetime.timedelta(days=6)
    mask = (completed["Interaction Date"] >= week_start) & (completed["Interaction Date"] <= week_end)
    completed = completed.loc[mask]

    df = (
        completed
        .groupby("Completion By Email")
        .apply(agent_summary)
        .reset_index()
    )

    df = df.loc[df["Total Completed Calls"] < MIN_COMPLETED_CASES].reset_index(drop=True)
    df = df.sort_values("Total Completed Calls", ascending=True).reset_index(drop=True)
    df.insert(1, "Week", f"{week_start.strftime('%m/%d/%Y')} - {week_end.strftime('%m/%d/%Y')}")

    return df


def build_agent_report(
    refill_report: pd.DataFrame,
    as_of_date=None,
    period: str = "weekly",
) -> pd.DataFrame:
    """Build agent performance report for the given period.

    Args:
        refill_report: Full YTD refill report DataFrame.
        as_of_date: datetime.date (or None for today). The report covers the
                    day / week / month that contains this date.
        period: One of "daily", "weekly", or "monthly".
    """
    import datetime

    completed = refill_report.loc[refill_report["Disposition"] == "Completed"].copy()
    completed["Interaction Date"] = pd.to_datetime(completed["Interaction Date"]).dt.date

    if as_of_date is None:
        as_of_date = datetime.date.today()

    if period == "daily":
        mask = completed["Interaction Date"] == as_of_date
    elif period == "weekly":
        # ISO week: Monday to Sunday containing as_of_date
        week_start = as_of_date - datetime.timedelta(days=as_of_date.weekday())
        week_end = week_start + datetime.timedelta(days=6)
        mask = (completed["Interaction Date"] >= week_start) & (completed["Interaction Date"] <= week_end)
    elif period == "monthly":
        mask = completed["Interaction Date"].apply(
            lambda d: d.year == as_of_date.year and d.month == as_of_date.month
        )
    else:
        raise ValueError(f"period must be 'daily', 'weekly', or 'monthly', got {period!r}")

    completed = completed.loc[mask]

    df = (
        completed
        .groupby("Completion By Email")
        .apply(agent_summary)
        .reset_index()
    )

    df = df.loc[df["Total Completed Calls"] >= MIN_COMPLETED_CASES].reset_index(drop=True)

    pct_col = "% Reminded - Refilled on Own (of Accepted)"
    threshold = df[pct_col].quantile(0.75)
    df["Above 75th Percentile"] = df[pct_col] > threshold

    avg_submitted = df["% Refill Submitted (of Accepted)"].mean()
    avg_does_not_want = df["% Member Does Not Want Refill"].mean()

    low_accepted = df["% Refill Accepted"] < ACCEPTED_RATE_THRESHOLD
    low_submitted = df["% Refill Submitted (of Accepted)"] < avg_submitted
    high_does_not_want = df["% Member Does Not Want Refill"] > avg_does_not_want

    flags = []
    for idx in df.index:
        agent_flags = []
        if not low_accepted[idx] and low_submitted[idx]:
            agent_flags.append("a")
        if low_accepted[idx] and low_submitted[idx]:
            agent_flags.append("b")
        if low_accepted[idx] and high_does_not_want[idx]:
            agent_flags.append("c")
        flags.append(", ".join(agent_flags))

    df["Performance Flag"] = flags

    df = df.sort_values("% Refill Submitted (of Accepted)", ascending=True).reset_index(drop=True)

    thresholds = {
        "min_completed": MIN_COMPLETED_CASES,
        "accepted_rate": ACCEPTED_RATE_THRESHOLD,
        "avg_submitted": avg_submitted,
        "avg_does_not_want": avg_does_not_want,
    }

    return df, thresholds
