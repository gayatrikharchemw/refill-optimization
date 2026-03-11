import pandas as pd


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
    blank = df["Refill Reminder Result"].isna().sum() + (df["Refill Reminder Result"].str.strip() == "").sum()

    return pd.Series({
        "Accepted": accepted,
        "Declined": declined,
        "Not Completed": blank,
    })


def _submission_week_summary(df):
    counts = df["Refill Submission Result"].value_counts()

    refilled_on_own = counts.get("Reminded - Refilled on own", 0)
    no_post_completion = counts.get("No Post Completion Workflow", 0)
    denominator = counts.get("Refill Submitted", 0) + counts.get("Out of Refill - Refill Request", 0)
    ratio = round((refilled_on_own + no_post_completion) / denominator, 2) if denominator > 0 else None

    result = counts.to_dict()
    result["(Refilled on Own + No Post Completion) : (Refill Submitted + Out of Refill)"] = ratio
    return pd.Series(result)


def build_reports(refill_report: pd.DataFrame):
    refill_report = refill_report.copy()
    refill_report["Interaction Date"] = pd.to_datetime(refill_report["Interaction Date"])
    refill_report["Week"] = refill_report["Interaction Date"].dt.to_period("W").dt.start_time.dt.date
    refill_report["Date Range"] = refill_report["Week"].apply(
        lambda w: f"{w.strftime('%m/%d/%Y')} - {(w + pd.Timedelta(days=6)).strftime('%m/%d/%Y')}"
    )

    week_to_date_range = refill_report.drop_duplicates("Week").set_index("Week")["Date Range"]

    result_summary_df = refill_report.groupby("Week").apply(_week_summary).reset_index()
    result_summary_df.insert(1, "Date Range", result_summary_df["Week"].map(week_to_date_range))
    result_summary_df = _add_total_and_pct(result_summary_df.drop(columns=["Week"]))

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
    week_ratios = accepted_df.groupby("Week").apply(_submission_week_summary)[ratio_key]

    submission_result_counts.insert(1, "Date Range", submission_result_counts["Week"].map(week_to_date_range))
    submission_result_counts[ratio_key] = submission_result_counts["Week"].map(week_ratios)
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

        return pd.Series({
            "Total Completed Calls": total,
            "Refill Accepted": accepted,
            "% Refill Accepted": round(accepted / total * 100, 1) if total > 0 else None,
            "Refill Declined": declined,
            "% Refill Declined": round(declined / total * 100, 1) if total > 0 else None,
            "Reminded - Refilled on Own": refilled_on_own,
            "% Reminded - Refilled on Own (of Total)": round(refilled_on_own / total * 100, 1) if total > 0 else None,
        })

def build_agent_report(refill_report: pd.DataFrame) -> pd.DataFrame:
    completed = refill_report.loc[refill_report["Disposition"] == "Completed"].copy()

    df = (
        completed
        .groupby("Completion By Email")
        .apply(agent_summary)
        .reset_index()
        .sort_values("% Reminded - Refilled on Own (of Total)", ascending=False)
        .reset_index(drop=True)
    )

    pct_col = "% Reminded - Refilled on Own (of Total)"
    threshold = df[pct_col].quantile(0.75)
    df["Above 75th Percentile"] = df[pct_col] > threshold

    return df
