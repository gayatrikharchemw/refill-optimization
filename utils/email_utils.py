import datetime

from mwemailer import Emailer
from typing import Dict, List, Tuple

import pandas as pd


def get_email_client(config: Dict) -> Emailer:
    email_config = config["email"]
    return Emailer(from_address=email_config["from_address"])


def render_metric_email(
    metrics: List[Tuple[str, float, float, bool, str]],
    prev_date_range: str,
    curr_date_range: str,
    config: dict,
) -> str:
    """
    metrics: list of (label, previous, current, higher_is_better, fmt)
    fmt: format string for values, e.g. "{:.1f}%" or "{:.2f}"
    """
    email_config = config["email"]

    rows = ""
    for label, previous, current, higher_is_better, fmt in metrics:
        change = current - previous
        improved = change > 0 if higher_is_better else change < 0
        color = "green" if improved else "red"
        direction = "increased" if change > 0 else "decreased"
        sign = "+" if change > 0 else ""

        rows += f"""
        <tr>
            <td>{label}</td>
            <td>{fmt.format(previous)}</td>
            <td>{fmt.format(current)}</td>
            <td style="color:{color}">{direction} by {sign}{fmt.format(abs(change))}</td>
        </tr>"""

    return f"""
    <p>Hi {email_config['recipient_name']},</p>
    <p>Here is the week-over-week refill metric summary ({prev_date_range} → {curr_date_range}):</p>
    <table border="1" cellpadding="6">
        <tr><th>Metric</th><th>Previous Week</th><th>Current Week</th><th>Change</th></tr>
        {rows}
    </table>
    <p>Thanks,<br>{email_config['sender_name']}</p>
    """


def _find_week_row(df: pd.DataFrame, as_of_date: datetime.date):
    """Return the row whose Date Range contains as_of_date, or the last row."""
    week_start = as_of_date - datetime.timedelta(days=as_of_date.weekday())
    week_end = week_start + datetime.timedelta(days=6)
    target = f"{week_start.strftime('%m/%d/%Y')} - {week_end.strftime('%m/%d/%Y')}"
    match = df[df["Date Range"] == target]
    return match.iloc[-1] if not match.empty else df.iloc[-1]


def send_metric_alerts(submission_result_counts: pd.DataFrame, result_summary_df: pd.DataFrame, decline_reason_counts: pd.DataFrame, config: dict, as_of_date: datetime.date = None) -> None:
    if as_of_date is None:
        as_of_date = datetime.date.today()

    if len(submission_result_counts) < 2:
        return

    prev = _find_week_row(submission_result_counts, as_of_date - datetime.timedelta(weeks=1))
    curr = _find_week_row(submission_result_counts, as_of_date)

    # (column, label, higher_is_better, fmt)
    ratio_col = "(Refilled on Own + No Post Completion) : (Refill Submitted + Out of Refill)"
    metric_config = [
        ("% Refill Submitted", "Refill Submitted Rate", True, "{:.1f}%"),
        ("% Reminded - Refilled on own", "Reminded - Refilled on Own Rate", False, "{:.1f}%"),
        (ratio_col, "Refilled on Own + No Post Completion : Refill Submitted + Out of Refill", True, "{:.2f}"),
    ]

    metrics = []
    for col, label, higher_is_better, fmt in metric_config:
        if col not in submission_result_counts.columns:
            continue
        metrics.append((label, float(prev[col]), float(curr[col]), higher_is_better, fmt))

    if not metrics:
        return

    prev_date_range = prev["Date Range"]
    curr_date_range = curr["Date Range"]

    emailer = get_email_client(config)
    email_config = config["email"]

    html = render_metric_email(metrics, prev_date_range, curr_date_range, config)
    emailer.send_email(to_addresses=email_config["to"], subject=email_config["subject"], html=html)

    no_post_col = "No Post Completion Workflow"
    if no_post_col in submission_result_counts.columns:
        no_post_count = int(curr[no_post_col])
        if no_post_count > 10:
            alert_html = f"""
            <p>Hi {email_config['recipient_name']},</p>
            <p>The <strong>No Post Completion Workflow</strong> count for the current week
            (<strong>{curr_date_range}</strong>) has exceeded 10.</p>
            <p>Current count: <strong style="color:red">{no_post_count}</strong></p>
            <p>Thanks,<br>{email_config['sender_name']}</p>
            """
            emailer.send_email(
                to_addresses=email_config["no_post_completion_to"],
                subject=f"Refill Alert: No Post Completion Workflow above 10 ({curr_date_range})",
                html=alert_html,
            )

    if len(result_summary_df) > 0:
        curr_week = _find_week_row(result_summary_df, as_of_date)
        completion_html = f"""
        <p>Hi {email_config['recipient_name']},</p>
        <p>Here is this week's case completion summary for <strong>{curr_week['Date Range']}</strong>:</p>
        <table border="1" cellpadding="6">
            <tr><th>Metric</th><th>Value</th></tr>
            <tr><td>Total Leads</td><td>{int(curr_week['Total'])}</td></tr>
            <tr><td>% Completed</td><td>{curr_week['% Completed']:.1f}%</td></tr>
            <tr><td>% Not Completed</td><td>{curr_week['% Not Completed']:.1f}%</td></tr>
        </table>
        <p>Thanks,<br>{email_config['sender_name']}</p>
        """
        emailer.send_email(
            to_addresses=email_config["to"],
            subject=f"Refill Weekly Completion Summary ({curr_week['Date Range']})",
            html=completion_html,
        )

    decline_col = "% Member Does Not Want Refill"

    if len(decline_reason_counts) > 0 and decline_col in decline_reason_counts.columns:
        curr_decline = _find_week_row(decline_reason_counts, as_of_date)
        decline_val = float(curr_decline[decline_col])
        if decline_val > 25:
            decline_alert_html = f"""
            <p>Hi {email_config['recipient_name']},</p>
            <p>The <strong>% Member Does Not Want Refill</strong> for the current week
            (<strong>{curr_decline['Date Range']}</strong>) has exceeded 25%.</p>
            <p>Current value: <strong style="color:red">{decline_val:.1f}%</strong></p>
            <p>Thanks,<br>{email_config['sender_name']}</p>
            """
            emailer.send_email(
                to_addresses=email_config["to"],
                subject=f"Refill Alert: Member Does Not Want Refill above 25% ({curr_decline['Date Range']})",
                html=decline_alert_html,
            )


FLAG_DESCRIPTIONS = {
    "a": (
        "High Accepted Rate + Low Refill Submissions",
        "Acceptance is good but refill conversion during calls is low. "
        "Inform agent to improve refill submission rate during calls.",
    ),
    "b": (
        "Low Accepted Rate + Low Refill Submissions",
        "Low acceptance and conversion effectiveness. "
        "Highlight for performance improvement / training.",
    ),
    "c": (
        "Low Accepted Rate + High 'Member Does Not Want Refill'",
        "Higher declines observed during calls. "
        "Inform agent to improve objection handling & member communication.",
    ),
}


def send_agent_performance_alert(agent_report: pd.DataFrame, config: dict, date_range: str = None) -> None:
    email_config = config["email"]
    flagged = agent_report.loc[agent_report["Performance Flag"] != ""].copy()

    if flagged.empty:
        return

    flag_sections = ""
    for flag, (title, recommendation) in FLAG_DESCRIPTIONS.items():
        agents = flagged.loc[flagged["Performance Flag"].str.contains(flag)]
        if agents.empty:
            continue

        rows = ""
        for _, row in agents.iterrows():
            rows += f"""
            <tr>
                <td>{row['Completion By Email']}</td>
                <td>{int(row['Total Completed Calls'])}</td>
                <td>{row['% Refill Accepted']:.1f}%</td>
                <td>{row['% Refill Submitted (of Accepted)']:.1f}%</td>
                <td>{row['% Reminded - Refilled on Own (of Accepted)']:.1f}%</td>
                <td>{row['% Member Does Not Want Refill']:.1f}%</td>
            </tr>"""

        flag_sections += f"""
        <h3>Category {flag.upper()}: {title}</h3>
        <p><em>{recommendation}</em></p>
        <table border="1" cellpadding="6">
            <tr>
                <th>Agent</th>
                <th>Total Calls</th>
                <th>% Accepted</th>
                <th>% Refill Submitted</th>
                <th>% Reminded on Own</th>
                <th>% Does Not Want Refill</th>
            </tr>
            {rows}
        </table>"""

    if not flag_sections:
        return

    date_range_str = f" ({date_range})" if date_range else ""
    html = f"""
    <p>Hi,</p>
    <p>The following agents have been flagged for performance review based on data for{date_range_str}
    (minimum {agent_report['Total Completed Calls'].min():.0f}+ completed cases, accepted rate threshold: 84%).</p>
    {flag_sections}
    <p>Thanks,<br>{email_config['sender_name']}</p>
    """

    emailer = get_email_client(config)
    subject = f"Refill Agent Performance Review — Flagged Agents{date_range_str}"
    emailer.send_email(
        to_addresses=email_config["agent_performance_to"],
        subject=subject,
        html=html,
    )
