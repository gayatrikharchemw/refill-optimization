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


def send_metric_alerts(submission_result_counts: pd.DataFrame, result_summary_df: pd.DataFrame, decline_reason_counts: pd.DataFrame, config: dict) -> None:
    if len(submission_result_counts) < 2:
        return

    prev = submission_result_counts.iloc[-2]
    curr = submission_result_counts.iloc[-1]

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
        curr_week = result_summary_df.iloc[-1]
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
        curr_decline = decline_reason_counts.iloc[-1]
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
