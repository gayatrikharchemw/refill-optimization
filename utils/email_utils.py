from mwemailer import Emailer
from typing import Dict

import pandas as pd


def get_email_client(config: Dict) -> Emailer:
    email_config = config["email"]
    return Emailer(from_address=email_config["from_address"])


def render_metric_email(metric_name: str, previous: float, current: float, config: dict) -> str:
    email_config = config["email"]
    direction = "increased" if current > previous else "decreased"
    change = abs(current - previous)
    color = "green" if current > previous else "red"

    return f"""
    <p>Hi {email_config['recipient_name']},</p>
    <p>The <strong>{metric_name}</strong> metric has <strong style="color:{color}">{direction} by {change:.1f}%</strong> week over week.</p>
    <table border="1" cellpadding="6">
        <tr><th>Previous Week</th><th>Current Week</th><th>Change</th></tr>
        <tr>
            <td>{previous:.1f}%</td>
            <td>{current:.1f}%</td>
            <td style="color:{color}">{'+' if current > previous else '-'}{change:.1f}%</td>
        </tr>
    </table>
    <p>Thanks,<br>{email_config['sender_name']}</p>
    """


def send_metric_alerts(submission_result_counts: pd.DataFrame, config: dict) -> None:
    if len(submission_result_counts) < 2:
        return

    prev = submission_result_counts.iloc[-2]
    curr = submission_result_counts.iloc[-1]

    metrics = [
        ("% Refill Submitted", "Refill Submitted Rate"),
        ("% Reminded - Refilled on own", "Reminded - Refilled on Own Rate"),
    ]

    emailer = get_email_client(config)
    recipients = config["email"]["to"]

    for col, label in metrics:
        if col not in submission_result_counts.columns:
            continue
        previous_val = float(prev[col])
        current_val = float(curr[col])
        prev_date_range = prev["Date Range"]
        curr_date_range = curr["Date Range"]

        html = render_metric_email(label, previous_val, current_val, config)
        subject = f"Refill Alert: {label} has {'increased' if current_val > previous_val else 'decreased'} ({prev_date_range} → {curr_date_range})"

        emailer.send_email(to_addresses=recipients, subject=subject, html=html)
