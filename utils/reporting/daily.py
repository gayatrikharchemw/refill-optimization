import logging
from datetime import datetime
from io import StringIO
from typing import Optional

import pandas as pd
from cmappclient.reports.report_generation import ReportGeneration

logger = logging.getLogger(__name__)


def download_cmapp_report(
    cmapp_client,
    report_category: str,
    account,
    start_date: datetime,
    end_date: datetime,
    name: Optional[str] = None,
) -> pd.DataFrame:
    logger.info(f"Downloading : {report_category} report.")
    rg = ReportGeneration(cmapp_client)
    now = datetime.now()

    if not name:
        name = f"{account} {report_category} {start_date.strftime('%Y%m%d %H%M%S')} to {end_date.strftime('%Y%m%d %H%M%S')} {int(now.timestamp())}".replace(
            "(", ""
        ).replace(")", "")

    return pd.read_csv(
        StringIO(
            rg.generate_report(
                name=name,
                account=account,
                report_category=report_category,
                created_date_from=start_date,
                created_date_to=end_date,
                wait_until_finished=True,
                num_retries=100,
                wait_for_running=False,
            ).text
        ),
        dtype="string",
    )
