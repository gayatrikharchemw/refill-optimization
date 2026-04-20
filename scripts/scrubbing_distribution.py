import argparse
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import pytz

from utils import file_utils, scrubbing
from utils.config_utils import HumanaRefillConfig
from utils.nice import GROUP_IDS, get_dnc_records

"""
Grabs daily data sent by humana and scrubs it

- Uses daily DNC list constructed by medwatchers to create initial scrub report
- Creates scrub report file and filters out scrubbed members from daily data
- TODO: Add retries to this script
"""


logger = logging.getLogger(__name__)


def main(
    config: HumanaRefillConfig,
    pst_now: Optional[datetime],
    overwrite: bool = False,
    first_day: bool = False,
):
    if not pst_now:
        pst_now = datetime.now().astimezone(pytz.timezone("America/Los_Angeles"))

    est_now = pst_now.astimezone(pytz.timezone("US/Eastern"))

    pst_now_str = pst_now.strftime("%Y-%m-%d")

    daily_etl_dir = config["paths"]["daily_etl_dir"] / pst_now_str

    daily_etl_dir.mkdir(exist_ok=True)

    original_data_dir = config["paths"]["original_data_dir"]

    nice_client = config.get_nice_client()

    all_dnc_numbers = []

    for group_id in GROUP_IDS:
        group_dnc_list = get_dnc_records(
            client=nice_client, dnc_group_id=group_id, date_collected_cutoff=pst_now
        )

        all_dnc_numbers += group_dnc_list

    dnc_list = pd.DataFrame({"DNC NUMBER": all_dnc_numbers})

    # find all of todays files
    # log as discovered in DB

    original_df = file_utils.load_all_of_todays_original_files(
        data_dir=original_data_dir, today=est_now
    )

    one_week_ago = pst_now - timedelta(days=7)

    with config.get_outreach_db_client() as conn:
        curr = conn.cursor()

        select_query = "select destination, timestamp from call_logs where skill_name in ('Refill-Humana-ENG-OB', 'Refill-Humana-SPA-OB') and timestamp >= %s"

        curr.execute(select_query, (one_week_ago,))

        call_logs = pd.DataFrame(curr.fetchall(), columns=["number", "timestamp"])

        call_logs["number"] = call_logs["number"].str.lstrip("+1")

        if call_logs.empty:
            call_logs = call_logs.astype(dtype={"number":"string", "timestamp": "datetime64[ns]"})


    initial_scrub_df = scrubbing.create_initial_scrub_report(
        original_data=original_df,
        dnc_list=dnc_list,
        call_log_df=call_logs,
        today=pst_now,
        first_day=first_day,
    )

    original_without_scrubbed = scrubbing.filter_out_scrubbed_members(
        original_data=original_df, scrub_df=initial_scrub_df
    )

    reports_dir = config["paths"]["reports"] / pst_now_str
    reports_dir.mkdir(parents=True, exist_ok=True)

    scrubbed_original_file_path = (
        reports_dir / scrubbing.get_scrubbed_original_filename(date=pst_now)
    )

    initial_scrub_report_path = (
        reports_dir / scrubbing.get_initial_scrub_report_filename(pst_now)
    )

    file_utils.save_df_to_csv(
        df=original_without_scrubbed,
        path=scrubbed_original_file_path,
        overwrite=overwrite,
    )

    file_utils.save_df_to_csv(
        df=initial_scrub_df, path=initial_scrub_report_path, overwrite=overwrite
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser()

    parser.add_argument("config_file")

    parser.add_argument("--date")

    parser.add_argument("--overwrite", action="store_true")

    parser.add_argument("--first_day", action="store_true")

    args = parser.parse_args()

    date = args.date

    config_path = Path(args.config_file)

    if args.date:
        date = datetime.strptime(args.date, "%Y-%m-%d").astimezone(
            pytz.timezone("America/Los_Angeles")
        )

    config = HumanaRefillConfig(config_file=config_path)

    main(config, date, args.overwrite, args.first_day)
