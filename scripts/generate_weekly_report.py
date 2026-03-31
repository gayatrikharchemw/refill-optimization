import argparse
from ast import Tuple
import logging
from datetime import datetime
from pathlib import Path
from typing import List
from zoneinfo import ZoneInfo

import pytz

from utils.config_utils import HumanaRefillConfig
from utils.date_util import get_current_pst_time
from utils.file_utils import save_df_to_csv
from utils.reporting import weekly


def main(
    config: HumanaRefillConfig,
    pst_now: datetime,
    overwrite: bool = False,
    ytd: bool = False,
    contract_splits: List[Tuple] = None,
):
    pst_now_str = pst_now.strftime("%Y-%m-%d")

    est_now = pst_now.astimezone(pytz.timezone("US/Eastern"))

    refill_report_dir = config["paths"]["reporting_dir"]

    weekly_report_dir = refill_report_dir / "weekly"

    weekly_report_dir.mkdir(exist_ok=True)

    this_weeks_reporting_dir = weekly_report_dir / pst_now_str

    this_weeks_reporting_dir.mkdir(exist_ok=True)

    original_folder = config["paths"]["original_data_dir"]

    etl_folder = config["paths"]["daily_etl_dir"]

    daily_reporting_folder = config["paths"]["reporting_dir"] / "daily"

    weekly_report_dict = weekly.create_weekly_refill_report(
        original_folder=original_folder,
        etl_folder=etl_folder,
        report_folder=daily_reporting_folder,
        current_date=est_now,
        split_by_contract=True,
        ytd=ytd,
        contract_splits=contract_splits,
    )

    for contract_star_identifier_pair in weekly_report_dict:
        contract, star_identifier = contract_star_identifier_pair.split(":")

        output_file = this_weeks_reporting_dir / weekly.WEEKLY_REPORT_FILE_FMT.format(
            pst_now_str + "_" + contract + "_" + star_identifier
        )

        save_df_to_csv(
            df=weekly_report_dict[contract_star_identifier_pair],
            path=output_file,
            overwrite=overwrite,
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser()

    parser.add_argument("config_path")

    parser.add_argument("--date", type=datetime.fromisoformat)

    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument(
        "--ytd",
        action="store_true",
        help="If passed, ignore weekly filter and generate a YTD Report.",
    )
    parser.add_argument(
        "--contract_splits",
        nargs="*",
        help="How to split up the report. Pass a list of ContractNumber:StarGroup pairs. StarGroup can be Stars, Nonstars, Both, or All (all rows for that contract). Example usage: --contract_splits H1019:Stars S5884:Stars H1019:All MAPD:All",
    )

    args = parser.parse_args()

    config_path = Path(args.config_path)

    if not args.date:
        pst_now = get_current_pst_time()

    else:
        pst_now = args.date.replace(tzinfo=ZoneInfo("America/Los_Angeles"))

    config = HumanaRefillConfig(config_file=config_path)

    if args.contract_splits:
        contract_splits = [tuple(item.split(":")) for item in args.contract_splits]

    else:
        contract_splits = None

    main(config, pst_now, args.overwrite, ytd=args.ytd, contract_splits=contract_splits)
