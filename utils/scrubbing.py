import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import pytz
from pytz import timezone

from humana_refill_etl.date_util import get_current_est_time, get_current_pst_time
from humana_refill_etl.etl import construct_member_id_col
from humana_refill_etl.file_utils import (
    REFILL_TZ,
    get_most_recent_refill_file,
    read_df_from_csv,
)

SCRUB_COLUMNS = ["leadid", "scrub_reason", "idcard_mbr_id", "scrub_date"]

SCRUB_REASONS = {"Do not call", "Invalid number", "Max attempt reached", "Missing data"}

SCRUB_DATE_FMT = "%Y-%m-%d"

SCRUBBED_ORIGINAL_DATA_FILE_FMT = "{}_original_refill_data_scrubbed.csv"

INITIAL_SCRUB_FILE_FMT = "{}_initial_scrub_file.csv"

SFTP_SCRUB_FILE_FMT = "Humana-MW-RefillConcierge-Scrub-{}.csv"


logger = logging.getLogger(__name__)


def construct_sftp_scrub_file_name(date: datetime) -> str:
    sftp_date = date.strftime("%Y%m%d%H")

    return SFTP_SCRUB_FILE_FMT.format(sftp_date)


class InvalidScrubFormatError(Exception):
    pass


def get_scrubbed_original_filename(date: datetime) -> str:
    """
    Returns filename of the scrubbed etl data Humana has sent us for the day
    """
    return SCRUBBED_ORIGINAL_DATA_FILE_FMT.format(date.strftime("%Y-%m-%d"))


def get_initial_scrub_report_filename(date: datetime) -> str:
    """
    Returns filename of report containing members we've scrubbed from the data Humana
    has sent us for the day
    """
    return INITIAL_SCRUB_FILE_FMT.format(date.strftime("%Y-%m-%d"))


def validate_scrub_df(scrub_df: pd.DataFrame):
    col_set = set(scrub_df.columns.tolist())

    scrub_set = set(SCRUB_COLUMNS)

    missing_columns = scrub_set.difference(col_set)

    extra_columns = col_set.difference(scrub_set)

    if len(missing_columns) > 0:
        raise InvalidScrubFormatError(
            f"The following required scrub columns are missing from the scrub df: {missing_columns}"
        )

    if len(extra_columns) > 0:
        raise InvalidScrubFormatError(
            f"The following extra columns were found in the scrub df: {extra_columns}"
        )

    unique_reasons = set(scrub_df["scrub_reason"])

    unknown_scrub_reasons = unique_reasons.difference(SCRUB_REASONS)

    if len(unknown_scrub_reasons) > 0:
        raise InvalidScrubFormatError(
            f"Found the following invalid scrub reasons in the scrub df: {unknown_scrub_reasons}"
        )


def create_scrub_df(
    slice_to_scrub: pd.DataFrame, scrub_reason: str, today: Optional[datetime] = None
):
    scrub_df = pd.DataFrame()
    scrub_df["leadid"] = slice_to_scrub["leadid"]

    scrub_df["scrub_reason"] = scrub_reason

    scrub_df["idcard_mbr_id"] = construct_member_id_col(df=slice_to_scrub)

    if not today:
        today = datetime.now(tz=pytz.timezone(REFILL_TZ))

    today_str = today.strftime(SCRUB_DATE_FMT)

    scrub_df["scrub_date"] = today_str

    return scrub_df


def find_members_past_call_limit(original_data: pd.DataFrame, five9_df: pd.DataFrame):
    groups = five9_df.groupby("Phone Number")["Contact Date"].unique().reset_index()

    numbers_past_call_limit = groups.loc[
        groups["Contact Date"].apply(lambda x: len(x) >= 2)
    ]["Phone Number"]

    logger.info(f"Found {len(numbers_past_call_limit)} numbers past call limit.")

    return original_data.loc[
        original_data["RecipientPhoneNumber"].isin(numbers_past_call_limit)
    ]


def find_members_on_dnc_list(
    original_data: pd.DataFrame,
    dnc_df: pd.DataFrame,
    dnc_number_col: str = "DNC NUMBER",
):
    numbers_to_scrub = original_data["RecipientPhoneNumber"].isin(
        dnc_df[dnc_number_col]
    )

    dnc_members = original_data.loc[numbers_to_scrub]

    logger.info(f"Found {len(dnc_members)} numbers on the DNC list.")

    return dnc_members


def find_members_called_yesterday(
    original_data: pd.DataFrame,
    call_log_df: pd.DataFrame,
    today: Optional[datetime] = None,
):
    if not today:
        today = get_current_pst_time()

    yesterday = today - timedelta(days=1)

    yesterday_df = call_log_df[call_log_df["timestamp"].dt.date == yesterday.date()]

    was_called_yesterday = original_data["RecipientPhoneNumber"].isin(
        yesterday_df["number"]
    )

    slice_to_scrub = original_data.loc[was_called_yesterday]

    scrub_df = create_scrub_df(
        slice_to_scrub, scrub_reason="Max attempt reached", today=today
    )

    logger.info(
        f"Found {len(scrub_df)} numbers who were called yesterday ({yesterday.strftime('%Y-%m-%d')})"
    )

    validate_scrub_df(scrub_df)

    return scrub_df


def scrub_fatigued_numbers_from_weekly_data(
    original_data: pd.DataFrame,
    weekly_call_logs: pd.DataFrame,
    max_allowable_contact_attempts: int = 4,
    today: Optional[datetime] = None,
):
    """
    finds numbers that have been contacted more than <max_allowable_contact_attempts> times

    meant to be used with data returned from combine_this_weeks_five9_reports()

    members can be called max three different days across a 7 day period
    """

    weekly_call_logs["Report Date"] = weekly_call_logs["timestamp"].dt.strftime(
        "%Y-%m-%d"
    )

    weekly_call_logs = weekly_call_logs[
        weekly_call_logs["timestamp"].dt.date < today.date()
    ]

    groups = weekly_call_logs.groupby("number")["Report Date"].unique().reset_index()

    numbers_past_call_limit = groups.loc[
        groups["Report Date"].apply(lambda x: len(x) >= max_allowable_contact_attempts)
    ]["number"]

    fatigued_members = original_data[
        original_data["RecipientPhoneNumber"].isin(numbers_past_call_limit)
    ]

    scrub_df = create_scrub_df(
        slice_to_scrub=fatigued_members, scrub_reason="Max attempt reached", today=today
    )

    validate_scrub_df(scrub_df)

    logger.info(f"Scrubbing {len(scrub_df)} members for call fatigue")

    return scrub_df


def create_initial_scrub_report(
    original_data: pd.DataFrame,
    dnc_list: pd.DataFrame,
    call_log_df: pd.DataFrame,
    dnc_number_col: str = "DNC NUMBER",
    today: Optional[datetime] = None,
    first_day: bool = False,
):
    """
    If first_day, then wont try to search for a prior days report, will only filter members from dnc list

    """

    dnc_members = find_members_on_dnc_list(
        original_data=original_data,
        dnc_df=dnc_list,
        dnc_number_col=dnc_number_col,
    )

    dnc_scrub_df = create_scrub_df(
        slice_to_scrub=dnc_members, scrub_reason="Do not call", today=today
    )

    if not first_day:
        members_called_yesterday = find_members_called_yesterday(
            original_data=original_data,
            call_log_df=call_log_df,
            today=today,
        )

        fatigued_members = scrub_fatigued_numbers_from_weekly_data(
            original_data=original_data,
            weekly_call_logs=call_log_df,
            today=today,
        )

        dfs_to_combine = [dnc_scrub_df, members_called_yesterday, fatigued_members]

    else:
        logger.info(f"Not looking for previous day's report since 'first_day' true")

        dfs_to_combine = [dnc_scrub_df]

    try:
        combined_scrub_df = pd.concat(dfs_to_combine).reset_index(drop=True)
    except ValueError as e:
        if "No objects to concatenate" in str(e):
            combined_scrub_df = pd.DataFrame(columns=SCRUB_COLUMNS)
        else:
            raise e

    logger.info(f"Added {len(combined_scrub_df)} rows to initial scrub report")

    dupes_dropped = combined_scrub_df.drop_duplicates(subset="leadid")

    validate_scrub_df(scrub_df=dupes_dropped)

    return dupes_dropped


def filter_out_scrubbed_members(original_data: pd.DataFrame, scrub_df: pd.DataFrame):
    original_data["idcard_mbr_id"] = construct_member_id_col(df=original_data)

    filtered = original_data.loc[
        ~original_data["idcard_mbr_id"].isin(scrub_df["idcard_mbr_id"])
    ].reset_index(drop=True)

    logger.info(
        f"Removed {len(original_data) - len(filtered)} members from original file after scrubbing"
    )

    return filtered
