import calendar
import logging
import re
from datetime import date, datetime, timedelta
from functools import reduce
from pathlib import Path
from typing import Dict, Iterable, List, Literal, Optional, Tuple

import pandas as pd
import pytz

from utils.date_util import get_dates_for_weekly_report
from utils.file_utils import (
    REFILL_DATE_FMT,
    REFILL_TZ,
    get_most_recent_refill_file,
    read_df_from_csv,
)

logger = logging.getLogger(__name__)

FINAL_REPORT_COLS = [
    "Contact Date",
    "Leads Received",
    "Leads Available for Outreach Count",
    "Attempted Leads Count",
    "Attempt Rate",
    "Available for 2nd Attempts",
    "2nd Attempt Count",
    "2nd Attempt Rate",
    "Reached Leads Count",
    "Reach Rate %",
    "Refill Submitted",
    "Reminded - Refilled on own",
    "Refill of Reached Rate %",
    'Refill of "Refill Submitted" Rate %',
    "Refill Request",
    "Scrub Rate",
    "Refill of Attempted Rate",
    "Refill Submitted of Attempted Rate",
]

WEEKLY_REPORT_FILE_FMT = "{}_weekly_humana_refill_report.csv"


def get_report_end_date(rundate: datetime) -> datetime:
    current_day = rundate.weekday()
    offset = (current_day + 6 - calendar.TUESDAY) % 7 + 1

    report_end = (rundate - timedelta(days=offset)).replace(
        hour=23, minute=59, second=59
    )

    if report_end.isocalendar()[1] < rundate.isocalendar()[1]:
        logger.warning(
            f"Report end date will be for last Friday ({report_end.month}/{report_end.day}).\n\tThis is probably because you are running the report on {rundate.month}/{rundate.day}, which is before the end of Friday this week."
        )

    return report_end


def filter_combined_weekly_reports_for_year(
    combined_report_df: pd.DataFrame, year: int
) -> pd.DataFrame:
    return combined_report_df[
        pd.to_datetime(combined_report_df["Contact Date"]).dt.year == year
    ].reset_index(drop=True)


def combine_all_weekly_reports_in_folder(base_dir: Path) -> pd.DataFrame:
    glob_str = WEEKLY_REPORT_FILE_FMT.format("*")

    found_files = list(base_dir.rglob(glob_str))

    contracts = [
        re.match(
            r"\d{4}-\d{2}-\d{2}_(.*)_weekly_humana_refill_report.csv", file.name
        ).groups(0)[0]
        for file in found_files
    ]

    dfs = [read_df_from_csv(file) for file in found_files]

    for i in range(len(dfs)):
        dfs[i]["Contract"] = contracts[i]

    combined = pd.concat(dfs).reset_index(drop=True)

    return combined


def collect_weekly_folders(base_dir: Path, rundate: datetime = None):
    if not rundate:
        rundate = datetime.now(tz=pytz.timezone("America/Los_Angeles"))

    dates = get_dates_for_weekly_report(rundate)

    date_strs = [date.strftime("%Y-%m-%d") for date in dates]

    possible_folders = [base_dir / date for date in date_strs]

    found_folders = [folder for folder in possible_folders if folder.exists()]

    missing = set(possible_folders).difference(set(found_folders))

    if len(missing) > 0:
        logger.warning(
            f"Missing folders for the following days this week: {[item.name for item in missing]}"
        )

    return found_folders


def combine_files_for_weekly_report(
    folder_list: Iterable[Path],
    file_pattern: str,
    date_pattern: str,
    # current_date: datetime = None,
    tz: str = None,
):
    found_files_with_dates = []

    for folder in folder_list:
        found_files = folder.glob(file_pattern)

        files_and_dates = [
            (
                file,
                datetime.strptime(file.name, file_pattern.replace("*", date_pattern)),
            )
            for file in found_files
        ]

        found_files_with_dates += files_and_dates

    dfs = [read_df_from_csv(file) for file, _ in found_files_with_dates]

    for i in range(len(dfs)):
        dfs[i]["File Date"] = found_files_with_dates[i][1].strftime("%m/%d/%Y")

    combined_df = pd.concat(dfs).reset_index(drop=True)

    return combined_df


def find_original_files_for_week(
    base_dir: Path,
    file_pattern: str = "Emme_MW0001_Med_Adher_*.csv",
    date_fmt: str = REFILL_DATE_FMT,
    current_date: datetime = None,
):
    if not current_date:
        current_date = datetime.now(tz=pytz.timezone(REFILL_TZ))

    report_dates = get_dates_for_weekly_report(current_date)

    start_date, end_date = report_dates[0], report_dates[-1]

    all_files = list(base_dir.glob(file_pattern))

    files_and_dates = [
        (
            file,
            datetime.strptime(file.name, file_pattern.replace("*", date_fmt)).replace(
                tzinfo=pytz.timezone(REFILL_TZ)
            ),
        )
        for file in all_files
    ]

    this_weeks_files_and_dates = [
        (file, date)
        for file, date in files_and_dates
        if date.date() >= start_date and date.date() <= end_date
    ]

    return this_weeks_files_and_dates


# CONTRACT_SPLITS = ["H5216", "H1019"]

CONTRACT_SPLITS = [
    ("MAPD", "Stars"),
    ("MAPD", "Both"),
    ("S5884", "Stars"),
    ("S5552", "Stars"),
]

PDP_CONTRACTS = ["S5884", "S5552"]


def is_mapd_contract(df: pd.DataFrame, contract_col: str) -> pd.Series:
    return ~df[contract_col].isin(PDP_CONTRACTS)


def split_df_by_contract(
    df: pd.DataFrame, contract_col: str, contract_splits: List[Tuple] = None
) -> Dict[str, pd.DataFrame]:
    splits = {}

    if not contract_splits:
        contract_splits = CONTRACT_SPLITS

    seen_contracts = []

    for contract_group, star_indicator in contract_splits:
        if star_indicator == "All":
            star_filter = pd.Series(True, index=df.index)
        else:
            star_filter = (
                df[["Stars_Nonstars_1", "Stars_Nonstars_2", "Stars_Nonstars_3"]]
                == star_indicator
            ).any(axis=1)

        if contract_group == "MAPD":
            contract_filter = is_mapd_contract(df, contract_col=contract_col)

        else:
            contract_filter = df[contract_col] == contract_group

        final_filter = star_filter & contract_filter

        splits[f"{contract_group}:{star_indicator}"] = df[final_filter].reset_index(
            drop=True
        )

        seen_contracts.append(contract_group)

    splits["MW_ALL:All"] = df

    return splits


def get_leads_count_by_file_date(df: pd.DataFrame, disposition: str) -> pd.DataFrame:
    """
    Gets count of column 'disposition' grouped by 'File Date' column

    'disposition' column should be boolean True/False
    """

    return df[df[disposition]].groupby("File Date").size().reset_index(name=disposition)


def merge_lead_count_dfs(dfs: List[pd.DataFrame]) -> pd.DataFrame:
    return reduce(
        lambda left, right: pd.merge(left, right, on="File Date", how="outer"), dfs
    ).fillna(0)


def create_attempt_counts_df(df: pd.DataFrame) -> pd.DataFrame:
    attempt_counts = dict()

    for date in df["File Date"].unique():
        attempt_counts[date] = {}
        attempt_counts[date]["Attempted Leads Count"] = (
            df[df["File Date"] == date]["leadid"].value_counts() >= 1
        ).sum()

        attempt_counts[date]["2nd Attempt Count"] = (
            df[df["File Date"] == date]["leadid"].value_counts() >= 2
        ).sum()

    attempt_counts_df = (
        pd.DataFrame.from_dict(attempt_counts, orient="index")
        .fillna(0)
        .reset_index()
        .rename(columns={"index": "File Date"})
    )

    return attempt_counts_df


def create_was_reached_col(df: pd.DataFrame) -> pd.Series:
    """
    Checks if any disposition across call status cols is equal to any dispo in reached_dispositions

    returns boolean pd.Series
    """

    reached_dispositions = [
        "Refill Submitted",
        "Reminded - Refilled on own",
        "Out of Refill - Refill Request",
        "Already Ordered Refill",
        "Member Does Not Want Refill",
        "Member Stopped Medication",
        "Member No Longer With Plan",
        "Provider Changed Dose",
        "Provider Changed Medication",
        "Provider Discontinued Medicine",
        "Pharmacy Change",
        "Deceased",
        "Declined",
        "DNC",
    ]

    was_reached = (
        df[["call_status_1", "call_status_2", "call_status_3"]]
        .apply(lambda x: x.isin(reached_dispositions), axis=1)
        .any(axis=1)
    )

    return was_reached


def sort_by_file_date(df: pd.DataFrame) -> pd.DataFrame:
    sort_col = "File Date"

    sort_col_dt = sort_col + "_dt"

    df["File Date_dt"] = pd.to_datetime(df["File Date"])

    df = df.sort_values(by="File Date_dt").reset_index(drop=True)

    df = df.drop(columns=["File Date_dt"])

    return df


def filter_etl_data_for_rows_with_star_indicator(
    etl_data: pd.DataFrame, star_indicator: Literal["Stars", "Nonstars", "Both"]
):
    filter = (
        etl_data[["Stars_Nonstars_1", "Stars_Nonstars_2", "Stars_Nonstars_3"]]
        == star_indicator
    ).any(axis=1)

    filtered = etl_data[filter].reset_index(drop=True)

    return filtered


def perform_weekly_calculations(
    original_df,
    scrubbed_df,
    combined_report_df: pd.DataFrame,
):
    lead_contact_date_map = original_df.copy().drop_duplicates(
        subset=["leadid", "File Date"]
    )[["leadid", "File Date"]]

    lead_contact_date_dict = pd.Series(
        lead_contact_date_map["File Date"].values,
        index=lead_contact_date_map["leadid"],
    ).to_dict()

    combined_report_df["Lead Index Date"] = combined_report_df["leadid"].map(
        lead_contact_date_dict
    )

    date_regex = re.compile(
        r"(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}.\d{3})(\d{3}-\d{2}:\d{2})?"
    )

    combined_report_df["contact_attempt_date_time"] = (
        combined_report_df["contact_attempt_date_time"]
        .str.extract(date_regex)
        .loc[:, 0]
    )

    combined_report_df["contact_attempt_date_time"] = pd.to_datetime(
        combined_report_df["contact_attempt_date_time"]
    )

    contacted_on_right_day_bool = (
        pd.to_datetime(combined_report_df["Lead Index Date"], format="mixed").dt.date
        == combined_report_df["contact_attempt_date_time"].dt.date
    )

    contacted_on_right_day = combined_report_df[contacted_on_right_day_bool]

    # Leads received
    leads_received = original_df.groupby("File Date").size().reset_index()

    leads_received.columns = ["File Date", "Leads Received"]

    # Leads available for Outreach Count

    leads_available_for_outreach = (
        scrubbed_df.groupby("File Date")
        .size()
        .reset_index(name="Leads Available for Outreach Count")
    )

    # Attempted Leads Count

    # If leadid appears at least once, increment for day

    # Available for 2nd Attempt
    #   - look at call_status_1
    #   - Check if any of these rows showed up in call_status_1

    is_available_for_second_attempt = [
        "No Answer",
        "Left VM",
        "Audio/Connectivity Issues",
        "Other-Unable to Complete",
        "Member Unavailable",
    ]

    attempt_counts_df = create_attempt_counts_df(df=contacted_on_right_day)

    where_first_contact = contacted_on_right_day.groupby(["leadid", "File Date"])[
        "contact_attempt_date_time"
    ].idxmin()

    first_disposition = contacted_on_right_day.loc[where_first_contact]

    first_disposition["Available for 2nd Attempts"] = (
        first_disposition[["call_status_1", "call_status_2", "call_status_3"]]
        .apply(lambda x: x.isin(is_available_for_second_attempt), axis=1)
        .any(axis=1)
    )

    available_for_second_attempt_counts = first_disposition.groupby("File Date")[
        "Available for 2nd Attempts"
    ].sum()

    attempt_counts_df_with_second_attempts = attempt_counts_df.merge(
        available_for_second_attempt_counts, on="File Date", how="left"
    )

    attempted_counts_with_rates = leads_available_for_outreach.merge(
        attempt_counts_df_with_second_attempts,
        left_on="File Date",
        right_on="File Date",
        how="outer",
    ).reset_index(drop=True)

    for count_col in [
        "Attempted Leads Count",
        "2nd Attempt Count",
        "Available for 2nd Attempts",
    ]:
        attempted_counts_with_rates[count_col] = attempted_counts_with_rates[
            count_col
        ].fillna(0)

    attempted_counts_with_rates["Attempt Rate"] = (
        attempted_counts_with_rates["Attempted Leads Count"]
        / attempted_counts_with_rates["Leads Available for Outreach Count"]
    )

    attempted_counts_with_rates["2nd Attempt Rate"] = (
        attempted_counts_with_rates["2nd Attempt Count"]
        / attempted_counts_with_rates["Leads Available for Outreach Count"]
    )

    combined_report_df["Was Reached"] = create_was_reached_col(combined_report_df)

    reached_leads_count = get_leads_count_by_file_date(
        combined_report_df, "Was Reached"
    ).rename(columns={"Was Reached": "Reached Leads Count"})

    refill_dispositions = [
        "Refill Submitted",
        "Out of Refill - Refill Request",
        "Reminded - Refilled on own",
    ]

    for col in refill_dispositions:
        combined_report_df[col] = (
            combined_report_df[["call_status_1", "call_status_2", "call_status_3"]]
            .apply(lambda x: x.isin([col]), axis=1)
            .any(axis=1)
        )

    refill_submitted_leads_count = get_leads_count_by_file_date(
        combined_report_df, "Refill Submitted"
    )

    refill_request_leads_count = get_leads_count_by_file_date(
        combined_report_df, "Out of Refill - Refill Request"
    )

    refilled_on_own_leads_count = get_leads_count_by_file_date(
        combined_report_df, "Reminded - Refilled on own"
    )

    merged_refill_leads_counts = merge_lead_count_dfs(
        [
            refill_submitted_leads_count,
            refill_request_leads_count,
            refilled_on_own_leads_count,
        ]
    )

    attempted_counts_with_reached = attempted_counts_with_rates.merge(
        reached_leads_count, on="File Date", how="left"
    ).merge(merged_refill_leads_counts, on="File Date", how="left")

    # if we didnt reac any leads for a particular group, there wont be a row for that day,
    # and so when we merge it, the value will be NaN. We have to do this with a few other
    # metrics too since some of the groups are so small that we dont reach anyone in the
    # group for that day

    attempted_counts_with_reached["Reached Leads Count"] = (
        attempted_counts_with_reached["Reached Leads Count"].fillna(0)
    )

    for dispo in refill_dispositions:
        attempted_counts_with_reached[dispo] = attempted_counts_with_reached[
            dispo
        ].fillna(0)

    attempted_counts_with_reached["Reach Rate %"] = (
        attempted_counts_with_reached["Reached Leads Count"]
        / attempted_counts_with_reached["Attempted Leads Count"]
    ).fillna(0)

    attempted_counts_with_reached["Refill of Reached Rate %"] = (
        (
            attempted_counts_with_reached["Refill Submitted"]
            + attempted_counts_with_reached["Reminded - Refilled on own"]
        )
        / attempted_counts_with_reached["Reached Leads Count"]
    ).fillna(0)

    attempted_counts_with_reached['Refill of "Refill Submitted" Rate %'] = (
        attempted_counts_with_reached["Refill Submitted"]
        / attempted_counts_with_reached["Reached Leads Count"]
    ).fillna(0)

    # merge Leads Received col into Attempted Counts df and keep only the dates within this week
    weekly_report_df = leads_received.merge(
        attempted_counts_with_reached, on="File Date", how="left"
    )

    weekly_report_df["Scrub Rate"] = (
        weekly_report_df["Leads Received"]
        - weekly_report_df["Leads Available for Outreach Count"]
    ) / weekly_report_df["Leads Received"]

    weekly_report_df["Refill of Attempted Rate"] = (
        (
            weekly_report_df["Refill Submitted"]
            + weekly_report_df["Reminded - Refilled on own"]
        )
        / weekly_report_df["Attempted Leads Count"]
    ).fillna(0)  # need to fill na in case we dont attempt any leads for that day

    weekly_report_df["Refill Submitted of Attempted Rate"] = (
        weekly_report_df["Refill Submitted"] / weekly_report_df["Attempted Leads Count"]
    ).fillna(0)  # need to fill na in case we dont attempt any leads for that day

    weekly_report_df = sort_by_file_date(weekly_report_df)

    cols_to_round = [
        "Attempt Rate",
        "2nd Attempt Rate",
        "Reach Rate %",
        "Refill of Reached Rate %",
        'Refill of "Refill Submitted" Rate %',
        "Scrub Rate",
        "Refill of Attempted Rate",
        "Refill Submitted of Attempted Rate",
    ]

    for col in cols_to_round:
        weekly_report_df[col] = (weekly_report_df[col] * 100).round(1)

    weekly_report_df = weekly_report_df.rename(
        columns={
            "File Date": "Contact Date",
            "Out of Refill - Refill Request": "Refill Request",
        }
    )

    return weekly_report_df[FINAL_REPORT_COLS]


def create_weekly_refill_report(
    original_folder: Path,
    etl_folder: Path,
    report_folder: Path,
    current_date: datetime,
    split_by_contract: bool = True,
    ytd: bool = False,
    contract_splits: List[Tuple] = None,
):
    date_range = get_dates_for_weekly_report(current_date)

    start_date, end_date = date_range[0], date_range[-1]

    current_year = current_date.year

    logger.info(
        f"Starting file search from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
    )

    """
    All columns up to Reached Leads Count are only calculated from leads this week

    All columns after are from any leads
    """

    original_file_pattern = "Emme_MW0001_Med_Adher_*.csv"

    all_original_files = list(original_folder.glob(original_file_pattern))

    original_file_dates = [
        datetime.strptime(
            file.name, original_file_pattern.replace("*", REFILL_DATE_FMT)
        )
        for file in all_original_files
    ]

    all_original_files = [
        all_original_files[i]
        for i in range(len(all_original_files))
        if original_file_dates[i].year == current_year
    ]

    original_file_dates = [
        item for item in original_file_dates if item.year == current_year
    ]

    all_original_dfs = [read_df_from_csv(file) for file in all_original_files]

    for i in range(len(all_original_dfs)):
        all_original_dfs[i]["File Date"] = original_file_dates[i].strftime("%m/%d/%Y")

    combined_original_df = pd.concat(all_original_dfs).reset_index(drop=True)

    combined_original_df["File Date_dt"] = pd.to_datetime(
        combined_original_df["File Date"]
    )

    combined_original_df = combined_original_df[combined_original_df["H"] != "F"]

    if ytd:
        etl_folder_list = list(etl_folder.glob("*"))
        report_folder_list = list(report_folder.glob("*"))

        this_weeks_originals = combined_original_df.copy()

    else:
        is_from_this_week = (
            combined_original_df["File Date_dt"].dt.date >= start_date
        ) & (combined_original_df["File Date_dt"].dt.date <= end_date)

        etl_folder_list = collect_weekly_folders(
            base_dir=etl_folder, rundate=current_date
        )
        report_folder_list = collect_weekly_folders(report_folder, rundate=current_date)

        this_weeks_originals = combined_original_df.copy()[is_from_this_week]

    combined_report_files = combine_files_for_weekly_report(
        report_folder_list,
        file_pattern="Humana-MW-RefillConcierge-CallDispositions-*.csv",
        date_pattern="%Y%m%d%H",
    )

    combined_etl_files = combine_files_for_weekly_report(
        etl_folder_list,
        file_pattern="*_original_refill_data_scrubbed.csv",
        date_pattern="%Y-%m-%d",
    ).drop_duplicates(subset=["File Date", "leadid"])

    report_dict = {}

    contract_splits = split_df_by_contract(
        df=this_weeks_originals,
        contract_col="MCO_Contract_Number",
        contract_splits=contract_splits,
    )

    for identifier in contract_splits:
        contract, star_indicator = identifier.split(":")

        identifier = f"{contract}:{star_indicator}"

        logger.info(f"Generating calculations for {identifier}")

        original_subset = contract_splits[identifier].reset_index(drop=True)

        if len(original_subset) == 0:
            continue

        relevant_report_files = combined_report_files[
            combined_report_files["idcard_mbr_id"].isin(
                original_subset["RecipientMemberCardId"]
                + original_subset["RecipientMemberDependentCode"]
            )
        ].reset_index(drop=True)

        relevant_etl_files = combined_etl_files[
            combined_etl_files["idcard_mbr_id"].isin(
                original_subset["RecipientMemberCardId"]
                + original_subset["RecipientMemberDependentCode"]
            )
        ].reset_index(drop=True)

        # filter files even further for matching star indicator

        if star_indicator != "All":
            original_subset = filter_etl_data_for_rows_with_star_indicator(
                etl_data=original_subset, star_indicator=star_indicator
            )

            relevant_etl_files = filter_etl_data_for_rows_with_star_indicator(
                etl_data=relevant_etl_files, star_indicator=star_indicator
            )

        if (len(original_subset) == 0) or (len(relevant_etl_files) == 0):
            logger.warning(
                f"No records found to report on for {contract}:{star_indicator} pair. Skipping"
            )

            continue

        current_weekly_report = perform_weekly_calculations(
            original_df=original_subset,
            scrubbed_df=relevant_etl_files,
            combined_report_df=relevant_report_files,
        )

        report_dict[f"{contract}:{star_indicator}"] = current_weekly_report

    return report_dict


def process_combined_weekly_reports(df: pd.DataFrame) -> pd.DataFrame:
    df["Leads Received"] = df["Leads Received"].astype(int)

    df["Leads Available for Outreach Count"] = (
        df["Leads Available for Outreach Count"].astype(float).fillna(0).astype(int)
    )

    df["Scrub Rate"] = df["Scrub Rate"].astype(float)

    df["Refill Submitted"] = df["Refill Submitted"].astype(float).fillna(0).astype(int)

    df["Reminded - Refilled on own"] = (
        df["Reminded - Refilled on own"].astype(float).fillna(0).astype(int)
    )

    df["Attempted Leads Count"] = (
        df["Attempted Leads Count"].astype(float).fillna(0).astype(int)
    )

    df["Refill of Attempted Rate"] = df["Refill of Attempted Rate"].astype(float)

    df["Reached Leads Count"] = (
        df["Reached Leads Count"].astype(float).fillna(0).astype(int)
    )

    df["Refill Submitted of Attempted Rate"] = df[
        "Refill Submitted of Attempted Rate"
    ].astype(float)

    missing_scrub_rate = df["Scrub Rate"].isna()

    df.loc[missing_scrub_rate, "Scrub Rate"] = (
        df.loc[missing_scrub_rate, "Leads Received"]
        - df.loc[missing_scrub_rate, "Leads Available for Outreach Count"]
    ) / df.loc[missing_scrub_rate, "Leads Received"]

    df.loc[missing_scrub_rate, "Scrub Rate"] = (
        df.loc[missing_scrub_rate, "Scrub Rate"] * 100
    ).round(1)

    missing_refill_of_attempted_rate = df["Refill of Attempted Rate"].isna()

    df.loc[missing_refill_of_attempted_rate, "Refill of Attempted Rate"] = (
        df.loc[missing_refill_of_attempted_rate, "Refill Submitted"]
        + df.loc[missing_refill_of_attempted_rate, "Reminded - Refilled on own"]
    ) / df.loc[missing_refill_of_attempted_rate, "Attempted Leads Count"]

    df.loc[missing_refill_of_attempted_rate, "Refill of Attempted Rate"] = (
        df.loc[missing_refill_of_attempted_rate, "Refill of Attempted Rate"] * 100
    ).round(1)

    missing_refill_submitted_of_attempted_rate = df[
        "Refill Submitted of Attempted Rate"
    ].isna()

    df.loc[
        missing_refill_submitted_of_attempted_rate, "Refill Submitted of Attempted Rate"
    ] = (
        df.loc[missing_refill_submitted_of_attempted_rate, "Refill Submitted"]
        / df.loc[missing_refill_submitted_of_attempted_rate, "Attempted Leads Count"]
    )

    df.loc[
        missing_refill_submitted_of_attempted_rate, "Refill Submitted of Attempted Rate"
    ] = (
        df.loc[
            missing_refill_submitted_of_attempted_rate,
            "Refill Submitted of Attempted Rate",
        ]
        * 100
    ).round(1)

    cols_to_round = [
        "Scrub Rate",
        "Refill of Attempted Rate",
        "Refill Submitted of Attempted Rate",
    ]

    # for col in cols_to_round:
    #    df[col] = (df[col] * 100).round(1)

    date_dt = pd.to_datetime(df["Contact Date"])

    last_date = date_dt.max().strftime("%Y-%m-%d")

    sorted_dates = date_dt.sort_values()

    df_sorted = df.loc[sorted_dates.index].reset_index(drop=True)

    return df_sorted
