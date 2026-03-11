import logging
from pathlib import Path
import argparse
from s3path import S3Path
from io import StringIO
from datetime import datetime
from typing import Optional, Tuple
from zoneinfo import ZoneInfo
import pandas as pd

logger = logging.getLogger(__name__)


def clean_ani(ani) -> str | None:
    """Normalize a phone number to a 10-digit string, or None if invalid."""
    if pd.isnull(ani):
        return None
    ani_str = str(ani).lstrip("+")
    if len(ani_str) > 10 and ani_str.startswith("1"):
        ani_str = ani_str[1:]
    if len(ani_str) == 10:
        return ani_str
    return None


def save_df_to_csv(df: pd.DataFrame, path: Path, overwrite: bool = True):
    if path.exists() and not overwrite:
        logger.warning(
            f"File {path} already exists and will not be overwritten. To overwrite, set 'overwrite=True'"
        )
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w") as fp:
            df.fillna("").to_csv(fp, index=False, lineterminator="\n")

def get_args(additional_args: list = None) -> argparse.Namespace:
    """
    Parse command line arguments.

    Args:
        additional_args: List of tuples with (arg_name, arg_kwargs) to add to parser
                        Example: [("--input-file", {"required": True, "help": "Input file"})]
    """
    parser = argparse.ArgumentParser(
        description="Analyze pickup rate for phone numbers"
    )
    parser.add_argument(
        "config",
        type=Path,
        help="Path to the configuration file",
    )
    parser.add_argument(
        "--start_datetime",
        type=datetime.fromisoformat,
        help="Start datetime for the data to be processed",
    )
    parser.add_argument(
        "--end_datetime",
        type=datetime.fromisoformat,
        help="End datetime for the data to be processed",
    )
    parser.add_argument(
        "--skill",
        type=str,
        help="Skill name",
    )

    if additional_args:
        for arg_name, arg_kwargs in additional_args:
            parser.add_argument(arg_name, **arg_kwargs)

    args = parser.parse_args()

    return args

def get_start_and_end_datetime(
    start_datetime: Optional[datetime] = None,
    end_datetime: Optional[datetime] = None,
) -> Tuple[datetime, datetime]:
    """
    Get the start and end datetime for the data to be processed.
    If start_datetime and end_datetime are not provided, use default values.
    """
    if start_datetime:
        if start_datetime.tzinfo is None:
            start_datetime = start_datetime.replace(
                tzinfo=ZoneInfo("America/Los_Angeles")
            )
        else:
            start_datetime = start_datetime.astimezone(ZoneInfo("America/Los_Angeles"))
    else:
        logger.info("Missing start datetime, using default value.")
        start_datetime = datetime.now(tz=ZoneInfo("America/Los_Angeles")).replace(
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )

    if end_datetime:
        if end_datetime.tzinfo is None:
            end_datetime = end_datetime.replace(tzinfo=ZoneInfo("America/Los_Angeles"))
        else:
            end_datetime = end_datetime.astimezone(ZoneInfo("America/Los_Angeles"))
    else:
        logger.info("Missing end datetime, using default value.")
        end_datetime = datetime.now(tz=ZoneInfo("America/Los_Angeles"))

    logger.info(f"Start datetime: {start_datetime}, End datetime: {end_datetime}")

    return start_datetime, end_datetime


def read_s3_csv(
    s3_file_path: S3Path,
    encoding: str = "iso-8859-1",
    **read_csv_kwargs,
) -> pd.DataFrame:
    """
    Read a CSV from S3 using the given encoding.
    Default read_csv_kwargs: dtype="string", keep_default_na=False.
    """
    defaults = {"dtype": "string", "keep_default_na": False}
    defaults.update(read_csv_kwargs)
    with s3_file_path.open("rb") as f:
        bytes_data = f.read()
        return pd.read_csv(
            StringIO(bytes_data.decode(encoding)),
            **defaults,
        )
