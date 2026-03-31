
import json
import logging
import re
from datetime import datetime, timedelta
from io import StringIO
from pathlib import Path
from typing import Optional, Tuple
from zoneinfo import ZoneInfo

import pandas as pd
from cmappclient import CMAPPClient
from cmappclient.reports.report_generation import ReportGeneration
from cmappmongo.case_registries import get_case_data
from cmappmongo.members import get_uploaded_member_info
from pymongo.database import Database
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

import utils.fields as fields

logger = logging.getLogger(__name__)

def get_skill_metrics(
    engine: Engine,
    skill: list[str],
    start_datetime: datetime,
    end_datetime: datetime,
    year: int = 2026,
) -> dict:
    """
    Get pickup rate, conversion rate, completion rate, and abandon rate for a specific skill and date range.

    Args:
        engine: SQLAlchemy engine
        skill: Skill names to filter by
        start_datetime: Start of date range (timezone-aware)
        end_datetime: End of date range (timezone-aware)
        year: Data year — determines which table/columns to query (2025 or 2026)

    Returns:
        Dictionary with pickup rate, conversion rate, completion rate, and abandon rate metrics
    """
    if start_datetime.tzinfo is None or end_datetime.tzinfo is None:
        raise ValueError("start_datetime and end_datetime must be timezone-aware")

    start_datetime = start_datetime.astimezone(ZoneInfo("UTC"))
    end_datetime = end_datetime.astimezone(ZoneInfo("UTC"))

    not_dialed_dispos = [
        dispo
        for dispo in fields.SYSTEM_DISPOSITION_MAPPING.values()
        if fields.SYSTEM_DISPOSITION_CATEGORY_MAPPING.get(dispo, "")
        in ["Error / Failure", "Suppressed"]
    ]

    answered_dispos = fields.ANSWERED_DISPOSITIONS
    converted_dispos = fields.CONVERTED_DISPOSITIONS

    logger.info(
        f"Fetching metrics for skill '{skill}' from {start_datetime} to {end_datetime}"
    )

    if year == 2025:
        query = text(
            """
            SELECT
                COUNT(*) FILTER (WHERE disposition != ALL(:not_dialed_dispos)) AS total_calls,
                COUNT(*) FILTER (WHERE disposition = ANY(:answered_dispos)) AS answered_calls,
                COUNT(*) FILTER (WHERE disposition = ANY(:converted_dispos)) AS converted_calls,
                COUNT(*) FILTER (WHERE disposition = 'Completed') AS completed_calls,
                COUNT(*) FILTER (WHERE disposition = 'Abandon') AS abandoned_calls
            FROM crm_2025.call_logs
            WHERE timestamp BETWEEN :start AND :end
                AND skill = ANY(:skill)
                AND NOT (disposition = 'UNKNOWN' AND abandoned = false)
            """
        )
    else:
        query = text(
            """
            SELECT
                COUNT(*) FILTER (WHERE dialer_disposition != ALL(:not_dialed_dispos)) AS total_calls,
                COUNT(*) FILTER (WHERE dialer_disposition = ANY(:answered_dispos)) AS answered_calls,
                COUNT(*) FILTER (WHERE dialer_disposition = ANY(:converted_dispos)) AS converted_calls,
                COUNT(*) FILTER (WHERE dialer_disposition = 'Completed') AS completed_calls,
                COUNT(*) FILTER (WHERE dialer_disposition = 'Abandon') AS abandoned_calls
            FROM call_logs
            WHERE timestamp BETWEEN :start AND :end
                AND skill_name = ANY(:skill)
                AND NOT (dialer_disposition = 'UNKNOWN' AND abandoned = false)
            """
        )

    with Session(engine) as session:
        result = session.execute(
            query,
            {
                "start": start_datetime,
                "end": end_datetime,
                "skill": skill,
                "not_dialed_dispos": list(not_dialed_dispos),
                "answered_dispos": list(answered_dispos),
                "converted_dispos": list(converted_dispos),
            },
        )
        row = result.fetchone()

    total_calls = row.total_calls
    answered_calls = row.answered_calls
    converted_calls = row.converted_calls
    completed_calls = row.completed_calls
    abandoned_calls = row.abandoned_calls

    pickup_rate = (answered_calls / total_calls * 100) if total_calls > 0 else 0
    conversion_rate = (converted_calls / answered_calls * 100) if answered_calls > 0 else 0
    completion_rate = (completed_calls / total_calls * 100) if total_calls > 0 else 0
    abandon_rate = (abandoned_calls / total_calls * 100) if total_calls > 0 else 0

    return {
        "skill": skill,
        "start_datetime": start_datetime,
        "end_datetime": end_datetime,
        "total_calls": total_calls,
        "answered_calls": answered_calls,
        "converted_calls": converted_calls,
        "abandoned_calls": abandoned_calls,
        "pickup_rate": pickup_rate,
        "conversion_rate": conversion_rate,
        "completion_rate": completion_rate,
        "abandon_rate": abandon_rate,
    }


def get_pickup_rate_by_destination(
    engine: Engine,
    start_datetime: datetime,
    end_datetime: datetime,
    skill: list[str] | None = None,
    year: int = 2026,
) -> pd.DataFrame:
    """
    Get pickup rate per destination for a given time period.

    Args:
        engine: SQLAlchemy engine
        start_datetime: Start of date range (timezone-aware)
        end_datetime: End of date range (timezone-aware)
        skill: Optional list of skill names to filter by
        year: Data year — determines which table/columns to query (2025 or 2026)

    Returns:
        DataFrame with columns: destination, total_calls, answered_calls, pickup_rate
    """
    if start_datetime.tzinfo is None or end_datetime.tzinfo is None:
        raise ValueError("start_datetime and end_datetime must be timezone-aware")

    start_datetime = start_datetime.astimezone(ZoneInfo("UTC"))
    end_datetime = end_datetime.astimezone(ZoneInfo("UTC"))

    not_dialed_dispos = [
        dispo
        for dispo in fields.SYSTEM_DISPOSITION_MAPPING.values()
        if fields.SYSTEM_DISPOSITION_CATEGORY_MAPPING.get(dispo, "")
        in ["Error / Failure", "Suppressed"]
    ]
    answered_dispos = fields.ANSWERED_DISPOSITIONS

    params = {
        "start": start_datetime,
        "end": end_datetime,
        "not_dialed_dispos": list(not_dialed_dispos),
        "answered_dispos": list(answered_dispos),
    }

    if year == 2025:
        if skill:
            query = text(
                """
                SELECT
                    destination,
                    COUNT(*) FILTER (WHERE disposition != ALL(:not_dialed_dispos)) AS total_calls,
                    COUNT(*) FILTER (WHERE disposition = ANY(:answered_dispos)) AS answered_calls
                FROM crm_2025.call_logs
                WHERE timestamp BETWEEN :start AND :end
                    AND NOT (disposition = 'UNKNOWN' AND abandoned = false)
                    AND skill = ANY(:skill)
                GROUP BY destination
                """
            )
        else:
            query = text(
                """
                SELECT
                    destination,
                    COUNT(*) FILTER (WHERE disposition != ALL(:not_dialed_dispos)) AS total_calls,
                    COUNT(*) FILTER (WHERE disposition = ANY(:answered_dispos)) AS answered_calls
                FROM crm_2025.call_logs
                WHERE timestamp BETWEEN :start AND :end
                    AND NOT (disposition = 'UNKNOWN' AND abandoned = false)
                GROUP BY destination
                """
            )
    else:
        if skill:
            query = text(
                """
                SELECT
                    destination,
                    COUNT(*) FILTER (WHERE dialer_disposition != ALL(:not_dialed_dispos)) AS total_calls,
                    COUNT(*) FILTER (WHERE dialer_disposition = ANY(:answered_dispos)) AS answered_calls
                FROM call_logs
                WHERE timestamp BETWEEN :start AND :end
                    AND NOT (dialer_disposition = 'UNKNOWN' AND abandoned = false)
                    AND skill_name = ANY(:skill)
                GROUP BY destination
                """
            )
        else:
            query = text(
                """
                SELECT
                    destination,
                    COUNT(*) FILTER (WHERE dialer_disposition != ALL(:not_dialed_dispos)) AS total_calls,
                    COUNT(*) FILTER (WHERE dialer_disposition = ANY(:answered_dispos)) AS answered_calls
                FROM call_logs
                WHERE timestamp BETWEEN :start AND :end
                    AND NOT (dialer_disposition = 'UNKNOWN' AND abandoned = false)
                GROUP BY destination
                """
            )

    if skill:
        params["skill"] = skill

    with Session(engine) as session:
        result = session.execute(query, params)
        rows = result.fetchall()

    df = pd.DataFrame(rows, columns=["destination", "total_calls", "answered_calls"])
    df["pickup_rate"] = (df["answered_calls"] / df["total_calls"] * 100).where(df["total_calls"] > 0, 0).round(2)

    return df.sort_values("pickup_rate", ascending=False).reset_index(drop=True)


def get_outbound_stats_both_years(
    engine: Engine,
    start_2026: datetime,
    end_2026: datetime,
    start_2025: datetime,
    end_2025: datetime,
    skills_2026: list[str],
    skills_2025: list[str],
    min_attempts: int = 3,
    never_answered_only: bool = False,
) -> pd.DataFrame:
    """
    Single cross-year query: inner joins 2026 and 2025 outbound call_logs so only
    phone numbers present in both years are returned. min_attempts is applied as a
    HAVING clause on 2026 attempts before the join, keeping the result set small.

    Args:
        never_answered_only: If True, only return numbers with 0 pickups in 2026.

    Returns DataFrame with columns:
        member_phone, attempts_2026, pickups_2026, pickup_rate_2026,
        attempts_2025, pickups_2025, pickup_rate_2025
    """
    for dt in (start_2026, end_2026, start_2025, end_2025):
        if dt.tzinfo is None:
            raise ValueError("All datetimes must be timezone-aware")

    not_dialed_dispos = [
        dispo
        for dispo in fields.SYSTEM_DISPOSITION_MAPPING.values()
        if fields.SYSTEM_DISPOSITION_CATEGORY_MAPPING.get(dispo, "") in ["Error / Failure", "Suppressed"]
    ]
    answered_dispos = fields.ANSWERED_DISPOSITIONS

    if never_answered_only:
        query = text("""
            WITH ob_2026 AS (
                SELECT
                    destination AS member_phone,
                    COUNT(*) FILTER (WHERE dialer_disposition != ALL(:not_dialed_dispos)) AS attempts_2026,
                    COUNT(*) FILTER (WHERE dialer_disposition = ANY(:answered_dispos))    AS pickups_2026
                FROM call_logs
                WHERE timestamp BETWEEN :start_2026 AND :end_2026
                    AND skill_name = ANY(:skills_2026)
                GROUP BY destination
                HAVING COUNT(*) FILTER (WHERE dialer_disposition != ALL(:not_dialed_dispos)) >= :min_attempts
                    AND COUNT(*) FILTER (WHERE dialer_disposition = ANY(:answered_dispos)) = 0
            ),
            ob_2025 AS (
                SELECT
                    dnis AS member_phone,
                    COUNT(*) FILTER (WHERE disposition != ALL(:not_dialed_dispos)) AS attempts_2025,
                    COUNT(*) FILTER (WHERE disposition = ANY(:answered_dispos))    AS pickups_2025
                FROM crm_2025.call_logs
                WHERE timestamp BETWEEN :start_2025 AND :end_2025
                    AND NOT (disposition = 'UNKNOWN' AND abandoned = false)
                    AND skill = ANY(:skills_2025)
                GROUP BY dnis
            )
            SELECT
                ob_2026.member_phone,
                ob_2026.attempts_2026,
                ob_2026.pickups_2026,
                ob_2025.attempts_2025,
                ob_2025.pickups_2025
            FROM ob_2026
            INNER JOIN ob_2025 ON ob_2026.member_phone = ob_2025.member_phone
        """)
    else:
        query = text("""
            WITH ob_2026 AS (
                SELECT
                    destination AS member_phone,
                    COUNT(*) FILTER (WHERE dialer_disposition != ALL(:not_dialed_dispos)) AS attempts_2026,
                    COUNT(*) FILTER (WHERE dialer_disposition = ANY(:answered_dispos))    AS pickups_2026
                FROM call_logs
                WHERE timestamp BETWEEN :start_2026 AND :end_2026
                    AND skill_name = ANY(:skills_2026)
                GROUP BY destination
                HAVING COUNT(*) FILTER (WHERE dialer_disposition != ALL(:not_dialed_dispos)) >= :min_attempts
            ),
            ob_2025 AS (
                SELECT
                    dnis AS member_phone,
                    COUNT(*) FILTER (WHERE disposition != ALL(:not_dialed_dispos)) AS attempts_2025,
                    COUNT(*) FILTER (WHERE disposition = ANY(:answered_dispos))    AS pickups_2025
                FROM crm_2025.call_logs
                WHERE timestamp BETWEEN :start_2025 AND :end_2025
                    AND NOT (disposition = 'UNKNOWN' AND abandoned = false)
                    AND skill = ANY(:skills_2025)
                GROUP BY dnis
            )
            SELECT
                ob_2026.member_phone,
                ob_2026.attempts_2026,
                ob_2026.pickups_2026,
                ob_2025.attempts_2025,
                ob_2025.pickups_2025
            FROM ob_2026
            INNER JOIN ob_2025 ON ob_2026.member_phone = ob_2025.member_phone
        """)

    with Session(engine) as session:
        rows = session.execute(query, {
            "start_2026": start_2026.astimezone(ZoneInfo("UTC")),
            "end_2026":   end_2026.astimezone(ZoneInfo("UTC")),
            "start_2025": start_2025.astimezone(ZoneInfo("UTC")),
            "end_2025":   end_2025.astimezone(ZoneInfo("UTC")),
            "skills_2026": skills_2026,
            "skills_2025": skills_2025,
            "not_dialed_dispos": list(not_dialed_dispos),
            "answered_dispos":   list(answered_dispos),
            "min_attempts": min_attempts,
        }).fetchall()

    df = pd.DataFrame(rows, columns=[
        "member_phone", "attempts_2026", "pickups_2026", "attempts_2025", "pickups_2025"
    ])
    df["pickup_rate_2026"] = (df["pickups_2026"] / df["attempts_2026"] * 100).where(df["attempts_2026"] > 0, 0).round(2)
    df["pickup_rate_2025"] = (df["pickups_2025"] / df["attempts_2025"] * 100).where(df["attempts_2025"] > 0, 0).round(2)
    return df


def get_outbound_stats_by_number(
    engine: Engine,
    start_datetime: datetime,
    end_datetime: datetime,
    skills: list[str],
    year: int = 2026,
    min_attempts: int = 1,
    never_answered_only: bool = False,
) -> pd.DataFrame:
    """
    Get outbound call attempts and pickups grouped by member phone number.

    Member phone column differs by year and direction:
      - 2025 outbound: dnis
      - 2026 outbound: destination

    Args:
        min_attempts: Minimum number of attempts to include (applied as HAVING clause).
        never_answered_only: If True, only return numbers with 0 pickups.
    """
    if start_datetime.tzinfo is None or end_datetime.tzinfo is None:
        raise ValueError("start_datetime and end_datetime must be timezone-aware")

    start_datetime = start_datetime.astimezone(ZoneInfo("UTC"))
    end_datetime = end_datetime.astimezone(ZoneInfo("UTC"))

    not_dialed_dispos = [
        dispo
        for dispo in fields.SYSTEM_DISPOSITION_MAPPING.values()
        if fields.SYSTEM_DISPOSITION_CATEGORY_MAPPING.get(dispo, "") in ["Error / Failure", "Suppressed"]
    ]
    answered_dispos = fields.ANSWERED_DISPOSITIONS

    params = {
        "start": start_datetime,
        "end": end_datetime,
        "skills": skills,
        "not_dialed_dispos": list(not_dialed_dispos),
        "answered_dispos": list(answered_dispos),
        "min_attempts": min_attempts,
    }

    if year == 2025 and never_answered_only:
        query = text("""
            SELECT
                dnis AS member_phone,
                COUNT(*) FILTER (WHERE disposition != ALL(:not_dialed_dispos)) AS total_calls,
                COUNT(*) FILTER (WHERE disposition = ANY(:answered_dispos)) AS answered_calls
            FROM crm_2025.call_logs
            WHERE timestamp BETWEEN :start AND :end
                AND NOT (disposition = 'UNKNOWN' AND abandoned = false)
                AND skill = ANY(:skills)
            GROUP BY dnis
            HAVING COUNT(*) FILTER (WHERE disposition != ALL(:not_dialed_dispos)) >= :min_attempts
                AND COUNT(*) FILTER (WHERE disposition = ANY(:answered_dispos)) = 0
        """)
    elif year == 2025:
        query = text("""
            SELECT
                dnis AS member_phone,
                COUNT(*) FILTER (WHERE disposition != ALL(:not_dialed_dispos)) AS total_calls,
                COUNT(*) FILTER (WHERE disposition = ANY(:answered_dispos)) AS answered_calls
            FROM crm_2025.call_logs
            WHERE timestamp BETWEEN :start AND :end
                AND NOT (disposition = 'UNKNOWN' AND abandoned = false)
                AND skill = ANY(:skills)
            GROUP BY dnis
            HAVING COUNT(*) FILTER (WHERE disposition != ALL(:not_dialed_dispos)) >= :min_attempts
        """)
    elif never_answered_only:
        query = text("""
            SELECT
                destination AS member_phone,
                COUNT(*) FILTER (WHERE dialer_disposition != ALL(:not_dialed_dispos)) AS total_calls,
                COUNT(*) FILTER (WHERE dialer_disposition = ANY(:answered_dispos)) AS answered_calls
            FROM call_logs
            WHERE timestamp BETWEEN :start AND :end
                AND NOT (dialer_disposition = 'UNKNOWN' AND abandoned = false)
                AND skill_name = ANY(:skills)
            GROUP BY destination
            HAVING COUNT(*) FILTER (WHERE dialer_disposition != ALL(:not_dialed_dispos)) >= :min_attempts
                AND COUNT(*) FILTER (WHERE dialer_disposition = ANY(:answered_dispos)) = 0
        """)
    else:
        query = text("""
            SELECT
                destination AS member_phone,
                COUNT(*) FILTER (WHERE dialer_disposition != ALL(:not_dialed_dispos)) AS total_calls,
                COUNT(*) FILTER (WHERE dialer_disposition = ANY(:answered_dispos)) AS answered_calls
            FROM call_logs
            WHERE timestamp BETWEEN :start AND :end
                AND NOT (dialer_disposition = 'UNKNOWN' AND abandoned = false)
                AND skill_name = ANY(:skills)
            GROUP BY destination
            HAVING COUNT(*) FILTER (WHERE dialer_disposition != ALL(:not_dialed_dispos)) >= :min_attempts
        """)

    with Session(engine) as session:
        rows = session.execute(query, params).fetchall()

    df = pd.DataFrame(rows, columns=["member_phone", "total_calls", "answered_calls"])
    df["pickup_rate"] = (df["answered_calls"] / df["total_calls"] * 100).where(df["total_calls"] > 0, 0).round(2)
    return df


def get_inbound_callbacks_by_number(
    engine: Engine,
    start_datetime: datetime,
    end_datetime: datetime,
    skills: list[str],
    year: int = 2026,
    numbers: list[str] | None = None,
) -> pd.DataFrame:
    """
    Get inbound callback counts grouped by member phone number.

    Member phone column differs by year and direction:
      - 2025 inbound: ani
      - 2026 inbound: source

    Args:
        numbers: Optional list of phone numbers to filter to. Pass the outbound
                 result set to avoid fetching the entire inbound table.
    """
    if start_datetime.tzinfo is None or end_datetime.tzinfo is None:
        raise ValueError("start_datetime and end_datetime must be timezone-aware")

    start_datetime = start_datetime.astimezone(ZoneInfo("UTC"))
    end_datetime = end_datetime.astimezone(ZoneInfo("UTC"))

    params = {"start": start_datetime, "end": end_datetime, "skills": skills}

    if year == 2025:
        if numbers:
            params["numbers"] = numbers
            query = text("""
                SELECT
                    ani AS member_phone,
                    COUNT(*) AS inbound_calls
                FROM crm_2025.call_logs
                WHERE timestamp BETWEEN :start AND :end
                    AND skill = ANY(:skills)
                    AND ani = ANY(:numbers)
                GROUP BY ani
            """)
        else:
            query = text("""
                SELECT
                    ani AS member_phone,
                    COUNT(*) AS inbound_calls
                FROM crm_2025.call_logs
                WHERE timestamp BETWEEN :start AND :end
                    AND skill = ANY(:skills)
                GROUP BY ani
            """)
    else:
        if numbers:
            params["numbers"] = numbers
            query = text("""
                SELECT
                    source AS member_phone,
                    COUNT(*) AS inbound_calls
                FROM call_logs
                WHERE timestamp BETWEEN :start AND :end
                    AND skill_name = ANY(:skills)
                    AND source = ANY(:numbers)
                GROUP BY source
            """)
        else:
            query = text("""
                SELECT
                    source AS member_phone,
                    COUNT(*) AS inbound_calls
                FROM call_logs
                WHERE timestamp BETWEEN :start AND :end
                    AND skill_name = ANY(:skills)
                GROUP BY source
            """)

    with Session(engine) as session:
        rows = session.execute(query, params).fetchall()

    return pd.DataFrame(rows, columns=["member_phone", "inbound_calls"])


def get_never_answered_destinations(
    engine: Engine,
    destinations: list[str],
    year: int,
) -> pd.DataFrame:
    """
    Given a list of phone numbers, return those that were never answered in the given year.

    Args:
        engine: SQLAlchemy engine
        destinations: List of phone numbers to check
        year: 2025 or 2026

    Returns:
        DataFrame with columns: destination, total_calls, answered_calls
    """
    answered_dispos = fields.ANSWERED_DISPOSITIONS
    not_dialed_dispos = [
        dispo
        for dispo in fields.SYSTEM_DISPOSITION_MAPPING.values()
        if fields.SYSTEM_DISPOSITION_CATEGORY_MAPPING.get(dispo, "")
        in ["Error / Failure", "Suppressed"]
    ]

    params = {
        "destinations": destinations,
        "answered_dispos": list(answered_dispos),
        "not_dialed_dispos": list(not_dialed_dispos),
    }

    if year == 2025:
        query = text(
            """
            SELECT
                destination,
                COUNT(*) FILTER (WHERE disposition != ALL(:not_dialed_dispos)) AS total_calls,
                COUNT(*) FILTER (WHERE disposition = ANY(:answered_dispos)) AS answered_calls
            FROM crm_2025.call_logs
            WHERE destination = ANY(:destinations)
            GROUP BY destination
            HAVING COUNT(*) FILTER (WHERE disposition = ANY(:answered_dispos)) = 0
            """
        )
    else:
        query = text(
            """
            SELECT
                destination,
                COUNT(*) FILTER (WHERE dialer_disposition != ALL(:not_dialed_dispos)) AS total_calls,
                COUNT(*) FILTER (WHERE dialer_disposition = ANY(:answered_dispos)) AS answered_calls
            FROM call_logs
            WHERE destination = ANY(:destinations)
            GROUP BY destination
            HAVING COUNT(*) FILTER (WHERE dialer_disposition = ANY(:answered_dispos)) = 0
            """
        )

    with Session(engine) as session:
        rows = session.execute(query, params).fetchall()

    return pd.DataFrame(rows, columns=["destination", "total_calls", "answered_calls"])