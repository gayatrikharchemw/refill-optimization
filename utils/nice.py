import logging
from datetime import datetime
from typing import List

from niceclient.niceclient import NiceClient

logger = logging.getLogger(__name__)

API_VERSION = "v31.0"
DEFAULT_HOST = "https://api-c38.nice-incontact.com/"
DNC_GROUPS_ENDPOINT = f"incontactapi/services/{API_VERSION}/dnc-groups"
CHUNK_SIZE = 500

GROUP_IDS = ["81", "46"]


def get_dnc_records(
    client: NiceClient,
    dnc_group_id: str,
    endpoint=f"{DNC_GROUPS_ENDPOINT}",
    date_collected_cutoff: datetime | None = None,
) -> List:
    logger.info(f"Getting DNC records from {endpoint}")

    headers = {
        "Authorization": client.authorization,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    params = {
        "top": CHUNK_SIZE,
        "skip": 0,
    }

    response = client.get(
        endpoint=f"{endpoint}/{dnc_group_id}/records",
        headers=headers,
        params=params,
        allowed_responses=[200, 204],
    )

    if response.status_code == 204:
        return []

    total_records = response.json()["dncList"]["totalRecords"]
    data = response.json()["dncList"]["dncRecords"]

    while int(total_records) <= CHUNK_SIZE or (response.status_code != 204):
        params["skip"] += CHUNK_SIZE
        response = client.get(
            endpoint=f"{endpoint}/{dnc_group_id}/records",
            headers=headers,
            params=params,
            allowed_responses=[200, 204],
        )

        if response.status_code == 204:
            break

        else:
            data += response.json()["dncList"]["dncRecords"]

    logger.info(f"Got {len(data)} DNC records for group {dnc_group_id}")

    # this is coming from needing to have a snapshot of who was on the dnc list at the time of scrubbing

    if date_collected_cutoff:
        data = [
            item
            for item in data
            if datetime.fromisoformat(item["dateCollected"]) <= date_collected_cutoff
        ]

    numbers = [item["phoneNumber"] for item in data]

    cleaned_numbers = [number.lstrip("+1") for number in numbers]

    return cleaned_numbers
