"""
Get pickup rate, completion rate, conversion rate, and abandon rate for a given skill name between start and end dates.

Usage:
    uv run scripts/metrics/get_skill_metrics.py configs/humana_cmr_shout_config.yml --skill "SKILL-NAME" --start_datetime "2025-01-01" --end_datetime "2025-01-09"
"""

import logging
from pathlib import Path

from utils.nice_utils import get_skill_metrics
from utils.config_utils import Config
from utils.file_utils import get_args, get_start_and_end_datetime

logger = logging.getLogger(__name__)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] - %(name)s: %(message)s"
    )

    args = get_args()
    config = Config(Path(args.config))
    start_datetime, end_datetime = get_start_and_end_datetime(
        start_datetime=args.start_datetime,
        end_datetime=args.end_datetime,
    )

    # Get skill from command line arguments
    if not args.skill:
        logger.error("Error: --skill argument is required")
        logger.info("Usage: uv run scripts/metrics/get_skill_metrics.py --config config.yaml --skill 'SKILL-NAME' --start_datetime '2025-01-01' --end_datetime '2025-01-09'")
        exit(1)

    skill = args.skill

    # Get database connection
    outreach_db_engine = config.get_outreach_db_engine()

    # Get metrics
    metrics = get_skill_metrics(
        outreach_db_engine,
        [skill],
        start_datetime,
        end_datetime,
    )

    # Print results
    print(f"\nSkill: {skill}")
    print(f"Date Range: {start_datetime.date()} to {end_datetime.date()}")
    print(f"Pickup Rate: {metrics['pickup_rate']:.2f}%")
    print(f"Conversion Rate: {metrics['conversion_rate']:.2f}%")
    print(f"Completion Rate: {metrics['completion_rate']:.2f}%")
    print(f"Abandon Rate: {metrics['abandon_rate']:.2f}%")
    print(metrics)
