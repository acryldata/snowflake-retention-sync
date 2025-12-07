#!/usr/bin/env python3
"""
Create Retention Period Structured Property in DataHub

This script creates the structured property definition for retention period data
in DataHub. Run this ONCE before using snowflake_retention_sync.py.

Usage:
    export DATAHUB_GMS_URL=https://your-instance.acryl.io
    export DATAHUB_TOKEN=your-token
    python create_retention_property.py

    Or with command-line arguments:
    python create_retention_property.py --datahub-url https://your-instance.acryl.io --datahub-token your-token
"""

import argparse
import logging
import os
import sys

from datahub.api.entities.structuredproperties.structuredproperties import StructuredProperties
from datahub.ingestion.graph.client import DataHubGraph, DataHubGraphConfig

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Structured property definition (must match RETENTION_PROPERTY_ID in snowflake_retention_sync.py)
PROPERTY_ID = "io.acryl.dataManagement.retentionPeriodDays"
PROPERTY_FQN = "io.acryl.dataManagement.retentionPeriodDays"
PROPERTY_DISPLAY_NAME = "Retention Period (Days)"
PROPERTY_DESCRIPTION = """Number of days data is retained in Snowflake based on table-level retention settings.

This property is automatically synced from Snowflake by the snowflake_retention_sync.py script.
Use this to identify tables with high retention periods that may be candidates for:
- Data lifecycle optimization
- Storage cost reduction
- Compliance policy adjustments
- Unused table cleanup
"""


def create_retention_property(datahub_url: str, datahub_token: str) -> bool:
    """Create the retention period structured property in DataHub"""
    try:
        # Create DataHub client
        logger.info(f"Connecting to DataHub at {datahub_url}...")
        client = DataHubGraph(
            DataHubGraphConfig(
                server=datahub_url,
                token=datahub_token,
            )
        )

        # Define the retention period structured property
        logger.info(f"Creating structured property: {PROPERTY_ID}")
        retention_property = StructuredProperties(
            id=PROPERTY_ID,
            qualified_name=PROPERTY_FQN,
            display_name=PROPERTY_DISPLAY_NAME,
            type="number",
            description=PROPERTY_DESCRIPTION,
            entity_types=["dataset"],  # Apply to datasets (tables, views, etc.)
            cardinality="SINGLE",  # Each dataset has one retention value
        )

        # Create the property in DataHub
        for mcp in retention_property.generate_mcps():
            client.emit(mcp)

        logger.info(f"✅ Successfully created structured property!")
        logger.info(f"   URN: {retention_property.urn}")
        logger.info(f"   ID: {PROPERTY_ID}")
        logger.info(f"   Display Name: {PROPERTY_DISPLAY_NAME}")
        logger.info("")
        logger.info("Next steps:")
        logger.info("1. Run snowflake_retention_sync.py to sync retention data from Snowflake")
        logger.info("2. Search for datasets in DataHub UI and filter by 'Retention Period (Days)'")
        logger.info("3. Combine with usage stats to identify high-retention, low-usage tables")

        return True

    except Exception as e:
        logger.error(f"Failed to create structured property: {e}", exc_info=True)
        return False


def main():
    """Main execution"""
    parser = argparse.ArgumentParser(
        description="Create retention period structured property in DataHub"
    )
    parser.add_argument(
        "--datahub-url",
        default=os.getenv("DATAHUB_GMS_URL"),
        help="DataHub GMS URL (e.g., https://your-instance.acryl.io)"
    )
    parser.add_argument(
        "--datahub-token",
        default=os.getenv("DATAHUB_TOKEN"),
        help="DataHub API token"
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging"
    )

    args = parser.parse_args()

    # Set logging level
    if args.verbose:
        logger.setLevel(logging.DEBUG)

    # Validate required arguments
    if not args.datahub_url or not args.datahub_token:
        logger.error("Missing required arguments. Use --help for details.")
        logger.error("You must provide either:")
        logger.error("  1. Environment variables: DATAHUB_GMS_URL and DATAHUB_TOKEN")
        logger.error("  2. Command-line arguments: --datahub-url and --datahub-token")
        sys.exit(1)

    # Create the property
    success = create_retention_property(args.datahub_url, args.datahub_token)

    if success:
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()
