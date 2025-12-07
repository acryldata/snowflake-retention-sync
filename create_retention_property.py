#!/usr/bin/env python3
"""
Create Retention Period Structured Property in DataHub - FIXED VERSION

This script creates a structured property definition with proper search filter configuration.
Run this ONCE before syncing data with snowflake_retention_sync.py.

IMPORTANT: This version correctly sets BOTH aspects needed for search filters:
1. StructuredPropertyDefinition aspect (with searchConfiguration)
2. StructuredPropertySettings aspect (with showInSearchFilters = True) ← THIS WAS MISSING!

Usage:
    python create_retention_property.py --datahub-url https://your-datahub.io/gms --datahub-token YOUR_TOKEN

    Or with environment variables:
    export DATAHUB_GMS_URL=https://your-datahub.io/gms
    export DATAHUB_TOKEN=your-token
    python create_retention_property.py
"""

import argparse
import logging
import os
import sys

from datahub.ingestion.graph.client import DataHubGraph, DataHubGraphConfig
from datahub.metadata.schema_classes import (
    StructuredPropertyDefinitionClass,
    StructuredPropertySettingsClass,
    DataHubSearchConfigClass,
    AuditStampClass,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.utilities.urns.urn import Urn

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Property configuration
PROPERTY_ID = "io.acryl.dataManagement.retentionPeriodDays"
PROPERTY_URN = f"urn:li:structuredProperty:{PROPERTY_ID}"
DISPLAY_NAME = "Retention Period (Days)"
DESCRIPTION = "Number of days data is retained in Snowflake based on table retention settings"


def create_property_definition(client: DataHubGraph) -> bool:
    """Create the structured property definition aspect"""
    logger.info(f"Creating structured property definition: {PROPERTY_ID}")

    try:
        # Create search configuration
        search_config = DataHubSearchConfigClass(
            addToFilters=True,  # This enables filtering
            enableAutocomplete=False,
            queryByDefault=False,
            filterNameOverride=DISPLAY_NAME,
        )

        # Create property definition
        prop_def = StructuredPropertyDefinitionClass(
            qualifiedName=PROPERTY_ID,
            displayName=DISPLAY_NAME,
            description=DESCRIPTION,
            valueType=Urn.make_data_type_urn("number"),  # Number type for retention days
            entityTypes=[Urn.make_entity_type_urn("dataset")],  # Apply to datasets
            cardinality="SINGLE",  # Each dataset has one retention value
            searchConfiguration=search_config,  # Search configuration
            immutable=False,
        )

        # Emit the property definition
        mcp = MetadataChangeProposalWrapper(
            entityUrn=PROPERTY_URN,
            aspect=prop_def,
        )
        client.emit_mcp(mcp)
        logger.info("✅ Property definition created")
        return True

    except Exception as e:
        logger.error(f"❌ Failed to create property definition: {e}")
        return False


def create_property_settings(client: DataHubGraph) -> bool:
    """
    Create the structured property settings aspect

    THIS IS THE MISSING PIECE! The StructuredPropertySettings aspect with
    showInSearchFilters=True is what actually makes the property appear in search filters.
    """
    logger.info(f"Creating structured property settings for: {PROPERTY_ID}")

    try:
        import time

        # Create property settings with showInSearchFilters enabled
        prop_settings = StructuredPropertySettingsClass(
            isHidden=False,
            showInSearchFilters=True,  # ← THIS IS THE KEY!!! Makes it appear in "More" filters
            showInAssetSummary=True,  # Also show in asset sidebar
            hideInAssetSummaryWhenEmpty=False,  # Always show, even if empty
            showAsAssetBadge=False,  # Don't show as badge
            showInColumnsTable=False,  # Don't show in schema columns table
            lastModified=AuditStampClass(
                time=int(time.time() * 1000),
                actor="urn:li:corpuser:datahub",
            ),
        )

        # Emit the property settings
        mcp = MetadataChangeProposalWrapper(
            entityUrn=PROPERTY_URN,
            aspect=prop_settings,
        )
        client.emit_mcp(mcp)
        logger.info("✅ Property settings created with showInSearchFilters=True")
        return True

    except Exception as e:
        logger.error(f"❌ Failed to create property settings: {e}")
        return False


def verify_property(client: DataHubGraph) -> bool:
    """Verify that both aspects were created correctly"""
    logger.info("Verifying property configuration...")

    try:
        # Check property definition
        prop_def = client.get_aspect(PROPERTY_URN, StructuredPropertyDefinitionClass)
        if not prop_def:
            logger.error("❌ Property definition not found!")
            return False

        logger.info(f"✅ Property definition exists")
        logger.info(f"   Display Name: {prop_def.displayName}")
        logger.info(f"   Type: {prop_def.valueType}")

        if prop_def.searchConfiguration:
            logger.info(f"   searchConfiguration.addToFilters: {prop_def.searchConfiguration.addToFilters}")
        else:
            logger.warning("   ⚠️  No searchConfiguration found")

        # Check property settings (THE IMPORTANT ONE!)
        prop_settings = client.get_aspect(PROPERTY_URN, StructuredPropertySettingsClass)
        if not prop_settings:
            logger.error("❌ Property settings not found! This is required for search filters!")
            return False

        logger.info(f"✅ Property settings exists")
        logger.info(f"   showInSearchFilters: {prop_settings.showInSearchFilters}")
        logger.info(f"   showInAssetSummary: {prop_settings.showInAssetSummary}")

        if not prop_settings.showInSearchFilters:
            logger.error("❌ showInSearchFilters is False! Property will NOT appear in search filters!")
            return False

        logger.info("\n🎉 SUCCESS! Property is fully configured and should appear in search filters!")
        return True

    except Exception as e:
        logger.error(f"❌ Error verifying property: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Create retention period structured property with search filters enabled"
    )
    parser.add_argument(
        "--datahub-url",
        default=os.getenv("DATAHUB_GMS_URL"),
        help="DataHub GMS URL (e.g., https://test-environment.acryl.io/gms)",
    )
    parser.add_argument(
        "--datahub-token",
        default=os.getenv("DATAHUB_TOKEN"),
        help="DataHub API token",
    )

    args = parser.parse_args()

    if not args.datahub_url or not args.datahub_token:
        logger.error("❌ Missing required arguments: --datahub-url and --datahub-token")
        logger.error("   Or set DATAHUB_GMS_URL and DATAHUB_TOKEN environment variables")
        sys.exit(1)

    # Create DataHub client
    logger.info(f"Connecting to DataHub: {args.datahub_url}")
    client = DataHubGraph(
        DataHubGraphConfig(
            server=args.datahub_url,
            token=args.datahub_token,
        )
    )

    # Create both aspects (both are required!)
    success = True

    # Step 1: Create property definition
    if not create_property_definition(client):
        success = False

    # Step 2: Create property settings (THE MISSING PIECE!)
    if not create_property_settings(client):
        success = False

    # Step 3: Verify everything is correct
    if success:
        if not verify_property(client):
            success = False

    if success:
        logger.info("\n" + "="*70)
        logger.info("✅ PROPERTY CREATED SUCCESSFULLY!")
        logger.info("="*70)
        logger.info(f"Property URN: {PROPERTY_URN}")
        logger.info(f"Property ID: {PROPERTY_ID}")
        logger.info("")
        logger.info("Next steps:")
        logger.info("1. Run snowflake_retention_sync.py to sync data")
        logger.info("2. Wait 5-10 minutes for Elasticsearch to reindex")
        logger.info("3. Check DataHub search UI - property should appear in 'More' filters!")
        logger.info("4. Hard refresh browser (Cmd+Shift+R or Ctrl+Shift+R) if needed")
    else:
        logger.error("\n❌ FAILED TO CREATE PROPERTY")
        sys.exit(1)


if __name__ == "__main__":
    main()
