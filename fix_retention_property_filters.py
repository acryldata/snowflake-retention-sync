#!/usr/bin/env python3
"""
Fix Existing Retention Property to Enable Search Filters

If you already created the retention property but it's not showing up in search filters,
run this script to add the missing StructuredPropertySettings aspect.

Usage:
    python fix_retention_property_filters.py
"""

import os
import sys
import time
import logging

from datahub.ingestion.graph.client import DataHubGraph, DataHubGraphConfig
from datahub.metadata.schema_classes import (
    StructuredPropertySettingsClass,
    AuditStampClass,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

PROPERTY_ID = "io.acryl.dataManagement.retentionPeriodDays"
PROPERTY_URN = f"urn:li:structuredProperty:{PROPERTY_ID}"

DATAHUB_URL = os.getenv("DATAHUB_GMS_URL", "https://test-environment.acryl.io/gms")
DATAHUB_TOKEN = os.getenv("DATAHUB_TOKEN")

if not DATAHUB_TOKEN:
    logger.error("❌ DATAHUB_TOKEN environment variable is required!")
    sys.exit(1)

logger.info(f"Connecting to DataHub: {DATAHUB_URL}")
client = DataHubGraph(DataHubGraphConfig(server=DATAHUB_URL, token=DATAHUB_TOKEN))

# Check current settings
logger.info(f"Checking current settings for: {PROPERTY_ID}")
current_settings = client.get_aspect(PROPERTY_URN, StructuredPropertySettingsClass)

if current_settings:
    logger.info(f"Current showInSearchFilters: {current_settings.showInSearchFilters}")
    if current_settings.showInSearchFilters:
        logger.info("✅ Property already has showInSearchFilters=True!")
        logger.info("If it's still not showing in UI, try:")
        logger.info("1. Re-run snowflake_retention_sync.py to re-sync all data")
        logger.info("2. Wait 5-10 minutes for Elasticsearch reindexing")
        logger.info("3. Hard refresh browser (Cmd+Shift+R)")
        sys.exit(0)

# Create/update settings to enable search filters
logger.info("Enabling search filters...")

prop_settings = StructuredPropertySettingsClass(
    isHidden=False,
    showInSearchFilters=True,  # Enable search filters
    showInAssetSummary=True,  # Show in asset sidebar
    hideInAssetSummaryWhenEmpty=False,
    showAsAssetBadge=False,
    showInColumnsTable=False,
    lastModified=AuditStampClass(
        time=int(time.time() * 1000),
        actor="urn:li:corpuser:datahub",
    ),
)

mcp = MetadataChangeProposalWrapper(
    entityUrn=PROPERTY_URN,
    aspect=prop_settings,
)
client.emit_mcp(mcp)
client.flush()

logger.info("✅ Property settings updated!")

# Verify
updated_settings = client.get_aspect(PROPERTY_URN, StructuredPropertySettingsClass)
if updated_settings and updated_settings.showInSearchFilters:
    logger.info("\n🎉 SUCCESS! showInSearchFilters is now TRUE!")
    logger.info("\nNext steps:")
    logger.info("1. Re-run snowflake_retention_sync.py to re-sync all datasets")
    logger.info("2. Wait 5-10 minutes for Elasticsearch to reindex")
    logger.info("3. Hard refresh browser (Cmd+Shift+R or Ctrl+Shift+R)")
    logger.info("4. Check DataHub search UI - property should now appear in 'More' filters!")
else:
    logger.error("❌ Failed to update settings!")
    sys.exit(1)
