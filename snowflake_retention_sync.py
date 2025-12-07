#!/usr/bin/env python3
"""
Snowflake Retention Period Sync to DataHub

This script extracts retention period data from Snowflake tables and syncs it
to DataHub as custom properties. Designed for repeatable, scheduled execution.

Usage:
    python snowflake_retention_sync.py --config config.yaml

    Or with environment variables:
    export SNOWFLAKE_ACCOUNT=your-account
    export SNOWFLAKE_USER=datahub_user
    export SNOWFLAKE_PASSWORD=password123
    export DATAHUB_GMS_URL=https://your-instance.acryl.io
    export DATAHUB_TOKEN=your-token
    python snowflake_retention_sync.py
"""

import argparse
import logging
import os
import sys
from datetime import datetime
from typing import Dict, List, Optional
from dataclasses import dataclass

import snowflake.connector
from snowflake.connector.errors import ProgrammingError, DatabaseError
from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import DatasetPropertiesClass

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class SnowflakeTable:
    """Represents a Snowflake table with retention metadata"""
    database: str
    schema: str
    table: str
    retention_days: int
    created_on: Optional[str] = None
    row_count: Optional[int] = None
    bytes: Optional[int] = None


class SnowflakeRetentionExtractor:
    """Extracts retention period data from Snowflake"""

    def __init__(
        self,
        account: str,
        user: str,
        password: str,
        role: Optional[str] = None,
        warehouse: Optional[str] = None,
        database_filter: Optional[List[str]] = None,
        schema_filter: Optional[List[str]] = None,
    ):
        self.account = account
        self.user = user
        self.password = password
        self.role = role
        self.warehouse = warehouse
        self.database_filter = database_filter or []
        self.schema_filter = schema_filter or []
        self.conn = None

    def connect(self):
        """Establish Snowflake connection"""
        try:
            conn_params = {
                'account': self.account,
                'user': self.user,
                'password': self.password,
            }
            if self.role:
                conn_params['role'] = self.role
            if self.warehouse:
                conn_params['warehouse'] = self.warehouse

            self.conn = snowflake.connector.connect(**conn_params)
            logger.info(f"Connected to Snowflake account: {self.account}")
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {e}")
            raise

    def close(self):
        """Close Snowflake connection"""
        if self.conn:
            self.conn.close()
            logger.info("Closed Snowflake connection")

    def get_databases(self) -> List[str]:
        """Get list of databases (filtered if specified)"""
        cursor = self.conn.cursor()
        try:
            cursor.execute("SHOW DATABASES")
            all_databases = [row[1] for row in cursor.fetchall()]

            # Apply filter if specified
            if self.database_filter:
                databases = [db for db in all_databases if db in self.database_filter]
                logger.info(f"Filtered to {len(databases)} databases: {databases}")
            else:
                databases = all_databases
                logger.info(f"Found {len(databases)} databases")

            return databases
        except ProgrammingError as e:
            logger.error(f"Error fetching databases: {e}")
            return []
        finally:
            cursor.close()

    def get_schemas(self, database: str) -> List[str]:
        """Get list of schemas in a database (filtered if specified)"""
        cursor = self.conn.cursor()
        try:
            cursor.execute(f"SHOW SCHEMAS IN DATABASE {database}")
            all_schemas = [row[1] for row in cursor.fetchall()]

            # Apply filter if specified
            if self.schema_filter:
                schemas = [s for s in all_schemas if s in self.schema_filter]
            else:
                schemas = all_schemas

            return schemas
        except ProgrammingError as e:
            logger.warning(f"Error fetching schemas from {database}: {e}")
            return []
        finally:
            cursor.close()

    def get_tables_with_retention(self, database: str, schema: str) -> List[SnowflakeTable]:
        """Get tables with retention data from a specific schema"""
        cursor = self.conn.cursor()
        tables = []

        try:
            # Use SHOW TABLES to get retention_time
            query = f"SHOW TABLES IN {database}.{schema}"
            cursor.execute(query)

            # SHOW TABLES columns: created_on, name, database_name, schema_name, kind, comment,
            #                      cluster_by, rows, bytes, owner, retention_time, ...
            for row in cursor.fetchall():
                try:
                    table = SnowflakeTable(
                        database=row[2],  # database_name
                        schema=row[3],     # schema_name
                        table=row[1],      # name
                        retention_days=int(row[5]) if row[5] is not None else 1,  # retention_time
                        created_on=str(row[0]) if row[0] else None,
                        row_count=int(row[6]) if row[6] is not None else None,  # rows
                        bytes=int(row[7]) if row[7] is not None else None,  # bytes
                    )
                    tables.append(table)
                except (IndexError, ValueError) as e:
                    logger.warning(f"Error parsing table {row[1]}: {e}")
                    continue

            logger.info(f"Found {len(tables)} tables in {database}.{schema}")
            return tables

        except ProgrammingError as e:
            logger.warning(f"Error fetching tables from {database}.{schema}: {e}")
            return []
        finally:
            cursor.close()

    def extract_all_retention_data(self) -> List[SnowflakeTable]:
        """Extract retention data from all databases/schemas"""
        all_tables = []

        databases = self.get_databases()

        for database in databases:
            schemas = self.get_schemas(database)

            for schema in schemas:
                # Skip information_schema and other system schemas
                if schema.upper() in ['INFORMATION_SCHEMA']:
                    continue

                tables = self.get_tables_with_retention(database, schema)
                all_tables.extend(tables)

        logger.info(f"Total tables extracted: {len(all_tables)}")
        return all_tables


class DataHubRetentionSyncer:
    """Syncs retention data to DataHub as custom properties"""

    def __init__(self, gms_url: str, token: str, env: str = "PROD"):
        self.gms_url = gms_url
        self.token = token
        self.env = env
        self.emitter = DatahubRestEmitter(gms_server=gms_url, token=token)

    def sync_table(self, table: SnowflakeTable) -> bool:
        """Sync a single table's retention data to DataHub"""
        try:
            # Build dataset URN
            dataset_name = f"{table.database}.{table.schema}.{table.table}".lower()
            urn = make_dataset_urn(
                platform="snowflake",
                name=dataset_name,
                env=self.env
            )

            # Build custom properties
            custom_properties = {
                "retention_period_days": str(table.retention_days),
                "retention_sync_timestamp": datetime.now().isoformat(),
            }

            # Add optional metadata if available
            if table.row_count is not None:
                custom_properties["row_count"] = str(table.row_count)
            if table.bytes is not None:
                custom_properties["size_bytes"] = str(table.bytes)
            if table.created_on:
                custom_properties["created_on"] = table.created_on

            # Create properties aspect
            properties = DatasetPropertiesClass(
                customProperties=custom_properties
            )

            # Emit to DataHub
            mcp = MetadataChangeProposalWrapper(
                entityUrn=urn,
                aspect=properties,
            )
            self.emitter.emit_mcp(mcp)

            logger.debug(f"Synced retention data for {dataset_name}")
            return True

        except Exception as e:
            logger.error(f"Error syncing {table.database}.{table.schema}.{table.table}: {e}")
            return False

    def sync_all(self, tables: List[SnowflakeTable]) -> Dict[str, int]:
        """Sync all tables to DataHub"""
        stats = {"success": 0, "failed": 0}

        for table in tables:
            if self.sync_table(table):
                stats["success"] += 1
            else:
                stats["failed"] += 1

        logger.info(f"Sync complete: {stats['success']} succeeded, {stats['failed']} failed")
        return stats

    def close(self):
        """Close emitter connection"""
        if self.emitter:
            self.emitter.close()


def main():
    """Main execution"""
    parser = argparse.ArgumentParser(description="Sync Snowflake retention data to DataHub")
    parser.add_argument("--snowflake-account", default=os.getenv("SNOWFLAKE_ACCOUNT"),
                       help="Snowflake account (e.g., your-account)")
    parser.add_argument("--snowflake-user", default=os.getenv("SNOWFLAKE_USER"),
                       help="Snowflake username")
    parser.add_argument("--snowflake-password", default=os.getenv("SNOWFLAKE_PASSWORD"),
                       help="Snowflake password")
    parser.add_argument("--snowflake-role", default=os.getenv("SNOWFLAKE_ROLE"),
                       help="Snowflake role (optional)")
    parser.add_argument("--snowflake-warehouse", default=os.getenv("SNOWFLAKE_WAREHOUSE"),
                       help="Snowflake warehouse (optional)")
    parser.add_argument("--datahub-url", default=os.getenv("DATAHUB_GMS_URL"),
                       help="DataHub GMS URL (e.g., https://your-instance.acryl.io)")
    parser.add_argument("--datahub-token", default=os.getenv("DATAHUB_TOKEN"),
                       help="DataHub API token")
    parser.add_argument("--datahub-env", default=os.getenv("DATAHUB_ENV", "PROD"),
                       help="DataHub environment (default: PROD)")
    parser.add_argument("--database-filter", default=os.getenv("DATABASE_FILTER"),
                       help="Comma-separated list of databases to include")
    parser.add_argument("--schema-filter", default=os.getenv("SCHEMA_FILTER"),
                       help="Comma-separated list of schemas to include")
    parser.add_argument("--dry-run", action="store_true",
                       help="Extract data but don't sync to DataHub")
    parser.add_argument("--verbose", action="store_true",
                       help="Enable debug logging")

    args = parser.parse_args()

    # Set logging level
    if args.verbose:
        logger.setLevel(logging.DEBUG)

    # Validate required arguments
    if not all([args.snowflake_account, args.snowflake_user, args.snowflake_password,
                args.datahub_url, args.datahub_token]):
        logger.error("Missing required arguments. Use --help for details.")
        sys.exit(1)

    # Parse filters
    database_filter = args.database_filter.split(',') if args.database_filter else None
    schema_filter = args.schema_filter.split(',') if args.schema_filter else None

    try:
        # Extract from Snowflake
        logger.info("Starting Snowflake retention data extraction...")
        extractor = SnowflakeRetentionExtractor(
            account=args.snowflake_account,
            user=args.snowflake_user,
            password=args.snowflake_password,
            role=args.snowflake_role,
            warehouse=args.snowflake_warehouse,
            database_filter=database_filter,
            schema_filter=schema_filter,
        )

        extractor.connect()
        tables = extractor.extract_all_retention_data()
        extractor.close()

        if not tables:
            logger.warning("No tables found. Exiting.")
            return

        # Print summary
        retention_summary = {}
        for table in tables:
            retention_summary[table.retention_days] = retention_summary.get(table.retention_days, 0) + 1

        logger.info("Retention period distribution:")
        for days, count in sorted(retention_summary.items()):
            logger.info(f"  {days} days: {count} tables")

        if args.dry_run:
            logger.info("Dry run mode - skipping DataHub sync")
            return

        # Sync to DataHub
        logger.info("Starting DataHub sync...")
        syncer = DataHubRetentionSyncer(
            gms_url=args.datahub_url,
            token=args.datahub_token,
            env=args.datahub_env,
        )

        stats = syncer.sync_all(tables)
        syncer.close()

        logger.info("=== SYNC COMPLETE ===")
        logger.info(f"Total tables processed: {len(tables)}")
        logger.info(f"Successfully synced: {stats['success']}")
        logger.info(f"Failed: {stats['failed']}")

    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
