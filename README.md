# Snowflake Retention Period Sync to DataHub

**Automatically sync Snowflake table retention periods to DataHub as searchable structured properties.**

This tool helps DataHub users identify tables with high retention periods that are not being used, enabling cost optimization and data lifecycle management. Perfect for POCs and production deployments.

## 🎯 Use Case

After syncing retention data to DataHub, you can:
- **Find unused tables with long retention periods** → Delete or reduce retention to save costs
- **Identify data lifecycle optimization opportunities** → Right-size retention based on usage
- **Combine with DataHub usage stats** → Prioritize tables with high retention + low usage
- **Track retention compliance** → Ensure tables meet data retention policies
- **Search and filter by retention period** in DataHub UI with proper indexed properties

## 🚀 Quick Start

### 0. Create the Structured Property (One-Time Setup)

**IMPORTANT:** Before syncing data, you must create the structured property definition in DataHub.

```bash
# Install dependencies first
pip install -r requirements.txt

# Then create the property definition
export DATAHUB_GMS_URL=https://your-instance.acryl.io
export DATAHUB_TOKEN=your-api-token
python create_retention_property.py
```

This creates a searchable "Retention Period (Days)" property that will show up in DataHub's search filters.

**You only need to run this once per DataHub instance.**

### 1. Configure Environment

```bash
cp config.example.env config.env
# Edit config.env with your credentials
source config.env
```

### 2. Run the Sync Script

```bash
# Dry run to test (no DataHub sync)
python snowflake_retention_sync.py --dry-run

# Full sync
python snowflake_retention_sync.py
```

## ⚙️ Configuration

### Environment Variables

```bash
# Snowflake Configuration
SNOWFLAKE_ACCOUNT=your-account           # Required
SNOWFLAKE_USER=service_account           # Required
SNOWFLAKE_PASSWORD=your-password         # Required
SNOWFLAKE_ROLE=ACCOUNTADMIN             # Optional
SNOWFLAKE_WAREHOUSE=COMPUTE_WH          # Optional

# DataHub Configuration
DATAHUB_GMS_URL=https://your-instance.acryl.io  # Required
DATAHUB_TOKEN=your-api-token                     # Required
DATAHUB_ENV=PROD                                 # Optional (default: PROD)

# Optional Filters
DATABASE_FILTER=DB1,DB2,DB3             # Comma-separated database names
SCHEMA_FILTER=PUBLIC,ANALYTICS          # Comma-separated schema names
```

### Command-Line Options

```bash
python snowflake_retention_sync.py \
  --snowflake-account your-account \
  --snowflake-user your-user \
  --snowflake-password your-password \
  --datahub-url https://your-instance.acryl.io \
  --datahub-token your-token \
  --database-filter "PROD_DB,ANALYTICS_DB" \
  --dry-run \
  --verbose
```

### Available Options

| Option | Description | Default |
|--------|-------------|---------|
| `--snowflake-account` | Snowflake account identifier | `$SNOWFLAKE_ACCOUNT` |
| `--snowflake-user` | Snowflake username | `$SNOWFLAKE_USER` |
| `--snowflake-password` | Snowflake password | `$SNOWFLAKE_PASSWORD` |
| `--snowflake-role` | Snowflake role to use | `$SNOWFLAKE_ROLE` |
| `--snowflake-warehouse` | Snowflake warehouse | `$SNOWFLAKE_WAREHOUSE` |
| `--datahub-url` | DataHub GMS URL | `$DATAHUB_GMS_URL` |
| `--datahub-token` | DataHub API token | `$DATAHUB_TOKEN` |
| `--datahub-env` | DataHub environment | `PROD` |
| `--database-filter` | Comma-separated database names | None (all) |
| `--schema-filter` | Comma-separated schema names | None (all) |
| `--dry-run` | Extract but don't sync to DataHub | False |
| `--verbose` | Enable debug logging | False |

## 📊 What Gets Synced

For each Snowflake table, the following **structured property** is added to DataHub:

- **`Retention Period (Days)`** - The retention period in days (searchable and filterable in UI)

This is stored as a **structured property**, which means it:
- ✅ Shows up in dataset pages
- ✅ Is **searchable** in DataHub UI
- ✅ Is **filterable** in the "More" section of search
- ✅ Is **indexed** in Elasticsearch for fast queries
- ✅ Has proper **typing** (number, not string)

## 🔍 Using Retention Data in DataHub

After syncing, you can search and filter in DataHub:

### 1. Search by Retention Period (NEW!)

In DataHub UI:
- Go to **Search** → Click **"More"** to expand filters
- Find **"Retention Period (Days)"** in the filter list
- Filter for high retention:
  - `> 90` (more than 90 days)
  - `>= 365` (1 year or more)
  - Range filters like `30-90` days

### 2. Combine with Usage Stats

Find high-retention, unused tables:
- **Retention Period (Days)** `> 90`
- **AND** Last Modified `> 6 months ago`
- **AND** Query Count `= 0` (last 30 days)

These tables are prime candidates for deletion or retention reduction!

### 3. Sort by Retention

Click the "Retention Period (Days)" column header to sort tables by retention period.

### 4. Generate Reports

Use DataHub's API or UI to export lists of:
- Tables with retention > 90 days and no recent usage
- Tables with retention > 365 days
- Tables sorted by retention period (descending)

## 📅 Scheduling

### Cron

Run daily at 2 AM:
```bash
0 2 * * * cd /path/to/snowflake-retention-sync && source config.env && python snowflake_retention_sync.py
```

### Airflow

```python
from airflow.operators.bash import BashOperator

sync_retention = BashOperator(
    task_id='sync_snowflake_retention',
    bash_command='cd /path/to/script && source config.env && python snowflake_retention_sync.py',
    dag=dag,
)
```

### Kubernetes CronJob

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: snowflake-retention-sync
spec:
  schedule: "0 2 * * *"
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: sync
            image: python:3.9
            command: ["python", "/app/snowflake_retention_sync.py"]
            envFrom:
            - secretRef:
                name: snowflake-retention-secrets
```

## 🔐 Security Best Practices

- **Use a dedicated service account** for Snowflake with minimal permissions
- **Required Snowflake privileges**: `USAGE` on databases/schemas, `SELECT` on `INFORMATION_SCHEMA`
- **Store credentials securely**: Use environment variables, secrets managers, or vault
- **Rotate DataHub tokens regularly**
- **Use read-only Snowflake role** if possible

## 🐛 Troubleshooting

### "Property not found" Error

If you see errors about the structured property not existing:
```
Error: Property io.acryl.dataManagement.retentionPeriodDays not found
```

**Solution:** Run the property creation script first (one-time setup):
```bash
python create_retention_property.py
```

### Connection Issues

```bash
# Test Snowflake connection
python snowflake_retention_sync.py --dry-run --verbose
```

### Permission Errors

Ensure your Snowflake user has:
```sql
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE DATAHUB_ROLE;
GRANT USAGE ON DATABASE your_database TO ROLE DATAHUB_ROLE;
GRANT USAGE ON SCHEMA your_database.your_schema TO ROLE DATAHUB_ROLE;
```

### DataHub API Errors

- Verify `DATAHUB_GMS_URL` is correct (should end in `.acryl.io` for cloud)
- Check token permissions in DataHub UI → Settings → Access Tokens
- Ensure token has `Edit Metadata` permission

### Retention Data Not Showing in UI

If retention data synced successfully but doesn't show in search filters:

**Problem:** You may have used custom properties instead of structured properties.

**Solution:**
1. Run `python create_retention_property.py` to create the structured property
2. Re-run `python snowflake_retention_sync.py` to sync data as structured properties

## 📝 Example Output

```
2024-03-15 14:30:00 - INFO - Connected to Snowflake account: your-account
2024-03-15 14:30:01 - INFO - Found 3 databases
2024-03-15 14:30:05 - INFO - Found 245 tables in PROD_DB.PUBLIC
2024-03-15 14:30:10 - INFO - Total tables extracted: 1,234
2024-03-15 14:30:10 - INFO - Retention period distribution:
2024-03-15 14:30:10 - INFO -   1 days: 450 tables
2024-03-15 14:30:10 - INFO -   7 days: 320 tables
2024-03-15 14:30:10 - INFO -   30 days: 180 tables
2024-03-15 14:30:10 - INFO -   90 days: 124 tables
2024-03-15 14:30:10 - INFO -   365 days: 160 tables
2024-03-15 14:30:15 - INFO - Starting DataHub sync...
2024-03-15 14:35:20 - INFO - Sync complete: 1234 succeeded, 0 failed
2024-03-15 14:35:20 - INFO - === SYNC COMPLETE ===
2024-03-15 14:35:20 - INFO - Total tables processed: 1234
2024-03-15 14:35:20 - INFO - Successfully synced: 1234
```

## 🤝 Contributing

Contributions welcome! Please open an issue or PR.

## 📄 License

MIT License - see LICENSE file for details

## 💬 Support

For questions or issues:
- Open a GitHub issue
- Contact your DataHub support team
- Email: support@acryl.io

---

**Built by Acryl Data** - The company behind DataHub
