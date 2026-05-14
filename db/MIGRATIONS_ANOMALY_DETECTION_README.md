# PostgreSQL Schema Migrations for Anomaly Detection

This directory contains SQL migration files for implementing the anomaly detection system.

## Migration Files

### 1. `migrations_anomaly_001_schema.sql`
**Main anomaly detection schema**
- `anomaly_events` - Core anomaly events table
- `anomaly_baselines` - Baseline statistics for detection
- `anomaly_rules` - Alert rules configuration
- `anomaly_alerts` - Alert tracking and resolution

**When to run**: First, before all other migrations
**Prerequisites**: PostgreSQL 13+, existing `bins`, `city_zones`, `vehicles` tables

### 2. `migrations_anomaly_002_security.sql`
**Security-focused anomaly detection**
- `anomaly_security_events` - Security incidents (hacking, spoofing, attacks)
- `security_anomaly_correlation` - Link security incidents to anomalies
- `network_activity_log` - API request tracking for network anomalies
- `entropy_scores` - Shannon entropy analysis for spoofing detection

**When to run**: After Phase 1, for security detection features
**Prerequisites**: `anomaly_events` table from migration 001

### 3. `migrations_anomaly_003_events.sql`
**Event-based anomaly detection**
- `special_events` - Manually logged special events (festivals, concerts, etc.)
- `event_impact_analysis` - Impact analysis of events on waste patterns
- `auto_detected_events` - Automatically detected event-driven anomalies
- `event_anomaly_correlation` - Link events to anomalies

**When to run**: After Phase 1, for event detection features
**Prerequisites**: `anomaly_events` and `city_zones` tables

## Running the Migrations

### Option 1: Manual SQL Execution
```bash
# Connect to PostgreSQL
psql -h localhost -U postgres -d smart_waste < migrations_anomaly_001_schema.sql
psql -h localhost -U postgres -d smart_waste < migrations_anomaly_002_security.sql
psql -h localhost -U postgres -d smart_waste < migrations_anomaly_003_events.sql
```

### Option 2: Using Docker
```bash
# From the Data Analysis repo root
docker exec waste-postgres psql -U postgres -d smart_waste < db/migrations_anomaly_001_schema.sql
docker exec waste-postgres psql -U postgres -d smart_waste < db/migrations_anomaly_002_security.sql
docker exec waste-postgres psql -U postgres -d smart_waste < db/migrations_anomaly_003_events.sql
```

### Option 3: Using Airflow (Automated)
Add migration tasks to Airflow DAGs that run on first deployment:

```python
@dag(schedule_interval=None, catchup=False, dag_id='db_migrations')
def run_migrations():
    run_migration_1 = BashOperator(
        task_id='run_migration_001',
        bash_command='psql -h postgres -U postgres -d smart_waste < /opt/migrations_anomaly_001_schema.sql'
    )
    # ... etc
```

## Verification

After running migrations, verify tables are created:

```bash
psql -h localhost -U postgres -d smart_waste << EOF
\dt anomaly_*
\dt security_*
\dt special_events
\dt event_*
EOF
```

Expected output should show:
```
anomaly_events
anomaly_baselines
anomaly_rules
anomaly_alerts
anomaly_security_events
security_anomaly_correlation
network_activity_log
entropy_scores
special_events
event_impact_analysis
auto_detected_events
event_anomaly_correlation
```

## Rollback Instructions

If you need to rollback a migration:

```bash
# Rollback security schema
DROP TABLE IF EXISTS security_anomaly_correlation CASCADE;
DROP TABLE IF EXISTS anomaly_security_events CASCADE;
DROP TABLE IF EXISTS network_activity_log CASCADE;
DROP TABLE IF EXISTS entropy_scores CASCADE;

# Rollback events schema
DROP TABLE IF EXISTS event_anomaly_correlation CASCADE;
DROP TABLE IF EXISTS auto_detected_events CASCADE;
DROP TABLE IF EXISTS event_impact_analysis CASCADE;
DROP TABLE IF EXISTS special_events CASCADE;

# Rollback main anomaly schema
DROP TABLE IF EXISTS anomaly_alerts CASCADE;
DROP TABLE IF EXISTS anomaly_rules CASCADE;
DROP TABLE IF EXISTS anomaly_baselines CASCADE;
DROP TABLE IF EXISTS anomaly_events CASCADE;
```

## Performance Considerations

- All tables include appropriate indexes on commonly queried columns
- Indexes on `timestamp` columns for efficient date-range queries
- Indexes on `zone_id` and `bin_id` for geo-spatial filtering
- Consider archiving old anomaly_events (>90 days) to maintain performance

## Data Retention Policy

Recommended retention periods:
- `anomaly_events`: 90 days (archive/delete older)
- `security_events`: 180 days (compliance requirement)
- `special_events`: Keep indefinitely (reference data)
- `event_impact_analysis`: Keep indefinitely (reference data)
- `network_activity_log`: 30 days

## Related Documentation

- See `API_INTEGRATION_GUIDE.md` for API endpoints
- See `sample_responses/` for example data structures
- See `ARCHITECTURE_README.md` for system overview
