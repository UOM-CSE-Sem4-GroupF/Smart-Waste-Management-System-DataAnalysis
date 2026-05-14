"""
Airflow DAG for batch anomaly analysis
Runs daily to analyze anomalies, train ML models, and correlate events
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.apache.spark.operators.spark_sql import SparkSqlOperator
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-analysis',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'anomaly_batch_analysis',
    default_args=default_args,
    description='Daily batch analysis of detected anomalies',
    schedule_interval='0 2 * * *',  # 2 AM daily
    catchup=False,
)


def aggregate_anomalies_24h() -> dict:
    """
    Aggregate anomalies from the last 24 hours
    Calculate statistics by anomaly type, severity, zone
    """
    try:
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        sql = """
        SELECT
            anomaly_type,
            severity,
            zone_id,
            COUNT(*) as count,
            COUNT(CASE WHEN anomaly_score > 0.7 THEN 1 END) as high_confidence_count,
            AVG(anomaly_score) as avg_score,
            MAX(timestamp) as latest_timestamp
        FROM anomaly_events
        WHERE timestamp > NOW() - INTERVAL '24 hours'
        GROUP BY anomaly_type, severity, zone_id
        ORDER BY count DESC
        """
        
        results = pg_hook.get_records(sql)
        logger.info(f"Aggregated {len(results)} anomaly types from last 24 hours")
        
        return {
            "status": "success",
            "records_aggregated": len(results),
            "timestamp": datetime.now().isoformat(),
        }
    except Exception as e:
        logger.error(f"Error aggregating anomalies: {e}")
        raise


def correlate_security_events() -> dict:
    """
    Correlate security incidents with anomalies
    Identify patterns and update correlation scores
    """
    try:
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # Find security incidents that correlate with anomaly patterns
        sql = """
        WITH security_events AS (
            SELECT id, affected_zones, timestamp FROM anomaly_security_events
            WHERE timestamp > NOW() - INTERVAL '24 hours'
        ),
        related_anomalies AS (
            SELECT se.id as security_id, ae.id as anomaly_id, ae.severity
            FROM security_events se
            CROSS JOIN LATERAL (
                SELECT id, severity FROM anomaly_events
                WHERE zone_id = ANY(se.affected_zones)
                AND timestamp >= se.timestamp - INTERVAL '30 minutes'
                AND timestamp <= se.timestamp + INTERVAL '30 minutes'
                LIMIT 100
            ) ae
        )
        INSERT INTO security_anomaly_correlation (security_event_id, anomaly_event_id, correlation_strength)
        SELECT security_id, anomaly_id, 0.8
        FROM related_anomalies
        ON CONFLICT DO NOTHING
        """
        
        pg_hook.run(sql)
        logger.info("Security events correlated with anomalies")
        
        return {
            "status": "success",
            "correlations_created": True,
            "timestamp": datetime.now().isoformat(),
        }
    except Exception as e:
        logger.error(f"Error correlating security events: {e}")
        raise


def analyze_event_impact() -> dict:
    """
    Analyze the impact of logged special events
    Calculate waste increase, collection rates, prediction accuracy during events
    """
    try:
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # Analyze each special event from last 30 days
        sql = """
        WITH event_windows AS (
            SELECT
                id as event_id,
                zone_ids,
                start_time,
                end_time,
                event_type
            FROM special_events
            WHERE start_time > NOW() - INTERVAL '30 days'
        ),
        event_anomalies AS (
            SELECT
                ew.event_id,
                ew.zone_ids[1]::int as zone_id,
                COUNT(*) as anomalies_count,
                COUNT(CASE WHEN ae.severity = 'HIGH' THEN 1 END) as high_count,
                COUNT(CASE WHEN ae.severity = 'CRITICAL' THEN 1 END) as critical_count
            FROM event_windows ew
            CROSS JOIN LATERAL (
                SELECT * FROM anomaly_events
                WHERE zone_id = ANY(ew.zone_ids)
                AND timestamp >= ew.start_time
                AND timestamp <= ew.end_time
            ) ae
            GROUP BY ew.event_id, ew.zone_ids[1]
        )
        INSERT INTO event_impact_analysis (event_id, zone_id, analysis_date, anomalies_detected_count, anomalies_high_severity_count, anomalies_critical_count)
        SELECT event_id, zone_id, CURRENT_DATE, anomalies_count, high_count, critical_count
        FROM event_anomalies
        ON CONFLICT (event_id, zone_id) DO UPDATE SET
            anomalies_detected_count = EXCLUDED.anomalies_detected_count,
            anomalies_high_severity_count = EXCLUDED.anomalies_high_severity_count,
            anomalies_critical_count = EXCLUDED.anomalies_critical_count
        """
        
        pg_hook.run(sql)
        logger.info("Event impact analysis completed")
        
        return {
            "status": "success",
            "events_analyzed": True,
            "timestamp": datetime.now().isoformat(),
        }
    except Exception as e:
        logger.error(f"Error analyzing event impact: {e}")
        raise


def calculate_detection_metrics() -> dict:
    """
    Calculate anomaly detection system metrics
    Track recall, false positive rate, detection latency
    Store in InfluxDB for monitoring
    """
    try:
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # Calculate false positive rate
        sql_fpr = """
        SELECT
            COUNT(CASE WHEN a.resolution_status = 'false_alarm' THEN 1 END)::float / 
            COUNT(*) as false_positive_rate
        FROM anomaly_alerts a
        WHERE a.created_at > NOW() - INTERVAL '24 hours'
        """
        
        results = pg_hook.get_first(sql_fpr)
        fpr = results[0] if results else 0
        
        # Calculate average resolution time
        sql_resolution = """
        SELECT
            AVG(EXTRACT(EPOCH FROM (resolved_at - created_at)) / 3600) as avg_hours
        FROM anomaly_alerts
        WHERE resolved_at IS NOT NULL
        AND created_at > NOW() - INTERVAL '7 days'
        """
        
        results = pg_hook.get_first(sql_resolution)
        avg_resolution_time = results[0] if results else 0
        
        logger.info(f"Detection metrics - FPR: {fpr:.2%}, Resolution time: {avg_resolution_time:.1f}h")
        
        return {
            "status": "success",
            "false_positive_rate": fpr,
            "avg_resolution_time_hours": avg_resolution_time,
            "timestamp": datetime.now().isoformat(),
        }
    except Exception as e:
        logger.error(f"Error calculating detection metrics: {e}")
        raise


# ============================================================================
# DAG Tasks
# ============================================================================

task_aggregate = PythonOperator(
    task_id='aggregate_anomalies_24h',
    python_callable=aggregate_anomalies_24h,
    dag=dag,
)

task_security_correlation = PythonOperator(
    task_id='correlate_security_events',
    python_callable=correlate_security_events,
    dag=dag,
)

task_event_impact = PythonOperator(
    task_id='analyze_event_impact',
    python_callable=analyze_event_impact,
    dag=dag,
)

task_metrics = PythonOperator(
    task_id='calculate_detection_metrics',
    python_callable=calculate_detection_metrics,
    dag=dag,
)

task_cleanup_old_events = BashOperator(
    task_id='cleanup_old_anomaly_events',
    bash_command="""
    psql -h postgres -U postgres -d smart_waste << EOF
    DELETE FROM anomaly_events WHERE timestamp < NOW() - INTERVAL '90 days';
    DELETE FROM anomaly_security_events WHERE timestamp < NOW() - INTERVAL '90 days';
    DELETE FROM auto_detected_events WHERE created_at < NOW() - INTERVAL '90 days';
    EOF
    """,
    dag=dag,
)

# ============================================================================
# Task Dependencies
# ============================================================================

task_aggregate >> [task_security_correlation, task_event_impact, task_metrics]
[task_security_correlation, task_event_impact, task_metrics] >> task_cleanup_old_events
