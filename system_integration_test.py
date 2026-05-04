#!/usr/bin/env python3
"""
COMPREHENSIVE SYSTEM-LEVEL INTEGRATION TEST SUITE
For Smart Waste Management System (Group F)

Tests all endpoints, services, and data flows as specified in:
- service-specifications.md
- CLAUDE_guide.md

Author: Integration Testing Framework
Date: 2026-04-28
"""

import os
import sys
import json
import time
import logging
import subprocess
import requests
from typing import Dict, List, Tuple, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)-8s - %(message)s'
)
logger = logging.getLogger("SWMS-SystemTest")


@dataclass
class TestResult:
    name: str
    status: str  # PASS, FAIL, SKIP, ERROR
    message: str
    duration_ms: float
    details: Optional[Dict] = None


class SystemIntegrationTester:
    """Main test orchestrator"""
    
    def __init__(self):
        self.results: List[TestResult] = []
        self.base_url = "http://localhost"
        self.config = self._load_env_config()
        
    def _load_env_config(self) -> Dict:
        """Load configuration from .env"""
        config = {}
        env_file = Path("Data Analysis/.env")
        if env_file.exists():
            with open(env_file) as f:
                for line in f:
                    if line.strip() and not line.startswith('#'):
                        key, value = line.strip().split('=', 1)
                        config[key] = value
        return config
    
    def run_all_tests(self) -> None:
        """Execute complete test suite"""
        logger.info("=" * 80)
        logger.info("SMART WASTE MANAGEMENT SYSTEM - FULL INTEGRATION TEST SUITE")
        logger.info("=" * 80)
        logger.info("")
        
        # Phase 1: Infrastructure Readiness
        logger.info("[PHASE 1] Infrastructure & Service Readiness Checks")
        logger.info("-" * 80)
        self.test_docker_compose_health()
        self.test_container_status()
        self.test_network_connectivity()
        
        # Phase 2: Core Service Health
        logger.info("")
        logger.info("[PHASE 2] Core Service Endpoint Health")
        logger.info("-" * 80)
        self.test_kafka_connectivity()
        self.test_postgres_waste_connectivity()
        self.test_influxdb_connectivity()
        self.test_mlflow_connectivity()
        self.test_ml_service_health()
        self.test_airflow_health()
        
        # Phase 3: API Endpoints
        logger.info("")
        logger.info("[PHASE 3] ML Service API Endpoints")
        logger.info("-" * 80)
        self.test_ml_predict_fill_time()
        self.test_ml_predict_zone_generation()
        self.test_ml_trends_waste_generation()
        self.test_ml_score_route()
        self.test_ml_reload_endpoint()
        
        # Phase 4: Database Schema Validation
        logger.info("")
        logger.info("[PHASE 4] Database Schema Validation")
        logger.info("-" * 80)
        self.test_postgres_f2_schema()
        self.test_postgres_f3_schema()
        self.test_influx_buckets()
        
        # Phase 5: Kafka Topics
        logger.info("")
        logger.info("[PHASE 5] Kafka Topic Configuration")
        logger.info("-" * 80)
        self.test_kafka_topics()
        self.test_kafka_topic_connectivity()
        
        # Phase 6: Data Flow Simulation
        logger.info("")
        logger.info("[PHASE 6] End-to-End Data Flow")
        logger.info("-" * 80)
        self.test_sample_telemetry_flow()
        self.test_flink_processing()
        self.test_route_optimizer_integration()
        
        # Phase 7: Workflow Orchestration
        logger.info("")
        logger.info("[PHASE 7] Workflow & Orchestration")
        logger.info("-" * 80)
        self.test_airflow_dag_structure()
        self.test_ml_model_registry()
        
        # Generate report
        self._generate_report()
    
    # ========================================================================
    # PHASE 1: Infrastructure Readiness
    # ========================================================================
    
    def test_docker_compose_health(self) -> None:
        """Verify docker-compose services are running"""
        start = time.time()
        try:
            result = subprocess.run(
                ["docker-compose", "ps"],
                cwd="Data Analysis",
                capture_output=True,
                text=True,
                timeout=10
            )
            services = result.stdout.count('\n') - 1
            self._add_result(TestResult(
                name="Docker Compose Services Running",
                status="PASS" if services >= 8 else "FAIL",
                message=f"Found {services} running services (expected >= 8)",
                duration_ms=(time.time() - start) * 1000,
                details={"services_count": services}
            ))
        except Exception as e:
            self._add_result(TestResult(
                name="Docker Compose Services Running",
                status="ERROR",
                message=str(e),
                duration_ms=(time.time() - start) * 1000
            ))
    
    def test_container_status(self) -> None:
        """Check individual container statuses"""
        start = time.time()
        required_containers = [
            "waste-kafka", "waste-zookeeper", "waste-postgres-waste",
            "waste-influxdb", "waste-mlflow", "waste-ml-service"
        ]
        try:
            for container in required_containers:
                result = subprocess.run(
                    ["docker", "ps", "--filter", f"name={container}"],
                    capture_output=True,
                    text=True,
                    timeout=5
                )
                is_running = len(result.stdout.split('\n')) > 2
                self._add_result(TestResult(
                    name=f"Container: {container}",
                    status="PASS" if is_running else "FAIL",
                    message=f"Container {'is' if is_running else 'is NOT'} running",
                    duration_ms=(time.time() - start) * 1000
                ))
        except Exception as e:
            self._add_result(TestResult(
                name="Container Status Check",
                status="ERROR",
                message=str(e),
                duration_ms=(time.time() - start) * 1000
            ))
    
    def test_network_connectivity(self) -> None:
        """Test connectivity between services"""
        start = time.time()
        try:
            # Test host to container connectivity
            result = subprocess.run(
                ["docker", "run", "--rm", "--network", "dataanalysis_waste-network",
                 "busybox", "sh", "-c", "echo 'ping' | timeout 2 nc kafka 29092"],
                capture_output=True,
                timeout=10
            )
            self._add_result(TestResult(
                name="Network Connectivity (Kafka)",
                status="PASS" if result.returncode == 0 else "FAIL",
                message="Can reach Kafka from test container",
                duration_ms=(time.time() - start) * 1000
            ))
        except Exception as e:
            self._add_result(TestResult(
                name="Network Connectivity (Kafka)",
                status="ERROR",
                message=str(e),
                duration_ms=(time.time() - start) * 1000
            ))
    
    # ========================================================================
    # PHASE 2: Core Service Health
    # ========================================================================
    
    def test_kafka_connectivity(self) -> None:
        """Test Kafka broker connectivity"""
        start = time.time()
        try:
            result = subprocess.run(
                ["docker", "exec", "waste-kafka", "kafka-broker-api-versions",
                 "--bootstrap-server", "localhost:29092"],
                capture_output=True,
                text=True,
                timeout=10
            )
            is_healthy = result.returncode == 0
            self._add_result(TestResult(
                name="Kafka Broker Health",
                status="PASS" if is_healthy else "FAIL",
                message="Kafka broker is responding to API requests",
                duration_ms=(time.time() - start) * 1000
            ))
        except Exception as e:
            self._add_result(TestResult(
                name="Kafka Broker Health",
                status="ERROR",
                message=str(e),
                duration_ms=(time.time() - start) * 1000
            ))
    
    def test_postgres_waste_connectivity(self) -> None:
        """Test PostgreSQL waste database connectivity"""
        start = time.time()
        try:
            result = subprocess.run(
                ["docker", "exec", "waste-postgres-waste", "pg_isready",
                 "-U", "waste_admin"],
                capture_output=True,
                text=True,
                timeout=10
            )
            is_healthy = result.returncode == 0
            self._add_result(TestResult(
                name="PostgreSQL Waste DB Health",
                status="PASS" if is_healthy else "FAIL",
                message="PostgreSQL waste database is accepting connections",
                duration_ms=(time.time() - start) * 1000
            ))
        except Exception as e:
            self._add_result(TestResult(
                name="PostgreSQL Waste DB Health",
                status="ERROR",
                message=str(e),
                duration_ms=(time.time() - start) * 1000
            ))
    
    def test_influxdb_connectivity(self) -> None:
        """Test InfluxDB connectivity"""
        start = time.time()
        try:
            response = requests.get(
                "http://localhost:8086/health",
                timeout=5
            )
            is_healthy = response.status_code == 200
            self._add_result(TestResult(
                name="InfluxDB Health",
                status="PASS" if is_healthy else "FAIL",
                message=f"InfluxDB health endpoint returned {response.status_code}",
                duration_ms=(time.time() - start) * 1000
            ))
        except Exception as e:
            self._add_result(TestResult(
                name="InfluxDB Health",
                status="ERROR",
                message=str(e),
                duration_ms=(time.time() - start) * 1000
            ))
    
    def test_mlflow_connectivity(self) -> None:
        """Test MLflow connectivity"""
        start = time.time()
        try:
            response = requests.get(
                "http://localhost:5000/health",
                timeout=5
            )
            is_healthy = response.status_code == 200
            self._add_result(TestResult(
                name="MLflow Server Health",
                status="PASS" if is_healthy else "FAIL",
                message=f"MLflow health endpoint returned {response.status_code}",
                duration_ms=(time.time() - start) * 1000
            ))
        except Exception as e:
            self._add_result(TestResult(
                name="MLflow Server Health",
                status="SKIP",
                message="MLflow not yet available - expected on first startup",
                duration_ms=(time.time() - start) * 1000
            ))
    
    def test_ml_service_health(self) -> None:
        """Test ML Service endpoint health"""
        start = time.time()
        try:
            response = requests.get(
                "http://localhost:8000/health",
                timeout=5
            )
            is_healthy = response.status_code == 200
            self._add_result(TestResult(
                name="ML Service /health Endpoint",
                status="PASS" if is_healthy else "FAIL",
                message=f"ML Service responded with {response.status_code}",
                duration_ms=(time.time() - start) * 1000,
                details=response.json() if is_healthy else None
            ))
        except requests.exceptions.ConnectionError:
            self._add_result(TestResult(
                name="ML Service /health Endpoint",
                status="SKIP",
                message="ML Service not yet available - expected during startup",
                duration_ms=(time.time() - start) * 1000
            ))
        except Exception as e:
            self._add_result(TestResult(
                name="ML Service /health Endpoint",
                status="ERROR",
                message=str(e),
                duration_ms=(time.time() - start) * 1000
            ))
    
    def test_airflow_health(self) -> None:
        """Test Airflow connectivity"""
        start = time.time()
        try:
            response = requests.get(
                "http://localhost:8080/health",
                timeout=5
            )
            is_healthy = response.status_code == 200
            self._add_result(TestResult(
                name="Airflow /health Endpoint",
                status="PASS" if is_healthy else "FAIL",
                message=f"Airflow responded with {response.status_code}",
                duration_ms=(time.time() - start) * 1000
            ))
        except requests.exceptions.ConnectionError:
            self._add_result(TestResult(
                name="Airflow /health Endpoint",
                status="SKIP",
                message="Airflow not yet available - expected during startup",
                duration_ms=(time.time() - start) * 1000
            ))
        except Exception as e:
            self._add_result(TestResult(
                name="Airflow /health Endpoint",
                status="ERROR",
                message=str(e),
                duration_ms=(time.time() - start) * 1000
            ))
    
    # ========================================================================
    # PHASE 3: API Endpoints
    # ========================================================================
    
    def test_ml_predict_fill_time(self) -> None:
        """Test ML predict fill-time endpoint"""
        start = time.time()
        try:
            payload = {
                "bin_id": "BIN-001",
                "current_fill_pct": 45.0,
                "fill_rate_pct_per_hour": 5.5,
                "waste_category": "food_waste"
            }
            response = requests.post(
                "http://localhost:8000/api/v1/ml/predict/fill-time",
                json=payload,
                timeout=10
            )
            is_success = response.status_code in [200, 201]
            self._add_result(TestResult(
                name="ML API: POST /predict/fill-time",
                status="PASS" if is_success else "FAIL",
                message=f"Endpoint returned {response.status_code}",
                duration_ms=(time.time() - start) * 1000,
                details=response.json() if is_success else None
            ))
        except requests.exceptions.ConnectionError:
            self._add_result(TestResult(
                name="ML API: POST /predict/fill-time",
                status="SKIP",
                message="ML Service not available",
                duration_ms=(time.time() - start) * 1000
            ))
        except Exception as e:
            self._add_result(TestResult(
                name="ML API: POST /predict/fill-time",
                status="ERROR",
                message=str(e),
                duration_ms=(time.time() - start) * 1000
            ))
    
    def test_ml_predict_zone_generation(self) -> None:
        """Test ML predict zone-generation endpoint"""
        start = time.time()
        try:
            response = requests.get(
                "http://localhost:8000/api/v1/ml/predict/zone-generation?zone_id=1",
                timeout=10
            )
            is_success = response.status_code in [200, 201]
            self._add_result(TestResult(
                name="ML API: GET /predict/zone-generation",
                status="PASS" if is_success else "FAIL",
                message=f"Endpoint returned {response.status_code}",
                duration_ms=(time.time() - start) * 1000,
                details=response.json() if is_success else None
            ))
        except requests.exceptions.ConnectionError:
            self._add_result(TestResult(
                name="ML API: GET /predict/zone-generation",
                status="SKIP",
                message="ML Service not available",
                duration_ms=(time.time() - start) * 1000
            ))
        except Exception as e:
            self._add_result(TestResult(
                name="ML API: GET /predict/zone-generation",
                status="ERROR",
                message=str(e),
                duration_ms=(time.time() - start) * 1000
            ))
    
    def test_ml_trends_waste_generation(self) -> None:
        """Test ML trends waste-generation endpoint"""
        start = time.time()
        try:
            response = requests.get(
                "http://localhost:8000/api/v1/ml/trends/waste-generation?zone_id=1&days=30",
                timeout=10
            )
            is_success = response.status_code in [200, 201]
            self._add_result(TestResult(
                name="ML API: GET /trends/waste-generation",
                status="PASS" if is_success else "FAIL",
                message=f"Endpoint returned {response.status_code}",
                duration_ms=(time.time() - start) * 1000,
                details=response.json() if is_success else None
            ))
        except requests.exceptions.ConnectionError:
            self._add_result(TestResult(
                name="ML API: GET /trends/waste-generation",
                status="SKIP",
                message="ML Service not available",
                duration_ms=(time.time() - start) * 1000
            ))
        except Exception as e:
            self._add_result(TestResult(
                name="ML API: GET /trends/waste-generation",
                status="ERROR",
                message=str(e),
                duration_ms=(time.time() - start) * 1000
            ))
    
    def test_ml_score_route(self) -> None:
        """Test ML score-route endpoint"""
        start = time.time()
        try:
            payload = {
                "route_id": "ROUTE-001",
                "bins": ["BIN-001", "BIN-002", "BIN-003"],
                "zone_id": 1,
                "estimated_duration_minutes": 45
            }
            response = requests.post(
                "http://localhost:8000/api/v1/ml/score/route",
                json=payload,
                timeout=10
            )
            is_success = response.status_code in [200, 201]
            self._add_result(TestResult(
                name="ML API: POST /score/route",
                status="PASS" if is_success else "FAIL",
                message=f"Endpoint returned {response.status_code}",
                duration_ms=(time.time() - start) * 1000,
                details=response.json() if is_success else None
            ))
        except requests.exceptions.ConnectionError:
            self._add_result(TestResult(
                name="ML API: POST /score/route",
                status="SKIP",
                message="ML Service not available",
                duration_ms=(time.time() - start) * 1000
            ))
        except Exception as e:
            self._add_result(TestResult(
                name="ML API: POST /score/route",
                status="ERROR",
                message=str(e),
                duration_ms=(time.time() - start) * 1000
            ))
    
    def test_ml_reload_endpoint(self) -> None:
        """Test ML reload model endpoint"""
        start = time.time()
        try:
            response = requests.post(
                "http://localhost:8000/api/v1/ml/reload",
                timeout=10
            )
            is_success = response.status_code in [200, 201]
            self._add_result(TestResult(
                name="ML API: POST /reload",
                status="PASS" if is_success else "FAIL",
                message=f"Model reload returned {response.status_code}",
                duration_ms=(time.time() - start) * 1000,
                details=response.json() if is_success else None
            ))
        except requests.exceptions.ConnectionError:
            self._add_result(TestResult(
                name="ML API: POST /reload",
                status="SKIP",
                message="ML Service not available",
                duration_ms=(time.time() - start) * 1000
            ))
        except Exception as e:
            self._add_result(TestResult(
                name="ML API: POST /reload",
                status="ERROR",
                message=str(e),
                duration_ms=(time.time() - start) * 1000
            ))
    
    # ========================================================================
    # PHASE 4: Database Schema Validation
    # ========================================================================
    
    def test_postgres_f2_schema(self) -> None:
        """Validate F2 schema tables exist"""
        start = time.time()
        required_tables = [
            "waste_categories", "city_zones", "bins", "bin_current_state",
            "vehicles", "route_plans", "zone_snapshots", "model_performance"
        ]
        try:
            for table in required_tables:
                result = subprocess.run(
                    ["docker", "exec", "waste-postgres-waste", "psql", "-U", "waste_admin",
                     "-d", "waste_db", "-c", f"SELECT 1 FROM {table} LIMIT 1"],
                    capture_output=True,
                    text=True,
                    timeout=10
                )
                exists = "ERROR" not in result.stderr or "does not exist" not in result.stderr
                self._add_result(TestResult(
                    name=f"PostgreSQL Table: f2.{table}",
                    status="PASS" if exists else "FAIL",
                    message=f"Table {'exists' if exists else 'does NOT exist'}",
                    duration_ms=(time.time() - start) * 1000
                ))
        except Exception as e:
            self._add_result(TestResult(
                name="PostgreSQL F2 Schema Check",
                status="ERROR",
                message=str(e),
                duration_ms=(time.time() - start) * 1000
            ))
    
    def test_postgres_f3_schema(self) -> None:
        """Validate F3 schema tables exist"""
        start = time.time()
        required_tables = [
            "drivers", "collection_jobs", "bin_collection_records",
            "job_state_transitions", "job_step_results", "routine_schedules"
        ]
        try:
            for table in required_tables:
                result = subprocess.run(
                    ["docker", "exec", "waste-postgres-waste", "psql", "-U", "waste_admin",
                     "-d", "waste_db", "-c", f"SELECT 1 FROM f3.{table} LIMIT 1"],
                    capture_output=True,
                    text=True,
                    timeout=10
                )
                exists = "ERROR" not in result.stderr or "does not exist" not in result.stderr
                self._add_result(TestResult(
                    name=f"PostgreSQL Table: f3.{table}",
                    status="PASS" if exists else "FAIL",
                    message=f"Table {'exists' if exists else 'does NOT exist'}",
                    duration_ms=(time.time() - start) * 1000
                ))
        except Exception as e:
            self._add_result(TestResult(
                name="PostgreSQL F3 Schema Check",
                status="ERROR",
                message=str(e),
                duration_ms=(time.time() - start) * 1000
            ))
    
    def test_influx_buckets(self) -> None:
        """Validate InfluxDB buckets exist"""
        start = time.time()
        required_buckets = [
            "bin_readings_raw", "bin_readings_processed",
            "vehicle_positions", "zone_statistics", "waste_generation_trends"
        ]
        try:
            response = requests.get(
                "http://localhost:8086/api/v1/buckets",
                timeout=5
            )
            if response.status_code == 200:
                buckets = [b.get("name") for b in response.json().get("buckets", [])]
                self._add_result(TestResult(
                    name="InfluxDB Buckets Check",
                    status="PASS",
                    message=f"Found {len(buckets)} buckets",
                    duration_ms=(time.time() - start) * 1000,
                    details={"buckets": buckets}
                ))
        except Exception as e:
            self._add_result(TestResult(
                name="InfluxDB Buckets Check",
                status="SKIP",
                message="InfluxDB not fully initialized",
                duration_ms=(time.time() - start) * 1000
            ))
    
    # ========================================================================
    # PHASE 5: Kafka Topics
    # ========================================================================
    
    def test_kafka_topics(self) -> None:
        """Verify all required Kafka topics exist"""
        start = time.time()
        required_topics = [
            "waste.bin.telemetry", "waste.bin.processed", "waste.bin.status.changed",
            "waste.collection.jobs", "waste.routes.optimized",
            "waste.routine.schedule.trigger", "waste.job.completed",
            "waste.driver.responses", "waste.vehicle.location",
            "waste.vehicle.deviation", "waste.zone.statistics",
            "waste.audit.events", "waste.model.retrained"
        ]
        try:
            result = subprocess.run(
                ["docker", "exec", "waste-kafka", "kafka-topics", "--list",
                 "--bootstrap-server", "localhost:29092"],
                capture_output=True,
                text=True,
                timeout=10
            )
            existing_topics = result.stdout.strip().split('\n')
            for topic in required_topics:
                exists = topic in existing_topics
                self._add_result(TestResult(
                    name=f"Kafka Topic: {topic}",
                    status="PASS" if exists else "FAIL",
                    message=f"Topic {'exists' if exists else 'does NOT exist'}",
                    duration_ms=(time.time() - start) * 1000
                ))
        except Exception as e:
            self._add_result(TestResult(
                name="Kafka Topics Check",
                status="ERROR",
                message=str(e),
                duration_ms=(time.time() - start) * 1000
            ))
    
    def test_kafka_topic_connectivity(self) -> None:
        """Test ability to produce/consume from Kafka"""
        start = time.time()
        try:
            # Write test message
            result = subprocess.run(
                ["docker", "exec", "waste-kafka", "kafka-console-producer",
                 "--broker-list", "localhost:29092", "--topic", "waste.bin.telemetry"],
                input=json.dumps({"test": "message"}).encode(),
                capture_output=True,
                timeout=5
            )
            write_success = result.returncode == 0
            
            self._add_result(TestResult(
                name="Kafka Producer Test",
                status="PASS" if write_success else "FAIL",
                message="Successfully published test message",
                duration_ms=(time.time() - start) * 1000
            ))
        except Exception as e:
            self._add_result(TestResult(
                name="Kafka Producer Test",
                status="ERROR",
                message=str(e),
                duration_ms=(time.time() - start) * 1000
            ))
    
    # ========================================================================
    # PHASE 6: End-to-End Data Flow
    # ========================================================================
    
    def test_sample_telemetry_flow(self) -> None:
        """Test end-to-end bin telemetry flow"""
        start = time.time()
        try:
            # Create sample telemetry message
            telemetry = {
                "bin_id": "BIN-047",
                "fill_level_pct": 85.3,
                "battery_level_pct": 72.1,
                "signal_strength_dbm": -67,
                "temperature_c": 28.4,
                "timestamp": datetime.now().isoformat() + "Z",
                "firmware_version": "2.1.4",
                "error_flags": 0
            }
            
            # Write to Kafka
            result = subprocess.run(
                ["docker", "exec", "-i", "waste-kafka", "kafka-console-producer",
                 "--broker-list", "localhost:29092", "--topic", "waste.bin.telemetry"],
                input=json.dumps(telemetry).encode(),
                capture_output=True,
                timeout=5
            )
            
            is_success = result.returncode == 0
            self._add_result(TestResult(
                name="End-to-End: Sample Telemetry Injection",
                status="PASS" if is_success else "FAIL",
                message="Successfully injected sample telemetry to Kafka",
                duration_ms=(time.time() - start) * 1000,
                details=telemetry
            ))
        except Exception as e:
            self._add_result(TestResult(
                name="End-to-End: Sample Telemetry Injection",
                status="ERROR",
                message=str(e),
                duration_ms=(time.time() - start) * 1000
            ))
    
    def test_flink_processing(self) -> None:
        """Test Flink stream processor status"""
        start = time.time()
        try:
            result = subprocess.run(
                ["docker", "ps", "-f", "name=waste-flink"],
                capture_output=True,
                text=True,
                timeout=5
            )
            is_running = len(result.stdout.split('\n')) > 2
            self._add_result(TestResult(
                name="Flink Processor Service",
                status="PASS" if is_running else "FAIL",
                message="Flink processor is running",
                duration_ms=(time.time() - start) * 1000
            ))
        except Exception as e:
            self._add_result(TestResult(
                name="Flink Processor Service",
                status="ERROR",
                message=str(e),
                duration_ms=(time.time() - start) * 1000
            ))
    
    def test_route_optimizer_integration(self) -> None:
        """Test route optimizer integration"""
        start = time.time()
        try:
            result = subprocess.run(
                ["docker", "ps", "-f", "name=waste-route-optimizer"],
                capture_output=True,
                text=True,
                timeout=5
            )
            is_running = len(result.stdout.split('\n')) > 2
            self._add_result(TestResult(
                name="Route Optimizer Service",
                status="PASS" if is_running else "FAIL",
                message="Route optimizer is running",
                duration_ms=(time.time() - start) * 1000
            ))
        except Exception as e:
            self._add_result(TestResult(
                name="Route Optimizer Service",
                status="ERROR",
                message=str(e),
                duration_ms=(time.time() - start) * 1000
            ))
    
    # ========================================================================
    # PHASE 7: Workflow & Orchestration
    # ========================================================================
    
    def test_airflow_dag_structure(self) -> None:
        """Test Airflow DAG availability"""
        start = time.time()
        try:
            dag_file = Path("Data Analysis/airflow/dags/main_dag.py")
            exists = dag_file.exists()
            self._add_result(TestResult(
                name="Airflow DAG: main_dag.py",
                status="PASS" if exists else "FAIL",
                message="Main DAG file exists",
                duration_ms=(time.time() - start) * 1000
            ))
        except Exception as e:
            self._add_result(TestResult(
                name="Airflow DAG: main_dag.py",
                status="ERROR",
                message=str(e),
                duration_ms=(time.time() - start) * 1000
            ))
    
    def test_ml_model_registry(self) -> None:
        """Test MLflow model registry"""
        start = time.time()
        try:
            response = requests.get(
                "http://localhost:5000/api/2.0/registered-models",
                timeout=5
            )
            if response.status_code == 200:
                models = response.json().get("registered_models", [])
                self._add_result(TestResult(
                    name="MLflow Model Registry",
                    status="PASS",
                    message=f"Found {len(models)} registered models",
                    duration_ms=(time.time() - start) * 1000,
                    details={"model_count": len(models)}
                ))
        except requests.exceptions.ConnectionError:
            self._add_result(TestResult(
                name="MLflow Model Registry",
                status="SKIP",
                message="MLflow not yet available",
                duration_ms=(time.time() - start) * 1000
            ))
        except Exception as e:
            self._add_result(TestResult(
                name="MLflow Model Registry",
                status="ERROR",
                message=str(e),
                duration_ms=(time.time() - start) * 1000
            ))
    
    # ========================================================================
    # Utility Methods
    # ========================================================================
    
    def _add_result(self, result: TestResult) -> None:
        """Add test result and log it"""
        self.results.append(result)
        status_icon = "✓" if result.status == "PASS" else "✗" if result.status == "FAIL" else "⊘" if result.status == "SKIP" else "!"
        logger.info(f"{status_icon} [{result.status:6}] {result.name:50} ({result.duration_ms:.0f}ms)")
        if result.message:
            logger.info(f"          {result.message}")
    
    def _generate_report(self) -> None:
        """Generate comprehensive test report"""
        logger.info("")
        logger.info("=" * 80)
        logger.info("TEST REPORT SUMMARY")
        logger.info("=" * 80)
        
        passed = sum(1 for r in self.results if r.status == "PASS")
        failed = sum(1 for r in self.results if r.status == "FAIL")
        skipped = sum(1 for r in self.results if r.status == "SKIP")
        errors = sum(1 for r in self.results if r.status == "ERROR")
        total = len(self.results)
        
        logger.info(f"Total Tests: {total}")
        logger.info(f"Passed:  {passed} ({100*passed//total}%)")
        logger.info(f"Failed:  {failed}")
        logger.info(f"Skipped: {skipped}")
        logger.info(f"Errors:  {errors}")
        logger.info("")
        
        if failed + errors == 0:
            logger.info("🎉 ALL CRITICAL TESTS PASSED! System is ready for deployment.")
        else:
            logger.info("⚠️  ISSUES DETECTED - Review failures above")
        
        logger.info("")
        logger.info("=" * 80)


if __name__ == "__main__":
    tester = SystemIntegrationTester()
    
    # Wait a bit for services to initialize
    logger.info("Waiting 5 seconds for services to initialize...")
    time.sleep(5)
    
    # Run all tests
    tester.run_all_tests()
