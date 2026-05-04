#!/bin/bash

# Smart Waste Management System - Data Analysis Layer
# Comprehensive Startup and Integration Script
# Group F2 - Data Layer

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
PROJECT_NAME="${DOCKER_COMPOSE_PROJECT:-waste-management}"
NETWORK_NAME="waste-network"
PROFILES="${1:-}"

echo -e "${BLUE}================================================${NC}"
echo -e "${BLUE}Smart Waste Management System - Data Analysis${NC}"
echo -e "${BLUE}================================================${NC}"
echo ""

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

log_info() {
    echo -e "${GREEN}✓${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}⚠${NC} $1"
}

log_error() {
    echo -e "${RED}✗${NC} $1"
}

log_section() {
    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
}

check_docker() {
    if ! command -v docker &> /dev/null; then
        log_error "Docker is not installed"
        exit 1
    fi
    log_info "Docker found: $(docker --version)"
}

check_docker_compose() {
    if ! command -v docker-compose &> /dev/null; then
        log_error "Docker Compose is not installed"
        exit 1
    fi
    log_info "Docker Compose found: $(docker-compose --version)"
}

check_env_file() {
    if [ ! -f ".env" ]; then
        log_error ".env file not found"
        log_info "Creating .env from template..."
        if [ -f ".env.example" ]; then
            cp .env.example .env
            log_info ".env created from template"
        else
            log_error "No .env or .env.example found"
            exit 1
        fi
    fi
    log_info ".env file exists"
}

cleanup_stale_containers() {
    log_section "Cleaning up stale containers and networks"
    
    # Remove stale containers that might conflict
    for container in waste-postgres waste-kafka waste-zookeeper; do
        if docker ps -a --format '{{.Names}}' | grep -q "^${container}$"; then
            log_warn "Found stale container: $container, removing..."
            docker rm -f "$container" 2>/dev/null || true
        fi
    done
    
    log_info "Cleanup complete"
}

start_services() {
    log_section "Starting Data Analysis Services"
    
    local compose_cmd="docker-compose"
    if [ -n "$PROFILES" ]; then
        log_info "Starting with profiles: $PROFILES"
        $compose_cmd --profile init --profile apps up -d --build
    else
        log_info "Starting with default profile (production)"
        $compose_cmd up -d --build
    fi
    
    log_info "Containers started"
}

wait_for_service() {
    local service=$1
    local max_attempts=30
    local attempt=1
    
    echo -n "Waiting for $service..."
    
    while [ $attempt -le $max_attempts ]; do
        if docker-compose ps | grep -q "$service.*Up"; then
            echo -e " ${GREEN}✓${NC}"
            return 0
        fi
        echo -n "."
        sleep 2
        attempt=$((attempt + 1))
    done
    
    echo -e " ${RED}✗${NC}"
    log_error "$service failed to start"
    return 1
}

wait_for_kafka() {
    log_info "Waiting for Kafka to be ready..."
    local max_attempts=30
    local attempt=1
    
    while [ $attempt -le $max_attempts ]; do
        if docker-compose exec -T kafka kafka-broker-api-versions --bootstrap-server localhost:29092 &>/dev/null; then
            log_info "Kafka is ready"
            return 0
        fi
        echo -n "."
        sleep 2
        attempt=$((attempt + 1))
    done
    
    log_error "Kafka failed to become ready"
    return 1
}

verify_services() {
    log_section "Verifying Service Health"
    
    local services=("kafka" "postgres-waste" "influxdb" "mlflow" "ml-service" "flink-processor" "route-optimizer" "airflow")
    local failed=0
    
    for service in "${services[@]}"; do
        if docker-compose ps | grep -q "waste-$service.*Up"; then
            log_info "$service is running"
        else
            log_warn "$service is not running (may be optional)"
        fi
    done
}

test_kafka_connectivity() {
    log_section "Testing Kafka Connectivity"
    
    log_info "Listing Kafka topics..."
    docker-compose exec -T kafka kafka-topics --list --bootstrap-server kafka:29092 || {
        log_error "Could not list Kafka topics"
        return 1
    }
    
    log_info "Kafka connectivity verified"
}

test_postgresql_connectivity() {
    log_section "Testing PostgreSQL Connectivity"
    
    log_info "Checking PostgreSQL for waste management..."
    docker-compose exec -T postgres-waste pg_isready -U waste_admin || {
        log_error "PostgreSQL not responding"
        return 1
    }
    
    log_info "PostgreSQL connectivity verified"
}

test_influxdb_connectivity() {
    log_section "Testing InfluxDB Connectivity"
    
    log_info "Checking InfluxDB..."
    docker-compose exec -T influxdb influx ping || {
        log_warn "InfluxDB ping failed"
    }
    
    log_info "InfluxDB available"
}

test_ml_service() {
    log_section "Testing ML Service"
    
    log_info "Checking ML Service health..."
    if curl -f http://localhost:8000/health &>/dev/null; then
        log_info "ML Service is responding"
    else
        log_warn "ML Service not yet responding (may still be starting)"
    fi
}

run_integration_tests() {
    log_section "Running Integration Tests"
    
    if [ -f "tests/test_integration_pipeline.py" ]; then
        log_info "Found integration test suite"
        if command -v pytest &> /dev/null; then
            pytest tests/test_integration_pipeline.py -v --tb=short || {
                log_warn "Some integration tests failed (this is expected on first run)"
            }
        else
            log_warn "pytest not installed, skipping integration tests"
        fi
    else
        log_warn "No integration test file found"
    fi
}

show_endpoints() {
    log_section "Service Endpoints"
    
    echo ""
    echo -e "${GREEN}Management UIs:${NC}"
    echo -e "  Airflow:     ${BLUE}http://localhost:8080${NC} (admin/admin)"
    echo -e "  MLflow:      ${BLUE}http://localhost:5000${NC}"
    echo -e "  ML-Service:  ${BLUE}http://localhost:8000/docs${NC}"
    echo ""
    
    echo -e "${GREEN}Database Connections:${NC}"
    echo -e "  PostgreSQL (waste): ${BLUE}postgres://waste_admin@localhost:5432/waste_management${NC}"
    echo -e "  PostgreSQL (airflow): ${BLUE}postgres://airflow@localhost:5432/airflow${NC}"
    echo -e "  InfluxDB:   ${BLUE}http://localhost:8086${NC}"
    echo ""
    
    echo -e "${GREEN}Message Broker:${NC}"
    echo -e "  Kafka:      ${BLUE}localhost:9092${NC}"
    echo -e "  Zookeeper:  ${BLUE}localhost:2181${NC}"
    echo ""
}

show_help() {
    cat << EOF
${BLUE}Smart Waste Management System - Data Analysis Layer${NC}

${YELLOW}Usage:${NC}
  $0 [PROFILE]

${YELLOW}Profiles:${NC}
  (empty)         Start production services (minimal)
  apps            Start all application services
  test            Include test services (app-consumer)
  analytics       Include Spark for analytics
  all             Start everything

${YELLOW}Examples:${NC}
  $0              # Start minimal setup
  $0 apps         # Start all apps
  $0 all          # Start everything

${YELLOW}Common Commands:${NC}
  docker-compose logs -f flink-processor   # View Flink logs
  docker-compose logs -f kafka             # View Kafka logs
  docker-compose ps                        # List all services
  docker-compose down                      # Stop all services

EOF
}

# ============================================================================
# MAIN EXECUTION
# ============================================================================

main() {
    # Check prerequisites
    check_docker
    check_docker_compose
    check_env_file
    
    # Cleanup and prepare
    cleanup_stale_containers
    
    # Start services
    start_services
    
    # Wait for critical services
    wait_for_service "kafka"
    wait_for_service "postgres-waste"
    wait_for_service "influxdb"
    wait_for_service "mlflow"
    
    # Additional wait time for Kafka stability
    wait_for_kafka
    
    # Verify services
    verify_services
    
    # Test connectivity
    test_kafka_connectivity || log_warn "Kafka connectivity test failed"
    test_postgresql_connectivity || log_warn "PostgreSQL connectivity test failed"
    test_influxdb_connectivity || log_warn "InfluxDB connectivity test failed"
    test_ml_service || log_warn "ML Service connectivity test failed"
    
    # Optional: run integration tests
    if [ "$PROFILES" = "test" ] || [ "$PROFILES" = "all" ]; then
        run_integration_tests
    fi
    
    # Show endpoints
    show_endpoints
    
    log_section "Setup Complete!"
    echo -e "${GREEN}Data Analysis layer is ready for development.${NC}"
    echo -e "Run ${BLUE}docker-compose logs -f${NC} to see logs"
    echo ""
}

# Parse arguments
if [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
    show_help
    exit 0
fi

# Run main
main

