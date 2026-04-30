# Smart Waste Management System - Data Analysis Layer
# Comprehensive Startup and Integration Script (Windows PowerShell)
# Group F2 - Data Layer

param(
    [string]$Profile = ""
)

# Colors
$GREEN = "`e[0;32m"
$YELLOW = "`e[1;33m"
$RED = "`e[0;31m"
$BLUE = "`e[0;34m"
$NC = "`e[0m"

function Write-Info($message) {
    Write-Host "${GREEN}✓${NC} $message"
}

function Write-Warn($message) {
    Write-Host "${YELLOW}⚠${NC} $message"
}

function Write-Error($message) {
    Write-Host "${RED}✗${NC} $message"
}

function Write-Section($title) {
    Write-Host ""
    Write-Host "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    Write-Host "${BLUE}$title${NC}"
    Write-Host "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
}

function Check-Docker {
    try {
        $version = docker --version
        Write-Info "Docker found: $version"
    }
    catch {
        Write-Error "Docker is not installed"
        exit 1
    }
}

function Check-DockerCompose {
    try {
        $version = docker-compose --version
        Write-Info "Docker Compose found: $version"
    }
    catch {
        Write-Error "Docker Compose is not installed"
        exit 1
    }
}

function Check-EnvFile {
    if (-not (Test-Path ".env")) {
        Write-Error ".env file not found"
        Write-Info "Creating .env from template..."
        if (Test-Path ".env.example") {
            Copy-Item ".env.example" ".env"
            Write-Info ".env created from template"
        }
        else {
            Write-Error "No .env or .env.example found"
            exit 1
        }
    }
    Write-Info ".env file exists"
}

function Cleanup-StaleContainers {
    Write-Section "Cleaning up stale containers and networks"
    
    $staleContainers = @("waste-postgres", "waste-kafka", "waste-zookeeper")
    foreach ($container in $staleContainers) {
        $exists = docker ps -a --format '{{.Names}}' | Where-Object { $_ -eq $container }
        if ($exists) {
            Write-Warn "Found stale container: $container, removing..."
            docker rm -f $container 2>$null | Out-Null
        }
    }
    
    Write-Info "Cleanup complete"
}

function Start-Services {
    Write-Section "Starting Data Analysis Services"
    
    if ($Profile) {
        Write-Info "Starting with profile: $Profile"
        docker-compose --profile $Profile up -d --build
    }
    else {
        Write-Info "Starting with default profile (production)"
        docker-compose up -d --build
    }
    
    Write-Info "Containers started"
}

function Wait-ForService {
    param(
        [string]$service,
        [int]$maxAttempts = 30
    )
    
    Write-Host -NoNewline "Waiting for $service..."
    $attempt = 1
    
    while ($attempt -le $maxAttempts) {
        $running = docker-compose ps | Select-String "waste-$service.*Up"
        if ($running) {
            Write-Host -ForegroundColor Green " ✓"
            return 0
        }
        Write-Host -NoNewline "."
        Start-Sleep -Seconds 2
        $attempt++
    }
    
    Write-Host -ForegroundColor Red " ✗"
    Write-Error "$service failed to start"
    return 1
}

function Wait-ForKafka {
    Write-Info "Waiting for Kafka to be ready..."
    $maxAttempts = 30
    $attempt = 1
    
    while ($attempt -le $maxAttempts) {
        try {
            $output = docker-compose exec -T kafka kafka-broker-api-versions --bootstrap-server localhost:29092 2>&1
            Write-Info "Kafka is ready"
            return 0
        }
        catch {
            Write-Host -NoNewline "."
            Start-Sleep -Seconds 2
            $attempt++
        }
    }
    
    Write-Error "Kafka failed to become ready"
    return 1
}

function Verify-Services {
    Write-Section "Verifying Service Health"
    
    $services = @("kafka", "postgres-waste", "influxdb", "mlflow", "ml-service", "flink-processor", "route-optimizer", "airflow")
    
    foreach ($service in $services) {
        $running = docker-compose ps | Select-String "waste-$service.*Up"
        if ($running) {
            Write-Info "$service is running"
        }
        else {
            Write-Warn "$service is not running (may be optional)"
        }
    }
}

function Test-KafkaConnectivity {
    Write-Section "Testing Kafka Connectivity"
    
    Write-Info "Listing Kafka topics..."
    try {
        $topics = docker-compose exec -T kafka kafka-topics --list --bootstrap-server kafka:29092
        Write-Info "Kafka connectivity verified"
        Write-Host $topics
    }
    catch {
        Write-Error "Could not list Kafka topics: $_"
        return 1
    }
}

function Test-PostgreSQLConnectivity {
    Write-Section "Testing PostgreSQL Connectivity"
    
    Write-Info "Checking PostgreSQL for waste management..."
    try {
        docker-compose exec -T postgres-waste pg_isready -U waste_admin | Out-Null
        Write-Info "PostgreSQL connectivity verified"
    }
    catch {
        Write-Error "PostgreSQL not responding: $_"
        return 1
    }
}

function Show-Endpoints {
    Write-Section "Service Endpoints"
    
    Write-Host ""
    Write-Host "${GREEN}Management UIs:${NC}"
    Write-Host "  Airflow:     ${BLUE}http://localhost:8080${NC} (admin/admin)"
    Write-Host "  MLflow:      ${BLUE}http://localhost:5000${NC}"
    Write-Host "  ML-Service:  ${BLUE}http://localhost:8000/docs${NC}"
    Write-Host ""
    
    Write-Host "${GREEN}Database Connections:${NC}"
    Write-Host "  PostgreSQL (waste): ${BLUE}postgres://waste_admin@localhost:5432/waste_management${NC}"
    Write-Host "  PostgreSQL (airflow): ${BLUE}postgres://airflow@localhost:5432/airflow${NC}"
    Write-Host "  InfluxDB:   ${BLUE}http://localhost:8086${NC}"
    Write-Host ""
    
    Write-Host "${GREEN}Message Broker:${NC}"
    Write-Host "  Kafka:      ${BLUE}localhost:9092${NC}"
    Write-Host "  Zookeeper:  ${BLUE}localhost:2181${NC}"
    Write-Host ""
}

# Main execution
Clear-Host
Write-Host "${BLUE}================================================${NC}"
Write-Host "${BLUE}Smart Waste Management System - Data Analysis${NC}"
Write-Host "${BLUE}================================================${NC}"
Write-Host ""

Check-Docker
Check-DockerCompose
Check-EnvFile
Cleanup-StaleContainers
Start-Services

Wait-ForService "kafka"
Wait-ForService "postgres-waste"
Wait-ForService "influxdb"
Wait-ForService "mlflow"

Wait-ForKafka
Verify-Services

Test-KafkaConnectivity
Test-PostgreSQLConnectivity

Show-Endpoints

Write-Section "Setup Complete!"
Write-Host "${GREEN}Data Analysis layer is ready for development.${NC}"
Write-Host "Run ${BLUE}docker-compose logs -f${NC} to see logs"
Write-Host ""
