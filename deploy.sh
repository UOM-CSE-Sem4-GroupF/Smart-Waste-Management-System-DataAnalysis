#!/bin/bash
# Deployment script for Waste Management System
# Usage: ./deploy.sh [local|docker|server] [start|stop|restart]

set -e

DEPLOYMENT_MODE=${1:-local}
ACTION=${2:-start}

case $DEPLOYMENT_MODE in
    local)
        echo "Deploying in LOCAL mode (Python scripts on Windows, Docker services running)"
        cp .env.local .env 2>/dev/null || cp .env.example .env
        ;;
    docker)
        echo "Deploying in DOCKER mode (All services in Docker containers)"
        cp .env.docker .env
        ;;
    server)
        echo "Deploying in SERVER mode (Configure .env.server with your server IP)"
        cp .env.server .env
        echo "⚠️  UPDATE .env.server with your server IP/domain before continuing!"
        ;;
    *)
        echo "Invalid deployment mode: $DEPLOYMENT_MODE"
        echo "Usage: $0 [local|docker|server]"
        exit 1
        ;;
esac

echo "Configuration copied to .env"
echo ""
echo "Deployment configuration:"
cat .env | grep -E "^[A-Z_]+" | head -10
echo ""

case $ACTION in
    start)
        echo "Starting services..."
        docker-compose up -d
        echo "✓ Services started"
        docker-compose ps
        ;;
    stop)
        echo "Stopping services..."
        docker-compose down
        echo "✓ Services stopped"
        ;;
    restart)
        echo "Restarting services..."
        docker-compose down
        docker-compose up -d
        echo "✓ Services restarted"
        ;;
    *)
        echo "Invalid action: $ACTION"
        echo "Usage: $0 [local|docker|server] [start|stop|restart]"
        exit 1
        ;;
esac
