@echo off
REM Deployment script for Windows
REM Usage: deploy.bat [local|docker|server] [start|stop|restart]

setlocal enabledelayedexpansion

if "%~1"=="" (
    set DEPLOYMENT_MODE=local
) else (
    set DEPLOYMENT_MODE=%~1
)

if "%~2"=="" (
    set ACTION=start
) else (
    set ACTION=%~2
)

echo Deploying in %DEPLOYMENT_MODE% mode...

if "%DEPLOYMENT_MODE%"=="local" (
    echo Deploying in LOCAL mode (Python scripts on Windows, Docker services running^)
    copy .env .env.backup >nul 2>&1
    type .env.example > .env 2>nul
) else if "%DEPLOYMENT_MODE%"=="docker" (
    echo Deploying in DOCKER mode (All services in Docker containers^)
    copy .env.docker .env
) else if "%DEPLOYMENT_MODE%"=="server" (
    echo Deploying in SERVER mode (Configure .env.server with your server IP^)
    copy .env.server .env
    echo WARNING: UPDATE .env.server with your server IP/domain before continuing!
) else (
    echo Invalid deployment mode: %DEPLOYMENT_MODE%
    echo Usage: deploy.bat [local^|docker^|server] [start^|stop^|restart]
    exit /b 1
)

echo Configuration copied to .env

if "%ACTION%"=="start" (
    echo Starting services...
    docker-compose up -d
    echo Services started
    docker-compose ps
) else if "%ACTION%"=="stop" (
    echo Stopping services...
    docker-compose down
    echo Services stopped
) else if "%ACTION%"=="restart" (
    echo Restarting services...
    docker-compose down
    docker-compose up -d
    echo Services restarted
) else (
    echo Invalid action: %ACTION%
    echo Usage: deploy.bat [local^|docker^|server] [start^|stop^|restart]
    exit /b 1
)
