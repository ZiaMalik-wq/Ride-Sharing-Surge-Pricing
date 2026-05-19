#!/bin/bash

# Ride Sharing Surge Pricing Pipeline Startup Script
# Starts all services: Docker, Flink, Producer, Sinks, API, and Dashboard

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
LOG_FILE="$SCRIPT_DIR/pipeline.log"
SKIP_BROWSER=false
SKIP_DASHBOARD=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --skip-browser)
            SKIP_BROWSER=true
            shift
            ;;
        --skip-dashboard)
            SKIP_DASHBOARD=true
            shift
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Color codes
GREEN='\033[0;32m'
CYAN='\033[0;36m'
RED='\033[0;31m'
NC='\033[0m' # No Color

function log() {
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    local message="[$timestamp] $1"
    echo -e "${CYAN}${message}${NC}"
    echo "$message" >> "$LOG_FILE"
}

function log_error() {
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    local message="[$timestamp] ERROR: $1"
    echo -e "${RED}${message}${NC}"
    echo "$message" >> "$LOG_FILE"
}

function wait_for_service() {
    local name=$1
    local test_command=$2
    local max_attempts=${3:-30}
    local wait_seconds=${4:-2}
    
    local attempts=0
    while [ $attempts -lt $max_attempts ]; do
        if eval "$test_command" &>/dev/null; then
            log "$name is ready"
            return 0
        fi
        
        attempts=$((attempts + 1))
        if [ $attempts -lt $max_attempts ]; then
            sleep $wait_seconds
        fi
    done
    
    log_error "$name did not become ready after $((max_attempts * wait_seconds)) seconds"
    return 1
}

# Clear log file
> "$LOG_FILE"

log "=========================================="
log "Ride Sharing Surge Pricing Pipeline Start"
log "=========================================="

# Check if Python venv exists
if [ ! -d "$SCRIPT_DIR/.venv" ]; then
    log_error "Python virtual environment not found at $SCRIPT_DIR/.venv"
    log "Please run: python -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt"
    exit 1
fi

# Activate venv
source "$SCRIPT_DIR/.venv/bin/activate" || true

# 1. Start Docker Compose
log "Step 1/9: Starting Docker containers..."
cd "$SCRIPT_DIR"
if docker-compose -f docker/docker-compose.yml up -d; then
    log "Docker containers started"
else
    log_error "Failed to start Docker Compose"
    exit 1
fi

# 2. Wait for Kafka to be ready
log "Step 2/9: Waiting for Kafka to be ready..."
wait_for_service "Kafka" "docker exec surge_kafka kafka-broker-api-versions.sh --bootstrap-server localhost:9092 2>&1 | grep -q 'ApiVersion'" 60 3

# 3. Wait for Redis to be ready
log "Step 3/9: Waiting for Redis to be ready..."
wait_for_service "Redis" "docker exec surge_redis redis-cli ping 2>&1 | grep -q 'PONG'" 30 2

# 4. Wait for Cassandra to be ready
log "Step 4/9: Waiting for Cassandra to be ready..."
wait_for_service "Cassandra" "docker exec surge_cassandra cqlsh -e 'DESCRIBE CLUSTER;' 2>&1 | grep -q 'Cluster Information'" 60 3 || true

# 5. Initialize Cassandra schema
log "Step 5/9: Initializing Cassandra schema..."
if docker exec surge_cassandra cqlsh -f /opt/flink/project/src/scripts/init_cassandra.cql 2>&1; then
    log "Cassandra schema initialized"
else
    log "Cassandra schema initialization completed (may continue in background)"
fi

# 6. Submit Flink job
log "Step 6/9: Submitting Flink processor job..."
if flink_output=$(docker exec surge_flink_jobmanager flink run -py /opt/flink/project/src/processor/processor.py 2>&1); then
    if [[ $flink_output =~ JobID\ ([a-f0-9]+) ]]; then
        log "Flink job submitted with JobID: ${BASH_REMATCH[1]}"
    else
        log "Flink job submitted: $flink_output"
    fi
    sleep 3
else
    log_error "Failed to submit Flink job"
fi

# 7. Start Redis Sink (background)
log "Step 7/9: Starting Redis Sink..."
python "$SCRIPT_DIR/src/processor/redis_sink.py" > /tmp/redis_sink.log 2>&1 &
REDIS_SINK_PID=$!
log "Redis Sink started (PID: $REDIS_SINK_PID)"

# 8. Start Cassandra Sink (background)
log "Step 8/9: Starting Cassandra Sink..."
python "$SCRIPT_DIR/src/processor/cassandra_sink.py" > /tmp/cassandra_sink.log 2>&1 &
CASSANDRA_SINK_PID=$!
log "Cassandra Sink started (PID: $CASSANDRA_SINK_PID)"

# 9a. Start Producer (background)
log "Step 9a/9: Starting Data Producer..."
python "$SCRIPT_DIR/src/generator/producer.py" > /tmp/producer.log 2>&1 &
PRODUCER_PID=$!
log "Producer started (PID: $PRODUCER_PID)"

# 9b. Start FastAPI (background)
log "Step 9b/9: Starting FastAPI server..."
python -m uvicorn src.api.main:app --host 127.0.0.1 --port 8000 > /tmp/fastapi.log 2>&1 &
API_PID=$!
log "FastAPI server started (PID: $API_PID)"
sleep 2

# 10. Open Dashboard
log "Step 10/10: Dashboard setup..."
if [ "$SKIP_DASHBOARD" = false ] && [ "$SKIP_BROWSER" = false ]; then
    if command -v xdg-open &> /dev/null; then
        xdg-open "http://127.0.0.1:5500" || true
    elif command -v open &> /dev/null; then
        open "http://127.0.0.1:5500" || true
    fi
    log "Dashboard should open in browser"
else
    log "Dashboard: http://127.0.0.1:5500 (skipped auto-open)"
fi

log ""
log "=========================================="
log "Pipeline started successfully!"
log "=========================================="
log ""
log "Services:"
log "  - Kafka:           localhost:9092"
log "  - Redis:           localhost:6379"
log "  - Cassandra:       localhost:9042"
log "  - Flink UI:        http://localhost:8081"
log "  - FastAPI:         http://localhost:8000"
log "  - Dashboard:       http://127.0.0.1:5500"
log ""
log "Background processes:"
log "  - Producer:        PID $PRODUCER_PID"
log "  - Redis Sink:      PID $REDIS_SINK_PID"
log "  - Cassandra Sink:  PID $CASSANDRA_SINK_PID"
log "  - FastAPI:         PID $API_PID"
log ""
log "To stop the pipeline:"
log "  1. Press Ctrl+C"
log "  2. Or run: docker-compose down"
log ""
log "Log file: $LOG_FILE"
log "Process logs:"
log "  - Producer:      /tmp/producer.log"
log "  - Redis Sink:    /tmp/redis_sink.log"
log "  - Cassandra Sink: /tmp/cassandra_sink.log"
log "  - FastAPI:       /tmp/fastapi.log"
log ""

# Keep script running and handle cleanup
trap "
    log 'Stopping pipeline...'
    kill $PRODUCER_PID $REDIS_SINK_PID $CASSANDRA_SINK_PID $API_PID 2>/dev/null || true
    log 'Pipeline stopped'
    exit 0
" SIGINT SIGTERM

log "Pipeline is running. Press Ctrl+C to stop all services..."
wait
