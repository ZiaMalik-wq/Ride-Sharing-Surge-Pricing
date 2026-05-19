# Ride Sharing Surge Pricing Pipeline Startup Script
# Starts all services: Docker, Flink, Producer, Sinks, API, and Dashboard

param(
    [switch]$SkipBrowser = $false,
    [switch]$SkipDashboard = $false
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$LogFile = "$ScriptDir\pipeline.log"

function Log {
    param([string]$Message)
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $logEntry = "[$timestamp] $Message"
    Write-Host $logEntry -ForegroundColor Cyan
    Add-Content -Path $LogFile -Value $logEntry
}

function LogError {
    param([string]$Message)
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $logEntry = "[$timestamp] ERROR: $Message"
    Write-Host $logEntry -ForegroundColor Red
    Add-Content -Path $LogFile -Value $logEntry
}

function WaitForService {
    param(
        [string]$Name,
        [string]$TestCommand,
        [int]$MaxAttempts = 30,
        [int]$WaitSeconds = 2
    )
    
    $attempts = 0
    while ($attempts -lt $MaxAttempts) {
        try {
            $result = Invoke-Expression $TestCommand -ErrorAction SilentlyContinue
            if ($result) {
                Log "$Name is ready"
                return $true
            }
        } catch {
            # Service not ready yet
        }
        
        $attempts++
        if ($attempts -lt $MaxAttempts) {
            Start-Sleep -Seconds $WaitSeconds
        }
    }
    
    LogError "$Name did not become ready after $($MaxAttempts * $WaitSeconds) seconds"
    return $false
}

# Clear log file
"" | Set-Content -Path $LogFile

Log "=========================================="
Log "Ride Sharing Surge Pricing Pipeline Start"
Log "=========================================="

# 1. Start Docker Compose
Log "Step 0/9: Cleaning up old processes..."
Get-Process | Where-Object { $_.ProcessName -eq "python" -and ($_.MainWindowTitle -like "*uvicorn*" -or $_.CommandLine -like "*http.server*" -or $_.CommandLine -like "*producer.py*" -or $_.CommandLine -like "*_sink.py*") } | Stop-Process -Force -ErrorAction SilentlyContinue
# Wait a moment for ports to be released
Start-Sleep -Seconds 2

Log "Step 1/9: Starting Docker containers..."
try {
    # Check if docker is running
    docker info > $null 2>&1
    if ($LASTEXITCODE -ne 0) {
        LogError "Docker Desktop/Engine is not running. Please start Docker first."
        exit 1
    }

    Push-Location $ScriptDir
    try {
        # Prefer 'docker compose' (V2) but fallback to 'docker-compose' (V1)
        $usingComposeV2 = $true
        & docker compose version > $null 2>&1
        if ($LASTEXITCODE -ne 0) { $usingComposeV2 = $false }

        if ($usingComposeV2) {
            & docker compose -f docker/docker-compose.yml up -d --build
            if ($LASTEXITCODE -ne 0) { throw "Docker Compose exited with code $LASTEXITCODE" }
            Log "Docker containers started using docker compose"
        } else {
            & docker-compose -f docker/docker-compose.yml up -d --build
            if ($LASTEXITCODE -ne 0) { throw "Docker Compose exited with code $LASTEXITCODE" }
            Log "Docker containers started using docker-compose"
        }
    } finally {
        Pop-Location
    }
} catch {
    LogError "Failed to start Docker: $_"
    exit 1
}

# 2. Wait for Kafka to be ready
Log "Step 2/9: Waiting for Kafka to be ready..."
if (-not (WaitForService "Kafka" "Test-NetConnection -ComputerName localhost -Port 9092 -WarningAction SilentlyContinue | Select-Object -ExpandProperty TcpTestSucceeded" 30 2)) {
    LogError "Kafka failed to start"
    exit 1
}

# 3. Wait for Redis to be ready
Log "Step 3/9: Waiting for Redis to be ready..."
if (-not (WaitForService "Redis" "Test-NetConnection -ComputerName localhost -Port 6379 -WarningAction SilentlyContinue | Select-Object -ExpandProperty TcpTestSucceeded" 30 2)) {
    LogError "Redis failed to start"
    exit 1
}

# 4. Wait for Cassandra to be ready
Log "Step 4/9: Waiting for Cassandra to be ready (this may take up to 60s)..."
$cassandraReady = $false
for ($i = 0; $i -lt 30; $i++) {
    $check = docker exec surge_cassandra cqlsh -e "describe keyspaces" 2>&1
    if ($LASTEXITCODE -eq 0) {
        $cassandraReady = $true
        Log "Cassandra is ready"
        break
    }
    Start-Sleep -Seconds 3
}

if (-not $cassandraReady) {
    LogError "Cassandra failed to initialize CQL interface"
    exit 1
}

# 5. Initialize Cassandra schema
Log "Step 5/9: Initializing Cassandra schema..."
$schemaSuccess = $false
for ($i = 0; $i -lt 3; $i++) {
    try {
        # The project root is mounted to /opt/flink/project in the container
        docker exec -i surge_cassandra cqlsh -f /opt/flink/project/src/scripts/init_cassandra.cql
        if ($LASTEXITCODE -eq 0) {
            $schemaSuccess = $true
            Log "Cassandra schema initialized"
            break
        }
        
        # Fallback: try to pipe the file directly
        Get-Content "$ScriptDir\src\scripts\init_cassandra.cql" | docker exec -i surge_cassandra cqlsh
        if ($LASTEXITCODE -eq 0) {
            $schemaSuccess = $true
            Log "Cassandra schema initialized (via fallback)"
            break
        }
    } catch {
        Log "Schema init attempt $($i+1) failed, retrying..."
    }
    Start-Sleep -Seconds 5
}

if (-not $schemaSuccess) {
    LogError "Cassandra schema initialization failed after multiple attempts"
}

# 6. Pre-create Kafka topics (required for Flink earliest-offset mode)
Log "Step 6/9: Pre-creating Kafka topics..."
docker exec surge_kafka kafka-topics --create --if-not-exists --topic ride_requests --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker exec surge_kafka kafka-topics --create --if-not-exists --topic driver_updates --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker exec surge_kafka kafka-topics --create --if-not-exists --topic surge_pricing --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# 7. Submit Flink job
Log "Step 7/9: Submitting Flink processor job..."
try {
    $flinkOutput = docker exec surge_flink_jobmanager flink run -py /opt/flink/project/src/processor/processor.py 2>&1
    
    # Check for various JobID formats in output
    if ($flinkOutput -match "Job (ID |has been submitted with ID )?([a-f0-9]{32})") {
        $jobId = $matches[2]
        Log "Flink job submitted with JobID: $jobId"
    } elseif ($flinkOutput -match "JobID\s+:\s+([a-f0-9]+)") {
        $jobId = $matches[1]
        Log "Flink job submitted with JobID: $jobId"
    } else {
        Log "Flink job submission started (output: $($flinkOutput -join ' '))"
    }
    Start-Sleep -Seconds 3
} catch {
    LogError "Flink job submission error: $_"
    # Don't exit - job may still be running
}

# 8. Start Background Services (API and Sinks)
Log "Step 8/9: Starting Background Services..."

# 8a. Start Redis Sink (background process)
Log "Starting Redis Sink..."
try {
    $redisSinkProcess = Start-Process -FilePath "$ScriptDir\.venv\Scripts\python.exe" `
        -ArgumentList "src/processor/redis_sink.py" `
        -WorkingDirectory $ScriptDir `
        -PassThru `
        -WindowStyle Hidden
    Log "Redis Sink started (PID: $($redisSinkProcess.Id))"
} catch {
    LogError "Failed to start Redis Sink: $_"
}

# 8b. Start Cassandra Sink (background process)
Log "Starting Cassandra Sink..."
try {
    $cassandraSinkProcess = Start-Process -FilePath "$ScriptDir\.venv\Scripts\python.exe" `
        -ArgumentList "src/processor/cassandra_sink.py" `
        -WorkingDirectory $ScriptDir `
        -PassThru `
        -WindowStyle Hidden
    Log "Cassandra Sink started (PID: $($cassandraSinkProcess.Id))"
} catch {
    LogError "Failed to start Cassandra Sink: $_"
}

# 8c. Start FastAPI (background process)
Log "Starting FastAPI server..."
try {
    $apiProcess = Start-Process -FilePath "$ScriptDir\.venv\Scripts\python.exe" `
        -ArgumentList "-m", "uvicorn", "src.api.main:app", "--host", "127.0.0.1", "--port", "8000" `
        -WorkingDirectory $ScriptDir `
        -PassThru `
        -WindowStyle Hidden
    Log "FastAPI server started (PID: $($apiProcess.Id))"
    Start-Sleep -Seconds 2
} catch {
    LogError "Failed to start FastAPI: $_"
}

# 8d. Start Dashboard HTTP server (background process)
Log "Starting Dashboard HTTP server..."
try {
    $dashboardProcess = Start-Process -FilePath "$ScriptDir\.venv\Scripts\python.exe" `
        -ArgumentList "-m", "http.server", "5500" `
        -WorkingDirectory "$ScriptDir\frontend" `
        -PassThru `
        -WindowStyle Hidden
    Log "Dashboard server started (PID: $($dashboardProcess.Id))"
    Start-Sleep -Seconds 1
} catch {
    LogError "Failed to start Dashboard server: $_"
}

# 9. Start Data Producer
Log "Step 9/9: Starting Data Producer..."
try {
    $producerProcess = Start-Process -FilePath "$ScriptDir\.venv\Scripts\python.exe" `
        -ArgumentList "src/generator/producer.py" `
        -WorkingDirectory $ScriptDir `
        -PassThru `
        -WindowStyle Hidden
    Log "Producer started (PID: $($producerProcess.Id))"
} catch {
    LogError "Failed to start Producer: $_"
}

# 10. Open Dashboard (optional)
Log "Step 10/10: Opening Dashboard..."
if (-not $SkipDashboard) {
    # Wait for dashboard to be ready
    $dashboardReady = $false
    for ($i = 0; $i -lt 10; $i++) {
        try {
            $response = Invoke-WebRequest -Uri "http://127.0.0.1:5500" -TimeoutSec 2 -ErrorAction SilentlyContinue
            if ($response.StatusCode -eq 200) {
                $dashboardReady = $true
                break
            }
        } catch {
            Start-Sleep -Seconds 1
        }
    }
    
    if ($dashboardReady -and -not $SkipBrowser) {
        try {
            Start-Process "http://127.0.0.1:5500"
            Log "Dashboard opened in browser"
        } catch {
            LogError "Failed to open browser: $_"
        }
    }
}

Log ""
Log "=========================================="
Log "Pipeline started successfully!"
Log "=========================================="
Log ""
Log "Services:"
Log "  - Kafka:           localhost:9092"
Log "  - Redis:           localhost:6379"
Log "  - Cassandra:       localhost:9042"
Log "  - Flink UI:        http://localhost:8081"
Log "  - FastAPI:         http://localhost:8000"
Log "  - Dashboard:       http://127.0.0.1:5500"
Log ""
Log "Background processes:"
Log "  - Producer:        PID $($producerProcess.Id)"
Log "  - Redis Sink:      PID $($redisSinkProcess.Id)"
Log "  - Cassandra Sink:  PID $($cassandraSinkProcess.Id)"
Log "  - FastAPI:         PID $($apiProcess.Id)"
Log "  - Dashboard:       PID $($dashboardProcess.Id)"
Log ""
Log "To stop the pipeline:"
Log "  1. Close this script with Ctrl+C or close all background processes"
Log "  2. Run: docker-compose down"
Log ""
Log "Log file: $LogFile"
Log ""

# Keep script running
Log "Pipeline is running. Press Ctrl+C to stop all services..."
try {
    while ($true) {
        Start-Sleep -Seconds 10
        
        # Check if any critical processes have died
        if ($null -ne $apiProcess -and $apiProcess.HasExited) {
            LogError "FastAPI process has exited!"
        }
        if ($null -ne $producerProcess -and $producerProcess.HasExited) {
            LogError "Producer process has exited!"
        }
        if ($null -ne $redisSinkProcess -and $redisSinkProcess.HasExited) {
            LogError "Redis Sink process has exited!"
        }
        if ($null -ne $cassandraSinkProcess -and $cassandraSinkProcess.HasExited) {
            LogError "Cassandra Sink process has exited!"
        }
    }
} finally {
    Log "Stopping pipeline..."
    
    # Terminate all background processes
    if ($null -ne $apiProcess -and -not $apiProcess.HasExited) {
        $apiProcess | Stop-Process -Force -ErrorAction SilentlyContinue
        Log "Stopped FastAPI"
    }
    if ($null -ne $producerProcess -and -not $producerProcess.HasExited) {
        $producerProcess | Stop-Process -Force -ErrorAction SilentlyContinue
        Log "Stopped Producer"
    }
    if ($null -ne $redisSinkProcess -and -not $redisSinkProcess.HasExited) {
        $redisSinkProcess | Stop-Process -Force -ErrorAction SilentlyContinue
        Log "Stopped Redis Sink"
    }
    if ($null -ne $cassandraSinkProcess -and -not $cassandraSinkProcess.HasExited) {
        $cassandraSinkProcess | Stop-Process -Force -ErrorAction SilentlyContinue
        Log "Stopped Cassandra Sink"
    }
    if ($null -ne $dashboardProcess -and -not $dashboardProcess.HasExited) {
        $dashboardProcess | Stop-Process -Force -ErrorAction SilentlyContinue
        Log "Stopped Dashboard"
    }
    
    Log "Pipeline stopped"
}
