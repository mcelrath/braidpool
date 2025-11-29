#!/bin/sh

PID_FILE="braidpool.pid"
LOG_FILE="braidpool.log"

# Outputs all logs with levels debug and higher
export CARGO_LOG=debug # cargo generate-lockfile

# Don't forget that you can filter by module as well
#export CARGO_LOG=cargo::core::resolver=trace cargo generate-lockfile

# This will print lots of info about the download process. `trace` prints even more.
#export CARGO_HTTP_DEBUG=true CARGO_LOG=network=debug cargo fetch

# This is an important command for diagnosing fingerprint issues.
#export CARGO_LOG=cargo::core::compiler::fingerprint=trace cargo build

export RUST_LOG=debug

start() {
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if ps -p "$PID" > /dev/null 2>&1; then
            echo "Braidpool boot node is already running (PID: $PID)"
            return 1
        else
            echo "Stale PID file found, removing..."
            rm -f "$PID_FILE"
        fi
    fi

    echo "Starting Braidpool boot node and logging to $LOG_FILE..."
    cargo run -- --network cpunet > "$LOG_FILE" 2>&1 &
    PID=$!
    echo $PID > "$PID_FILE"
    echo "Boot node started (PID: $PID)"
    echo "Most recent log messages are:"
    echo
    tail "$LOG_FILE"
}

stop() {
    if [ ! -f "$PID_FILE" ]; then
        echo "PID file not found. Boot node may not be running."
        return 1
    fi

    PID=$(cat "$PID_FILE")
    if ps -p "$PID" > /dev/null 2>&1; then
        echo "Stopping Braidpool boot node (PID: $PID)..."
        kill "$PID"
        rm -f "$PID_FILE"
        echo "Boot node stopped."
    else
        echo "Process $PID is not running. Removing stale PID file."
        rm -f "$PID_FILE"
    fi
}

restart() {
    echo "Restarting Braidpool boot node..."
    stop
    sleep 2
    start
}

status() {
    if [ ! -f "$PID_FILE" ]; then
        echo "PID file not found. Boot node is not running."
        return 1
    fi

    PID=$(cat "$PID_FILE")
    if ps -p "$PID" > /dev/null 2>&1; then
        echo "Braidpool boot node is running (PID: $PID)"
        echo "Recent log messages:"
        echo
        tail "$LOG_FILE"
    else
        echo "Process $PID is not running. Boot node is stopped."
        echo "Removing stale PID file."
        rm -f "$PID_FILE"
        return 1
    fi
}

# Parse command line arguments
case "$1" in
    start)
        start
        ;;
    stop)
        stop
        ;;
    restart)
        restart
        ;;
    status)
        status
        ;;
    *)
        echo "Usage: $0 {start|stop|restart|status}"
        echo "  start   - Start the Braidpool boot node"
        echo "  stop    - Stop the Braidpool boot node"
        echo "  restart - Restart the Braidpool boot node"
        echo "  status  - Check if the boot node is running and show recent logs"
        exit 1
        ;;
esac
