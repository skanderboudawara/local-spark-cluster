#!/bin/bash

BASE_PATH="//src/scripts"

deploy() {
    echo "Deploying the Spark cluster..."
    docker compose up -d --build
    if [ $? -eq 0 ]; then
        echo "✅ Spark cluster successfully deployed!"
    else
        echo "❌ Failed to deploy the Spark cluster." >&2
        exit 1
    fi
}

stop() {
    echo "Stopping the Spark cluster..."
    docker compose down
    if [ $? -eq 0 ]; then
        echo "✅ Spark cluster successfully stopped!"
    else
        echo "❌ Failed to stop the Spark cluster." >&2
        exit 1
    fi
    exit /b
}

status() {
    echo "Checking Spark cluster status..."
    docker compose ps
    exit /b
}

run() {
    if [ -z "$1" ]; then
        echo "Please provide a file to run."
        exit 1
    fi
    FILE_PATH="${BASE_PATH}/$1"
    echo "Running Spark job from file: $1"
    docker exec -it spark-cluster-spark-master-1 spark-submit "$FILE_PATH"
    if [ $? -eq 0 ]; then
        echo "✅ Successfully executed Spark job: $1"
    else
        echo "❌ Failed to execute Spark job: $1" >&2
        exit 1
    fi
}

venv() {
    echo "Setting up Python environment..."
    python -m venv .venv
    source .venv/bin/activate
    pip install -e .
    if [ $? -eq 0 ]; then
        echo "✅ Python environment successfully set up!"
    else
        echo "❌ Failed to set up Python environment." >&2
        exit 1
    fi
    deactivate
}

# Check for command line arguments
if [ "$#" -lt 1 ]; then
    echo "Usage: $0 {deploy|stop|status|run|}"
    exit 1
fi

# Parse the command
case "$1" in
    deploy)
        deploy
        ;;
    stop)
        stop
        ;;
    status)
        status
        ;;
    run)
        shift
        run "$@"
        ;;
    venv)
        venv
        ;;
    *)
        echo "Invalid command. Use deploy, run, or test."
        exit 1
        ;;
esac
