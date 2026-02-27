#!/usr/bin/env bash

# Sparkle Development - Utility Script
# Usage: ./utilities.sh <command> [arguments]

set -e

# Podman compose command - use python module if command not in PATH
if command -v podman-compose > /dev/null 2>&1; then
    PODMAN_COMPOSE="podman-compose"
else
    PODMAN_COMPOSE="python3 -m podman_compose"
fi

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Helper function to print colored messages
print_info() {
    echo -e "${BLUE}ℹ${NC} $1"
}

print_success() {
    echo -e "${GREEN}✓${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

# Display help message
help() {
    cat << EOF
Sparkle Development - Utility Commands
=================================================

Usage: ./utilities.sh <command> [arguments]

Commands:
  build          - Build the Podman container
  up             - Start the environment
  down           - Stop the environment
  restart        - Restart the environment
  logs           - Show container logs
  shell          - Open bash shell in container
  spark-shell    - Open Spark shell
  pyspark        - Open PySpark shell
  submit <job>   - Submit a Spark job (e.g., submit scripts/example_spark_job.py)
  jupyter        - Open Jupyter Lab in browser
  status         - Show status of containers
  clean          - Remove containers and volumes
  help           - Show this help message

Examples:
  ./utilities.sh build
  ./utilities.sh up
  ./utilities.sh submit scripts/example_spark_job.py
  ./utilities.sh logs

EOF
}

# Build the Podman container
build() {
    print_info "Building Podman containers..."
    $PODMAN_COMPOSE build
    print_success "Build complete!"
}

# Start the environment
up() {
    print_info "Starting Sparkle environment..."
    $PODMAN_COMPOSE up -d

    echo ""
    print_success "Environment started!"
    echo ""
    echo "Access the following services:"
    echo "  📓 Jupyter Lab:          http://localhost:8888"
    echo "  📊 Spark Application UI: http://localhost:4040"
    echo "  ☁️ Azurite (Blob):       http://localhost:10000"
    echo ""
}

# Stop the environment
down() {
    print_info "Stopping Sparkle environment..."
    $PODMAN_COMPOSE down
    print_success "Environment stopped!"
}

# Restart the environment
restart() {
    print_info "Restarting environment..."
    down
    echo ""
    up
}

# Show container logs
logs() {
    print_info "Showing container logs (Ctrl+C to exit)..."
    $PODMAN_COMPOSE logs -f
}

# Open bash shell in container
shell() {
    print_info "Opening bash shell in sparkle container..."
    podman exec -it sparkle bash
}

# Open Spark shell
spark_shell() {
    print_info "Opening Spark shell..."
    podman exec -it sparkle spark-shell
}

# Open PySpark shell
pyspark() {
    print_info "Opening PySpark shell..."
    podman exec -it sparkle pyspark
}

# Submit a Spark job
submit() {
    local job_path="$1"

    if [ -z "$job_path" ]; then
        print_error "Please specify a job path"
        echo ""
        echo "Usage: ./utilities.sh submit <path/to/script.py>"
        echo "Example: ./utilities.sh submit scripts/example_spark_job.py"
        exit 1
    fi

    if [ ! -f "$job_path" ]; then
        print_error "Job file not found: $job_path"
        exit 1
    fi

    print_info "Submitting Spark job: $job_path"
    podman exec -it sparkle spark-submit "/home/onyxia/work/$job_path"
    print_success "Job submission complete!"
}

# Open Jupyter Lab in browser
jupyter() {
    print_info "Opening Jupyter Lab at http://localhost:8888"

    # Try different browser opening commands based on OS
    if command -v xdg-open > /dev/null 2>&1; then
        xdg-open http://localhost:8888
    elif command -v open > /dev/null 2>&1; then
        open http://localhost:8888
    elif command -v start > /dev/null 2>&1; then
        start http://localhost:8888
    else
        print_warning "Could not detect browser command"
        echo "Please open http://localhost:8888 in your browser manually"
    fi
}

# Show status of containers
status() {
    print_info "Container status:"
    echo ""
    $PODMAN_COMPOSE ps
}

# Clean up everything
clean() {
    print_warning "This will remove all containers and volumes!"
    read -p "Are you sure? (y/N) " -n 1 -r
    echo

    if [[ $REPLY =~ ^[Yy]$ ]]; then
        print_info "Cleaning up containers and volumes..."
        $PODMAN_COMPOSE down -v

        print_success "Cleanup complete!"
        print_info "Note: The base image (inseefrlab/onyxia-vscode-pyspark) is retained."
        print_info "To remove it, run: podman rmi inseefrlab/onyxia-vscode-pyspark:py3.12.12-spark4.0.1-2025.12.29"
    else
        print_info "Cleanup cancelled"
    fi
}

# Main function to route commands
main() {
    local command="${1:-help}"

    case "$command" in
        build)
            build
            ;;
        up)
            up
            ;;
        down)
            down
            ;;
        restart)
            restart
            ;;
        logs)
            logs
            ;;
        shell)
            shell
            ;;
        spark-shell)
            spark_shell
            ;;
        pyspark)
            pyspark
            ;;
        submit)
            submit "$2"
            ;;
        jupyter)
            jupyter
            ;;
        status)
            status
            ;;
        clean)
            clean
            ;;
        help|--help|-h)
            help
            ;;
        *)
            print_error "Unknown command: $command"
            echo ""
            help
            exit 1
            ;;
    esac
}

# Run main function with all arguments
main "$@"
