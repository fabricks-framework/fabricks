
#!/usr/bin/env bash

# Exit on error
set -e

# Color codes
RED="\033[1;31m"
GREEN="\033[1;32m"
YELLOW="\033[1;33m"
BLUE="\033[1;34m"
MAGENTA="\033[1;35m"
CYAN="\033[1;36m"
RESET="\033[0m"

# Logging helpers
log_info() {
	local msg="$1"
	echo -e "${BLUE}[$(date '+%Y-%m-%d %H:%M:%S')] [INFO] $msg${RESET}"
}

log_warn() {
	local msg="$1"
	echo -e "${YELLOW}[$(date '+%Y-%m-%d %H:%M:%S')] [WARN] $msg${RESET}"
}

log_error() {
	local msg="$1"
	echo -e "${RED}[$(date '+%Y-%m-%d %H:%M:%S')] [ERROR] $msg${RESET}"
}

log_section() {
	local msg="$1"
	echo -e "${CYAN}========== $msg ==========${RESET}"
}

log_success() {
	local msg="$1"
	echo -e "${GREEN}[$(date '+%Y-%m-%d %H:%M:%S')] [SUCCESS] $msg${RESET}"
}

install_dependencies(){
    log_section "Installing Python dev & test dependencies"
    uv sync --group dev --group test || log_error "Failed to install Python dependencies"
}

check_dependencies(){
    log_section "Checking dependencies"

    log_info "Running deptry..."
    uv run deptry . || log_error "deptry found missing dependencies"
    log_success "All dependencies are satisfied"
}

format_python() {
	log_section "Python Formatting Started"

	log_info "Running autoflake..."
	uv run autoflake -r -i . || log_error "autoflake failed"

	log_info "Running isort..."
	uv run isort . || log_error "isort failed"

	log_info "Running pycln..."
	uv run pycln . || log_warn "pycln failed (optional)"

	log_info "Running ruff format..."
	uv run ruff format . || log_error "ruff format failed"

	log_info "Running ruff check..."
	uv run ruff check . --fix || log_error "ruff check failed"

	log_info "Running pyright..."
	uv run pyright . || log_warn "pyright found issues"

	log_success "Python formatting completed"
}

format_sql() {
    log_section "SQL Formatting Started"

    log_info "Running sqlfmt..."
    uv run sqlfmt .

    log_success "SQL formatting completed"
}

format_yaml() {
    log_section "YAML Formatting Started"

    log_info "Running yamlfix..."
    uv run yamlfix . --exclude .venv --exclude .dev --exclude .idea --include *.yml

    log_success "YAML formatting completed"
}

format_all() {
    format_python
    format_sql
    format_yaml
}

format_commit(){
    format

    log_section "Committing formatted code..."

    git add .
    git commit -m "chore: format code"

    log_success "Formatted code committed"
}

show_help() {
		echo -e "   ${MAGENTA}Usage: $0 <command>${RESET}"
		echo -e "   ${CYAN}Available commands:${RESET}"
		echo -e "   ${GREEN}format-python${RESET}            - Run Python code formatters and linters"
        echo -e "   ${GREEN}format-sql${RESET}               - Run SQL formatter"
        echo -e "   ${GREEN}format-yaml${RESET}              - Run YAML formatter"
        echo -e "   ${GREEN}format-all${RESET}               - Run all formatters"
        echo -e "   ${GREEN}format-commit${RESET}            - Format code and commit changes"
        echo -e "   ${GREEN}install-dependencies${RESET}     - Install dev and test dependencies"
		echo -e "   ${GREEN}check-dependencies${RESET}       - Check for missing dependencies"
		echo -e "   ${GREEN}help${RESET}                     - Show help"
}

main() {
    if [ $# -eq 0 ]; then
        show_help
        exit 1
    fi

    local command="$1"
    shift

    case "$command" in
        format-python)
            format_python "$@"
            ;;
        format-sql)
            format_sql "$@"
            ;;
        format-yaml)
            format_yaml "$@"
            ;;
        format-all)
            format_all "$@"
            ;;
        format-commit)
            format_commit "$@"
            ;;
        install-dependencies)
            install_dependencies "$@"
            ;;
        check-dependencies)
            check_dependencies "$@"
            ;;
        help|*)
            show_help
            ;;
    esac
}

main "$@"
