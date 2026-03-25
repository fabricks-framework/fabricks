
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

log() {
	local msg="$1"
	echo -e "${BLUE}$msg${RESET}"
}

warn() {
	local msg="$1"
	echo -e "${RED}$msg${RESET}"
}

header() {
	local msg="$1"
	echo -e "${CYAN}=============== $msg ===============${RESET}"
}

install_prettier() {
    header "installing prettier"

    if npm list prettier --depth=0 >/dev/null 2>&1; then
        log "prettier is already installed"
    else
        log "installing prettier..."
        npm install --save-dev prettier || {
            warn "failed to install prettier"
            exit 1
        }
        log "prettier installed successfully"
    fi
}

install_dependencies(){
    header "installing uv dependencies"
    uv sync --all-groups || warn "failed to install uv dependencies"

    install_prettier
}

check_dependencies(){
    header "checking dependencies"

    log "running deptry..."
    uv run deptry . || warn "deptry found missing dependencies"
    log "all dependencies are satisfied"
}

format_python() {
	local target_dir="${1:-.}"
	header "python formatting started (target: $target_dir)"

	log "running autoflake..."
	uv run autoflake -r -i "$target_dir" || warn "autoflake failed"

	log "running isort..."
	uv run isort "$target_dir" || warn "isort failed"

	log "running pycln..."
	uv run pycln "$target_dir" || warn "pycln failed (optional)"

	log "running ruff format..."
	uv run ruff format "$target_dir" || warn "ruff format failed"

	log "running ruff check..."
	uv run ruff check "$target_dir" --fix || warn "ruff check failed"

    log "running basedpyright..."
    uv run basedpyright "$target_dir" --level error || warn "basedpyright found issues"

	log "Python formatting completed"
}

format_sql() {
    local target_dir="${1:-.}"
    header "sql formatting started (target: $target_dir)"

    log "running sqlfmt..."
    uv run sqlfmt "$target_dir"

    log "SQL formatting completed"
}

format_yaml() {
    local target_dir="${1:-.}"
    header "yaml formatting started (target: $target_dir)"

    log "running yamlfix..."
    uv run yamlfix "$target_dir" --exclude .venv --exclude .dev --exclude .idea --include *.yml

    log "YAML formatting completed"
}

format_prettier() {
    local target_dir="${1:-.}"
    header "prettier formatting started (target: $target_dir)"

    if [ "$target_dir" = "." ]; then
        npx prettier --write "{,*/**/}*.{ts,tsx,js,jsx,css,scss,json,md}" || {
            warn "prettier failed"
            exit 1
        }
    else
        npx prettier --write "$target_dir/**/*.{ts,tsx,js,jsx,css,scss,json,md}" || {
            warn "prettier failed"
            exit 1
        }
    fi
    log "prettier formatting completed"
}

format_all() {
    local target_dir="${1:-.}"
    format_python "$target_dir"
    format_sql "$target_dir"
    format_yaml "$target_dir"
    format_prettier "$target_dir"
}

format_commit(){
    local target_dir="${1:-.}"
    format_all "$target_dir"

    header "committing formatted code..."

    git add .
    git commit -m "chore: format code"

    log "formatted code committed"
}

show_help() {
		echo -e "   ${MAGENTA}Usage: $0 <command> [folder]${RESET}"
		echo -e "   ${CYAN}Available commands:${RESET}"
		echo -e "       - ${GREEN}python [folder]${RESET}   : Run Python code formatters and linters"
        echo -e "       - ${GREEN}sql [folder]${RESET}      : Run SQL formatter"
        echo -e "       - ${GREEN}yaml [folder]${RESET}     : Run YAML formatter"
        echo -e "       - ${GREEN}all [folder]${RESET}      : Run all formatters"
        echo -e "       - ${GREEN}prettier [folder]${RESET} : Run prettier for all non-Python files"
        echo -e "       - ${GREEN}commit [folder]${RESET}   : Format code and commit changes"
        echo -e "       - ${GREEN}install-dependencies${RESET}     : Install dev and test dependencies"
		echo -e "       - ${GREEN}check-dependencies${RESET}       : Check for missing dependencies"
		echo -e "       - ${GREEN}help${RESET}                     : Show help"
		echo ""
		echo -e "   ${CYAN}Examples:${RESET}"
		echo -e "       $0 python                    # Format all Python files in current directory"
		echo -e "       $0 python invokers/powerbi   # Format Python files in specific folder"
		echo -e "       $0 all src                   # Run all formatters on src folder"
}

main() {
    if [ $# -eq 0 ]; then
        show_help
        exit 1
    fi

    local command="$1"
    shift

    case "$command" in
        python)
            format_python "$@"
            ;;
        sql)
            format_sql "$@"
            ;;
        prettier)
            format_prettier "$@"
            ;;
        yaml)
            format_yaml "$@"
            ;;
        all)
            format_all "$@"
            ;;
        commit)
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