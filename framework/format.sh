
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


check_ty() {
    local target_dir="${1:-.}"

    uv run ty check "$target_dir"
}

standardize() {
    local target_dir="${1:-.}"

	uv run python standardize.py "$target_dir"
}

format_python() {
	local target_dir="${1:-.}"

    uv run ruff check --select I --fix "$target_dir"
	uv run ruff format "$target_dir"
	uv run ruff check --fix "$target_dir"

    check_ty
}

format_sql() {
    local target_dir="${1:-.}"

    uv run sqlfmt "$target_dir"

}

format_yaml() {
    local target_dir="${1:-.}"

    uv run yamlfix "$target_dir" --exclude .venv --exclude .dev --exclude .idea --include *.yml
}

format_prettier() {
    local target_dir="${1:-.}"

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
}

all() {
    local target_dir="${1:-.}"
    format_python "$target_dir"
    format_sql "$target_dir"
    format_yaml "$target_dir"
    format_prettier "$target_dir"
}

show_help() {
		echo -e "   ${MAGENTA}Usage: $0 <command> [folder]${RESET}"
		echo -e "   ${CYAN}Available commands:${RESET}"
		echo -e "       - ${GREEN}-p, --python [folder]${RESET}         : Run Python code formatters and linters"
        echo -e "       - ${GREEN}-s, --sql [folder]${RESET}            : Run SQL formatter"
        echo -e "       - ${GREEN}-y, --yaml [folder]${RESET}           : Run YAML formatter"
        echo -e "       - ${GREEN}-t, --ty [folder]${RESET}             : Run type checking with ty"
        echo -e "       - ${GREEN}-a, --all [folder]${RESET}            : Run all formatters"
        echo -e "       - ${GREEN}-P, --prettier [folder]${RESET}       : Run prettier for all non-Python files"
        echo -e "       - ${GREEN}-S, --standardize [folder]${RESET}    : Run standardize for all code"
		echo -e "       - ${GREEN}-h, --help${RESET}                    : Show help"
		echo ""
		echo -e "   ${CYAN}Examples:${RESET}"
		echo -e "       $0 -p                         # Format all Python files in current directory"
		echo -e "       $0 --python invokers/powerbi  # Format Python files in specific folder"
		echo -e "       $0 -a src                     # Run all formatters on src folder"
}

main() {
    if [ $# -eq 0 ]; then
        show_help
        exit 1
    fi

    case "$1" in
        -p|--python)       shift; format_python "$@" ;;
        -s|--sql)          shift; format_sql "$@" ;;
        -P|--prettier)     shift; format_prettier "$@" ;;
        -y|--yaml)         shift; format_yaml "$@" ;;
        -a|--all)          shift; all "$@" ;;
        -t|--ty)           shift; check_ty "$@" ;;
        -S|--standardize)  shift; standardize "$@" ;;
        -h|--help|*)       show_help ;;
    esac
}

main "$@"
