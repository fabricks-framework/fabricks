#!/usr/bin/env bash
# Usage: run_logged.sh <category> <command...>
# Runs the command, teeing its output to .logs/<category>/<timestamp>.log (gitignored) and pointing
# .logs/<category>/latest.log at it. Keeps the newest 20 logs per category and exits with the command's status.
# Opt out with FABRICKS_TEST_LOG=off (also accepts 0/false/no), e.g. in framework/.env.
set -uo pipefail

category="${1:?usage: run_logged.sh <category> <command...>}"
shift

case "${FABRICKS_TEST_LOG:-on}" in
    off | 0 | false | no) exec "$@" ;;
esac

dir="$(cd "$(dirname "$0")/.." && pwd)/.logs/$category"
mkdir -p "$dir"
log="$dir/$(date +%Y%m%d-%H%M%S).log"
ln -sf "$(basename "$log")" "$dir/latest.log"

echo "\$ $*" | tee "$log"
"$@" 2>&1 | tee -a "$log"
status=${PIPESTATUS[0]}
echo "exit=$status" >> "$log"

find "$dir" -maxdepth 1 -name '*.log' ! -name latest.log -printf '%T@ %p\n' | sort -rn | tail -n +21 | cut -d' ' -f2- | xargs -r rm --
echo "log: $log" >&2
exit "$status"
