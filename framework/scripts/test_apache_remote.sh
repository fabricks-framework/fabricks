#!/usr/bin/env bash
# Usage: test_apache_remote.sh [TARGET [WORKERS]]   (WORKERS = pytest-xdist worker count; empty = serial)
# Rsyncs the working tree to $FABRICKS_REMOTE and runs `just test-apache` there.
# FABRICKS_REMOTE_DIR defaults to ~/Fabricks; FABRICKS_REMOTE_JAVA_HOME optionally picks the remote JDK.
set -euo pipefail

target="${1:-tests/spark/apache}"
workers="${2:-}"
host="${FABRICKS_REMOTE:?set FABRICKS_REMOTE to the ssh host}"
dir="${FABRICKS_REMOTE_DIR:-Fabricks}"
java_home="${FABRICKS_REMOTE_JAVA_HOME:-}"

cd "$(dirname "$0")/.."
rsync -az --delete --exclude .git --exclude .venv --exclude '__pycache__' --exclude .worker_cwd \
    --exclude .pytest_cache --exclude .ruff_cache --exclude .logs ../ "$host:$dir/"
ssh -t "$host" "export PATH=\$HOME/.local/bin:\$PATH; ${java_home:+export JAVA_HOME=$java_home;} cd $dir/framework && just test-apache $target $workers"
