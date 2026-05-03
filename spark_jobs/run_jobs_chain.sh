#!/usr/bin/env bash
set -euo pipefail

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

SPARK="spark-submit --num-executors 6 --executor-cores 3 --executor-memory 8g --driver-memory 4g"

log "Starting Job 04..."
$SPARK /tmp/04_top_repos_all.py > /tmp/job04.log 2>&1
log "Job 04 done."

log "Starting Job 07..."
$SPARK /tmp/07_contributor_health.py > /tmp/job07.log 2>&1
log "Job 07 done."

log "Starting Job 06..."
$SPARK /tmp/06_star_growth_hype.py > /tmp/job06.log 2>&1
log "Job 06 done."

log "ALL JOBS DONE"
