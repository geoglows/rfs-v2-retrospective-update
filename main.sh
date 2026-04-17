#!/usr/bin/env bash
set -Eeuo pipefail

# sample cron command. Should be UTC
# 5 0 * * * /usr/bin/lockf -t 0 /Users/Shared/lockfiles/retrospective-update.lock /Users/Shared/code/rfs-v2-retrospective-update/main.sh

log_and_shutdown() {
    local message="$1"
    curl -X POST -H "Content-Type: application/json" -d "{\"text\": \"$message\"}" "$WEBHOOK_ERROR_URL" || true
    exit 1
}

FORCE_SYNC_FIRST=0
for arg in "$@"; do
    case "$arg" in
        --force-sync-first) FORCE_SYNC_FIRST=1 ;;
        *) echo "Unknown argument: $arg"; exit 1 ;;
    esac
done

# read the environment variables at ./variables.*.env relative to the location of this script
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$SCRIPT_DIR"/variables.macstudio.env
mkdir -p "$LOG_ROOT"
LOG_FILE="$LOG_ROOT/update_$(date +'%Y%m%d').log"
echo "Logging to $LOG_FILE"
exec >> "$LOG_FILE" 2>&1

# try to activate conda then the environment
source "$CONDA_ACTIVATE_PATH"
if ! conda activate $CONDA_ENV_NAME; then
    log_and_shutdown "Failed to activate conda environment."
fi

# check that the necessary packages are installed and in the PATH
commands=("curl" "python" "s5cmd")
for cmd in "${commands[@]}"; do
    if ! command -v "$cmd" &> /dev/null; then
        log_and_shutdown "Required command '$cmd' is not available in the PATH."
    fi
done

# check that the configs directory and all local copies of zarrs exist and are not empty
if [ "$FORCE_SYNC_FIRST" -eq 1 ] || [ ! -d "$CONFIGS_DIR" ] || [ -z "$(ls -A "$CONFIGS_DIR" 2>/dev/null)" ]; then
    echo "Syncing configs directory from S3."
    s5cmd --log error --stat --no-sign-request sync "$S3_CONFIGS_DIR/*" "$CONFIGS_DIR"
fi
if [ "$FORCE_SYNC_FIRST" -eq 1 ] || [ ! -d "$HOURLY_ZARR" ]; then
    echo "Syncing hourly zarr from S3."
    s5cmd --log error --stat --no-sign-request sync --exclude "*Q/0.*" "$S3_HOURLY_ZARR/*" "$HOURLY_ZARR"
fi
if [ "$FORCE_SYNC_FIRST" -eq 1 ] || [ ! -d "$DAILY_ZARR" ]; then
    echo "Syncing daily zarr from S3."
    s5cmd --log error --stat --no-sign-request sync --exclude "*Q/0.*" "$S3_DAILY_ZARR/*" "$DAILY_ZARR"
fi
if [ "$FORCE_SYNC_FIRST" -eq 1 ] || [ ! -d "$MONTHLY_TIMESERIES_ZARR" ]; then
    echo "Syncing monthly timeseries zarr from S3."
    s5cmd --log error --stat --no-sign-request sync --exclude "*Q/0.*" "$S3_MONTHLY_TIMESERIES_ZARR/*" "$WORK_DIR"/monthly-timeseries.zarr
fi
if [ "$FORCE_SYNC_FIRST" -eq 1 ] || [ ! -d "$MONTHLY_TIMESTEPS_ZARR" ]; then
    echo "Syncing monthly timesteps zarr from S3."
    s5cmd --log error --stat --no-sign-request sync "$S3_MONTHLY_TIMESTEPS_ZARR/*" "$WORK_DIR"/monthly-timesteps.zarr
fi

# Prepare directories
rm -rf "$DISCHARGE_DIR" "$FORECAST_INITS_DIR"
mkdir -p "$WORK_DIR" "$DISCHARGE_DIR" "$ERA5_DIR" "$FINAL_STATES_DIR" "$FORECAST_INITS_DIR" "$HYDROSOS_DIR"
chmod -R 777 "$DISCHARGE_DIR" "$ERA5_DIR" "$FINAL_STATES_DIR" "$FORECAST_INITS_DIR" "$HYDROSOS_DIR"

if ! python "$SCRIPTS_ROOT"/prepare.py; then
    log_and_shutdown "Failed to validate the environment. Shutting down."
fi

if ! python "$SCRIPTS_ROOT"/download_era5.py; then
    log_and_shutdown "Failed to download ERA5 data. Shutting down."
fi

if ! python "$SCRIPTS_ROOT"/route.py; then
    log_and_shutdown "Failed to route data. Shutting down."
fi

# synchronize inits only to s3 so they are available asap
s5cmd --credentials-file "$AWS_CREDENTIALS_FILE" cp "$FINAL_STATES_DIR/*" "$S3_FINAL_STATES_DIR"/
s5cmd --credentials-file "$AWS_CREDENTIALS_FILE" cp "$FORECAST_INITS_DIR/*" "$S3_FORECAST_INITS_DIR"/

if ! python "$SCRIPTS_ROOT"/append_discharge.py; then
    log_and_shutdown "Failed to append discharge to zarrs. Shutting down."
fi

s5cmd --credentials-file "$AWS_CREDENTIALS_FILE" cp "$HOURLY_ZARR/*" "$S3_HOURLY_ZARR"/
s5cmd --credentials-file "$AWS_CREDENTIALS_FILE" cp "$DAILY_ZARR/*" "$S3_DAILY_ZARR"/

rm -r "$DISCHARGE_DIR" "$ERA5_DIR"

if ! python "$SCRIPTS_ROOT"/monthly_products.py; then
  log_and_shutdown "Failed to prepare monthly derived products. Shutting down."
fi

s5cmd --credentials-file "$AWS_CREDENTIALS_FILE" sync "$WORK_DIR/monthly-timeseries.zarr/*" "$S3_MONTHLY_TIMESERIES_ZARR"/
s5cmd --credentials-file "$AWS_CREDENTIALS_FILE" sync "$WORK_DIR/monthly-timesteps.zarr/*" "$S3_MONTHLY_TIMESTEPS_ZARR"/
s5cmd --credentials-file "$AWS_CREDENTIALS_FILE" sync "$HYDROSOS_DIR/*.tif" "$S3_HYDROSOS_COGS"/
rm -f "$HYDROSOS_DIR"/*.tif || true  # include "|| true" so that failure to remove files when they don't exist doesn't cause an early error and exit

log_and_shutdown "Script completed successfully. Shutting down."
