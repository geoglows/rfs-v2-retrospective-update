#!/usr/bin/env bash
set -Eeuo pipefail

# sample cron command. Should be UTC
# 5 0 * * * /usr/bin/lockf -t 0 /Users/Shared/lockfiles/retrospective-update.lock /Users/Shared/code/rfs-v2-retrospective-update/main.sh

log_termination_message() {
  # this logs either a pipeline failure or success message and optionally shuts down the machine after arriving at this completed state
    local message="$1"
    curl -X POST -H "Content-Type: application/json" -d "{\"text\": \"$message\"}" "$WEBHOOK_LOG_ALERTS" || true
    if [ "${SHUTDOWN_AFTER_RUN:-0}" = "1" ]; then
        sudo shutdown -h now
    fi
    exit 1
}

step_begin() {
    echo ""
    echo "----------------------------------------"
    echo "$(date +'%Y-%m-%d %H:%M:%S')  START: $1"
}

step_end() {
    echo "$(date +'%Y-%m-%d %H:%M:%S')  END:   $1"
    echo "----------------------------------------"
}

write_sentinels() {
    local ts
    ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)
    local name dir
    for name in "$@"; do
        case "$name" in
            hourly)             dir="$HOURLY_ZARR" ;;
            daily)              dir="$DAILY_ZARR" ;;
            monthly-timeseries) dir="$MONTHLY_TIMESERIES_ZARR" ;;
            monthly-timesteps)  dir="$MONTHLY_TIMESTEPS_ZARR" ;;
            *) echo "write_sentinels: unknown store '$name'" >&2; return 1 ;;
        esac
        mkdir -p "$dir"
        printf '{"updated": "%s"}' "$ts" > "$dir/sentinel.json"
    done
}

REDOWNLOAD_S3=0
LOCAL_IS_TRUTH=0
for arg in "$@"; do
    case "$arg" in
        --redownload-s3) REDOWNLOAD_S3=1 ;;
        --local-is-truth) LOCAL_IS_TRUTH=1 ;;
        *) echo "Unknown argument: $arg"; exit 1 ;;
    esac
done

# read the environment variables at ./variables.*.env relative to script location
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$SCRIPT_DIR"/variables.macstudio.env
umask 002
ulimit -n 65536  # raise the open-file limit
export PYTHONPATH="$SCRIPTS_ROOT${PYTHONPATH:+:$PYTHONPATH}"
mkdir -p "$LOG_ROOT" "$DASK_TEMPORARY_DIRECTORY" "$DASK_ROOT_CONFIG"
chmod -R 777 "$DASK_TEMPORARY_DIRECTORY" "$DASK_ROOT_CONFIG"
LOG_FILE="$LOG_ROOT/update_$(date +'%Y%m%d').log"
echo "Logging to $LOG_FILE"
exec >> "$LOG_FILE" 2>&1 # all future messages go to the log path

step_begin "environment setup"
source "$CONDA_ACTIVATE_PATH"
if ! conda activate $CONDA_ENV_NAME; then
    log_termination_message "Failed to activate conda environment."
fi
commands=("curl" "python" "s5cmd")
for cmd in "${commands[@]}"; do
    if ! command -v "$cmd" &> /dev/null; then
        log_termination_message "Required command '$cmd' is not available in the PATH."
    fi
done
if [ "$REDOWNLOAD_S3" -eq 1 ]; then
    echo "Redownloading S3 copies requested — deleting local sentinels to force re-sync."
    rm -f "$HOURLY_ZARR/sentinel.json" "$DAILY_ZARR/sentinel.json" "$MONTHLY_TIMESERIES_ZARR/sentinel.json" "$MONTHLY_TIMESTEPS_ZARR/sentinel.json"
fi
step_end "environment setup"

step_begin "download S3 copies"
if [ "$(find "$CONFIGS_DIR" -maxdepth 1 -type d -name 'vpu=*' 2>/dev/null | wc -l)" -ne 125 ]; then
    echo "Syncing configs directory from S3."
    s5cmd --log error --no-sign-request sync "$S3_CONFIGS_DIR/*" "$CONFIGS_DIR"
fi
if [ ! -f "$HOURLY_ZARR/sentinel.json" ]; then
    echo "Syncing hourly zarr from S3."
    s5cmd --log error --no-sign-request sync --delete --exclude "*Q/0.*" "$S3_HOURLY_ZARR/*" "$HOURLY_ZARR"
fi
if [ ! -f "$DAILY_ZARR/sentinel.json" ]; then
    echo "Syncing daily zarr from S3."
    s5cmd --log error --no-sign-request sync --delete --exclude "*Q/0.*" "$S3_DAILY_ZARR/*" "$DAILY_ZARR"
fi
if [ ! -f "$MONTHLY_TIMESERIES_ZARR/sentinel.json" ]; then
    echo "Syncing monthly timeseries zarr from S3."
    s5cmd --log error --no-sign-request sync --delete --exclude "*Q/0.*" "$S3_MONTHLY_TIMESERIES_ZARR/*" "$WORK_DIR"/monthly-timeseries.zarr
fi
if [ ! -f "$MONTHLY_TIMESTEPS_ZARR/sentinel.json" ]; then
    echo "Syncing monthly timesteps zarr from S3."
    s5cmd --log error --no-sign-request sync --delete "$S3_MONTHLY_TIMESTEPS_ZARR/*" "$WORK_DIR"/monthly-timesteps.zarr
fi
step_end "download S3 copies"

step_begin "prepare working directories"
rm -rf "$DISCHARGE_DIR" "$FORECAST_INITS_DIR"
mkdir -p "$WORK_DIR" "$DISCHARGE_DIR" "$ERA5_DIR" "$FINAL_STATES_DIR" "$FORECAST_INITS_DIR" "$HYDROSOS_DIR"
chmod -R 777 "$DISCHARGE_DIR" "$ERA5_DIR" "$FINAL_STATES_DIR" "$FORECAST_INITS_DIR" "$HYDROSOS_DIR"
step_end "prepare working directories"

step_begin "preflight validation"
preflight_args=()
if [ "$LOCAL_IS_TRUTH" -eq 1 ]; then
    preflight_args+=(--local-is-truth)
fi
if ! python -m preflight_validation "${preflight_args[@]}"; then
    log_termination_message "Failed to validate the environment. Shutting down."
fi
step_end "preflight validation"

step_begin "download ERA5"
if ! python -m download_era5; then
    log_termination_message "Failed to download ERA5 data. Shutting down."
fi
step_end "download ERA5"

step_begin "routing"
if ! python -m route; then
    log_termination_message "Failed to route data. Shutting down."
fi
step_end "routing"

step_begin "Upload init states"
s5cmd --log error --credentials-file "$AWS_CREDENTIALS_FILE" cp "$FINAL_STATES_DIR/*" "$S3_FINAL_STATES_DIR"/
s5cmd --log error --credentials-file "$AWS_CREDENTIALS_FILE" cp "$FORECAST_INITS_DIR/*" "$S3_FORECAST_INITS_DIR"/
step_end "Upload init states"

step_begin "append discharge to zarrs"
if ! python -m append_discharge; then
    log_termination_message "Failed to append discharge to zarrs. Shutting down."
fi
write_sentinels hourly daily
step_end "append discharge to zarrs"

step_begin "upload hourly + daily zarrs to S3"
s5cmd --log error --credentials-file "$AWS_CREDENTIALS_FILE" cp --exclude "sentinel.json" "$HOURLY_ZARR/*" "$S3_HOURLY_ZARR"/
s5cmd --log error --credentials-file "$AWS_CREDENTIALS_FILE" cp --exclude "sentinel.json" "$DAILY_ZARR/*" "$S3_DAILY_ZARR"/
s5cmd --log error --credentials-file "$AWS_CREDENTIALS_FILE" cp "$HOURLY_ZARR/sentinel.json" "$S3_HOURLY_ZARR/sentinel.json"
s5cmd --log error --credentials-file "$AWS_CREDENTIALS_FILE" cp "$DAILY_ZARR/sentinel.json" "$S3_DAILY_ZARR/sentinel.json"
step_end "upload hourly + daily zarrs to S3"

step_begin "cleanup working discharge + ERA5 dirs"
rm -r "$DISCHARGE_DIR" "$ERA5_DIR"
step_end "cleanup working discharge + ERA5 dirs"

step_begin "generate monthly products"
if ! python -m monthly_products; then
    log_termination_message "Failed to prepare monthly derived products. Shutting down."
fi
write_sentinels monthly-timeseries monthly-timesteps
step_end "generate monthly products"

step_begin "upload monthly products and HydroSOS COGs to S3"
s5cmd --log error --credentials-file "$AWS_CREDENTIALS_FILE" sync --exclude "sentinel.json" "$WORK_DIR/monthly-timeseries.zarr/*" "$S3_MONTHLY_TIMESERIES_ZARR"/
s5cmd --log error --credentials-file "$AWS_CREDENTIALS_FILE" sync --exclude "sentinel.json" "$WORK_DIR/monthly-timesteps.zarr/*" "$S3_MONTHLY_TIMESTEPS_ZARR"/
s5cmd --log error --credentials-file "$AWS_CREDENTIALS_FILE" cp "$MONTHLY_TIMESERIES_ZARR/sentinel.json" "$S3_MONTHLY_TIMESERIES_ZARR/sentinel.json"
s5cmd --log error --credentials-file "$AWS_CREDENTIALS_FILE" cp "$MONTHLY_TIMESTEPS_ZARR/sentinel.json" "$S3_MONTHLY_TIMESTEPS_ZARR/sentinel.json"
s5cmd --log error --credentials-file "$AWS_CREDENTIALS_FILE" sync "$HYDROSOS_DIR/*.tif" "$S3_HYDROSOS_COGS"/
rm -f "$HYDROSOS_DIR"/*.tif || true  # include "|| true" so that failure to remove files when they don't exist doesn't cause an early error and exit
step_end "upload monthly products and HydroSOS COGs to S3"

log_termination_message "Script completed successfully. Shutting down."
