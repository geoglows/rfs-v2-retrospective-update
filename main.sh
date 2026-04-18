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

step_start() {
    echo ""
    echo "----------------------------------------"
    echo "$(date +'%Y-%m-%d %H:%M:%S')  START: $1"
}

step_end() {
    echo "$(date +'%Y-%m-%d %H:%M:%S')  END:   $1"
    echo "----------------------------------------"
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
# Prepare logging
mkdir -p "$LOG_ROOT"
LOG_FILE="$LOG_ROOT/update_$(date +'%Y%m%d').log"
echo "Logging to $LOG_FILE"
exec >> "$LOG_FILE" 2>&1 # all future messages go to the log path

# raise the open-file limit — cron has a lower cap which can sometimes kill s5cmd and xarray/zarr appends because of zarr chunk counts
ulimit -n 65536

step_start "environment setup"
# try to activate conda then the environment
source "$CONDA_ACTIVATE_PATH"
if ! conda activate $CONDA_ENV_NAME; then
    log_termination_message "Failed to activate conda environment."
fi
# check that the necessary packages are installed and in the PATH
commands=("curl" "python" "s5cmd")
for cmd in "${commands[@]}"; do
    if ! command -v "$cmd" &> /dev/null; then
        log_termination_message "Required command '$cmd' is not available in the PATH."
    fi
done
if [ "$REDOWNLOAD_S3" -eq 1 ]; then
    echo "Redownloading S3 copies requested — deleting local copies."
    rm -rf "$CONFIGS_DIR" "$HOURLY_ZARR" "$DAILY_ZARR" "$MONTHLY_TIMESERIES_ZARR" "$MONTHLY_TIMESTEPS_ZARR"
fi
step_end "environment setup"

step_start "download S3 copies"
# sync the configs if we don't have 125 expected subdirecctories
if [ "$(find "$CONFIGS_DIR" -maxdepth 1 -type d -name 'vpu=*' 2>/dev/null | wc -l)" -ne 125 ]; then
    echo "Syncing configs directory from S3."
    s5cmd --log error --no-sign-request sync "$S3_CONFIGS_DIR/*" "$CONFIGS_DIR"
fi

# Each zarr has a sentinel.json added.
# Existence implies complete and correct dataset which gets strictly checked in preflight checks.
if [ ! -f "$HOURLY_ZARR/sentinel.json" ]; then
    echo "Downloading hourly zarr from S3."
    rm -rf "$HOURLY_ZARR"
    s5cmd --log error --no-sign-request cp --exclude "*Q/0.*" "$S3_HOURLY_ZARR/*" "$HOURLY_ZARR"
fi
if [ ! -f "$DAILY_ZARR/sentinel.json" ]; then
    echo "Downloading daily zarr from S3."
    rm -rf "$DAILY_ZARR"
    s5cmd --log error --no-sign-request cp --exclude "*Q/0.*" "$S3_DAILY_ZARR/*" "$DAILY_ZARR"
fi
if [ ! -f "$MONTHLY_TIMESERIES_ZARR/sentinel.json" ]; then
    echo "Downloading monthly timeseries zarr from S3."
    rm -rf "$MONTHLY_TIMESERIES_ZARR"
    s5cmd --log error --no-sign-request cp --exclude "*Q/0.*" "$S3_MONTHLY_TIMESERIES_ZARR/*" "$WORK_DIR"/monthly-timeseries.zarr
fi
if [ ! -f "$MONTHLY_TIMESTEPS_ZARR/sentinel.json" ]; then
    echo "Downloading monthly timesteps zarr from S3."
    rm -rf "$MONTHLY_TIMESTEPS_ZARR"
    s5cmd --log error --no-sign-request cp "$S3_MONTHLY_TIMESTEPS_ZARR/*" "$WORK_DIR"/monthly-timesteps.zarr
fi
step_end "download S3 copies"

step_start "prepare transient directories"
rm -rf "$DISCHARGE_DIR" "$FORECAST_INITS_DIR"
mkdir -p "$WORK_DIR" "$DISCHARGE_DIR" "$ERA5_DIR" "$FINAL_STATES_DIR" "$FORECAST_INITS_DIR" "$HYDROSOS_DIR"
chmod -R 777 "$DISCHARGE_DIR" "$ERA5_DIR" "$FINAL_STATES_DIR" "$FORECAST_INITS_DIR" "$HYDROSOS_DIR"
step_end "prepare transient directories"

step_start "preflight validation"
if ! python "$SCRIPTS_ROOT"/preflight_validation.py --local-is-truth "$LOCAL_IS_TRUTH"; then
    log_termination_message "Failed to validate the environment. Shutting down."
fi
step_end "preflight validation"

step_start "download ERA5"
if ! python "$SCRIPTS_ROOT"/download_era5.py; then
    log_termination_message "Failed to download ERA5 data. Shutting down."
fi
step_end "download ERA5"

step_start "routing"
if ! python "$SCRIPTS_ROOT"/route.py; then
    log_termination_message "Failed to route data. Shutting down."
fi
step_end "routing"

step_start "upload init states and forecast inits to S3"
s5cmd --log error --credentials-file "$AWS_CREDENTIALS_FILE" cp "$FINAL_STATES_DIR/*" "$S3_FINAL_STATES_DIR"/
s5cmd --log error --credentials-file "$AWS_CREDENTIALS_FILE" cp "$FORECAST_INITS_DIR/*" "$S3_FORECAST_INITS_DIR"/
step_end "upload init states and forecast inits to S3"

step_start "append discharge to zarrs"
if ! python "$SCRIPTS_ROOT"/append_discharge.py; then
    log_termination_message "Failed to append discharge to zarrs. Shutting down."
fi
python "$SCRIPTS_ROOT"/sentinels.py set-updated hourly daily
step_end "append discharge to zarrs"

step_start "upload hourly + daily zarrs to S3"
# Pattern A: upload body first (excluding sentinel.json), then sentinel as the
# commit. If the body upload fails partway, S3's sentinel still reflects the
# previous synced state and the next run's preflight reports "local ahead"
# rather than silently claiming success.
s5cmd --log error --credentials-file "$AWS_CREDENTIALS_FILE" cp --exclude "sentinel.json" "$HOURLY_ZARR/*" "$S3_HOURLY_ZARR"/
s5cmd --log error --credentials-file "$AWS_CREDENTIALS_FILE" cp --exclude "sentinel.json" "$DAILY_ZARR/*" "$S3_DAILY_ZARR"/
s5cmd --log error --credentials-file "$AWS_CREDENTIALS_FILE" cp "$HOURLY_ZARR/sentinel.json" "$S3_HOURLY_ZARR/sentinel.json"
s5cmd --log error --credentials-file "$AWS_CREDENTIALS_FILE" cp "$DAILY_ZARR/sentinel.json" "$S3_DAILY_ZARR/sentinel.json"
step_end "upload hourly + daily zarrs to S3"

step_start "cleanup transient discharge + ERA5 dirs"
rm -r "$DISCHARGE_DIR" "$ERA5_DIR"
step_end "cleanup transient discharge + ERA5 dirs"

step_start "generate monthly products"
if ! python "$SCRIPTS_ROOT"/monthly_products.py; then
    log_termination_message "Failed to prepare monthly derived products. Shutting down."
fi
python "$SCRIPTS_ROOT"/sentinels.py set-updated monthly-timeseries monthly-timesteps
step_end "generate monthly products"

step_start "upload monthly products and HydroSOS COGs to S3"
# Pattern A: body first (excluding sentinel), then sentinel as the commit
s5cmd --log error --credentials-file "$AWS_CREDENTIALS_FILE" sync --exclude "sentinel.json" "$WORK_DIR/monthly-timeseries.zarr/*" "$S3_MONTHLY_TIMESERIES_ZARR"/
s5cmd --log error --credentials-file "$AWS_CREDENTIALS_FILE" sync --exclude "sentinel.json" "$WORK_DIR/monthly-timesteps.zarr/*" "$S3_MONTHLY_TIMESTEPS_ZARR"/
s5cmd --log error --credentials-file "$AWS_CREDENTIALS_FILE" cp "$MONTHLY_TIMESERIES_ZARR/sentinel.json" "$S3_MONTHLY_TIMESERIES_ZARR/sentinel.json"
s5cmd --log error --credentials-file "$AWS_CREDENTIALS_FILE" cp "$MONTHLY_TIMESTEPS_ZARR/sentinel.json" "$S3_MONTHLY_TIMESTEPS_ZARR/sentinel.json"
s5cmd --log error --credentials-file "$AWS_CREDENTIALS_FILE" sync "$HYDROSOS_DIR/*.tif" "$S3_HYDROSOS_COGS"/
rm -f "$HYDROSOS_DIR"/*.tif || true  # include "|| true" so that failure to remove files when they don't exist doesn't cause an early error and exit
step_end "upload monthly products and HydroSOS COGs to S3"

log_termination_message "Script completed successfully. Shutting down."
