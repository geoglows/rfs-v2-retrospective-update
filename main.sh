#!/usr/bin/env bash
set -Eeuo pipefail

COMPUTE_ENVIRONMENT="macstudio"

log_and_shutdown() {
    local message="$1"
    curl -X POST -H "Content-Type: application/json" -d "{\"text\": \"$message\"}" "$WEBHOOK_ERROR_URL" || true
    sleep 60  # allow time to interrupt in case of retry, accidents, etc
    # shutdown if compute environment is awsec2 only
    if [ "$COMPUTE_ENVIRONMENT" == "awsec2" ]; then
        sudo shutdown -h now
    else
        exit 1
    fi
}

# read the environment variables at ./variables.*.env relative to the location of this script
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$SCRIPT_DIR"/variables.macstudio.env

# sleep to give users time to log on and cancel the job if this is not a normally scheduled run
sleep "$SLEEP_LENGTH"

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
if [ ! -d "$CONFIGS_DIR" ] || [ -z "$(ls -A "$CONFIGS_DIR")" ]; then
    echo "Error: Configs directory is empty or does not exist."
    s5cmd --no-sign-request sync "$S3_CONFIGS_DIR/*" "$CONFIGS_DIR"
fi
if [ ! -d "$HOURLY_ZARR" ]; then
    echo "Hourly zarr directory is empty or does not exist. Downloading a copy."
    s5cmd --no-sign-request sync --exclude "*Q/0.*" "$S3_HOURLY_ZARR/*" "$HOURLY_ZARR"
fi
if [ ! -d "$DAILY_ZARR" ]; then
    echo "Daily zarr directory is empty or does not exist. Downloading a copy."
    s5cmd --no-sign-request sync --exclude "*Q/0.*" "$S3_DAILY_ZARR/*" "$DAILY_ZARR"
fi
if [ ! -d "$MONTHLY_TIMESERIES_ZARR" ]; then
    echo "Monthly timeseries directory is empty or does not exist. Downloading a copy."
    s5cmd --no-sign-request sync --exclude "*Q/0.*" "$S3_MONTHLY_TIMESERIES_ZARR/*" "$WORK_DIR"/monthly-timeseries.zarr
fi
if [ ! -d "$MONTHLY_TIMESTEPS_ZARR" ]; then
    echo "Monthly timesteps directory is empty or does not exist. Downloading a copy."
    s5cmd --no-sign-request sync "$S3_MONTHLY_TIMESTEPS_ZARR/*" "$WORK_DIR"/monthly-timesteps.zarr
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
