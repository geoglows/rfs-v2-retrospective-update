"""
Pre-flight validation for the retrospective update pipeline.

Runs three classes of check, aggregates ALL issues before exiting so every
problem is surfaced at once:

  1. Per-store internal consistency — .zmetadata vs .zarray, and every
     time-dim variable's length matches the time coord.
  2. Time coord integrity — no NaT/NaN values, uniform spacing, no
     duplicates.
  3. Local-vs-S3 shape + last-time-value comparison — cheap (reads a few
     bytes of .zarray JSON and one float) instead of comparing the full
     time coord array byte-for-byte.

Also checks the download sentinels haven't been invalidated, and ensures
the required init-state parquet files are present locally.
"""

import argparse
import json
import os
import subprocess
import traceback
from datetime import datetime
from glob import glob
from pathlib import Path

import numpy as np
import pandas as pd
import s3fs
import xarray as xr
import zarr
from natsort import natsorted

import helpers.sentinels as sentinels
from helpers.cloud_logger import CloudLog
from helpers.set_env_vars import (
    DAILY_ZARR, FINAL_STATES_DIR, HOURLY_ZARR, S3_HOURLY_ZARR, S3_DAILY_ZARR,
    S3_FINAL_STATES_DIR, CONFIGS_DIR,
    MONTHLY_TIMESTEPS_ZARR, MONTHLY_TIMESERIES_ZARR,
    S3_MONTHLY_TIMESTEPS_ZARR, S3_MONTHLY_TIMESERIES_ZARR,
)
from helpers.validators import validate_internal, validate_time

STORES = [
    {
        'name': 'hourly',
        'local': HOURLY_ZARR,
        's3': S3_HOURLY_ZARR,
        'sentinel': 'hourly',
        'freq': 'uniform',
    },
    {
        'name': 'daily',
        'local': DAILY_ZARR,
        's3': S3_DAILY_ZARR,
        'sentinel': 'daily',
        'freq': 'uniform',
    },
    {
        'name': 'monthly-timeseries',
        'local': MONTHLY_TIMESERIES_ZARR,
        's3': S3_MONTHLY_TIMESERIES_ZARR,
        'sentinel': 'monthly-timeseries',
        'freq': 'monthly',
    },
    {
        'name': 'monthly-timesteps',
        'local': MONTHLY_TIMESTEPS_ZARR,
        's3': S3_MONTHLY_TIMESTEPS_ZARR,
        'sentinel': 'monthly-timesteps',
        'freq': 'monthly',
    },
]

_S3_FS = None


def _s3fs() -> s3fs.S3FileSystem:
    global _S3_FS
    if _S3_FS is None:
        _S3_FS = s3fs.S3FileSystem(anon=True)
    return _S3_FS


def _fetch_s3_zarray(s3_path: str, var: str) -> dict:
    key = f'{s3_path.replace("s3://", "")}/{var}/.zarray'
    with _s3fs().open(key, 'rb') as f:
        return json.loads(f.read())


def _fetch_s3_time_last(s3_path: str) -> float:
    fs_store = zarr.storage.FSStore(s3_path, fs=_s3fs())
    z = zarr.open(fs_store, mode='r')
    return float(z['time'][-1])


def _fetch_s3_sentinel(s3_path: str) -> dict | None:
    """Fetch the sentinel.json from inside an S3 zarr store. None if absent."""
    key = f'{s3_path.replace("s3://", "")}/{sentinels.SENTINEL_FILENAME}'
    try:
        with _s3fs().open(key, 'rb') as f:
            return json.loads(f.read())
    except FileNotFoundError:
        return None
    except OSError as e:
        # s3fs can raise OSError for missing keys depending on version
        if 'Not Found' in str(e) or '404' in str(e):
            return None
        raise


def compare_local_to_s3(local_path: str, s3_path: str, label: str) -> list[str]:
    """Compare shape and last-time-value between local and S3 zarrs."""
    issues: list[str] = []

    for var in ['time', 'Q']:
        try:
            local_meta = json.loads((Path(local_path) / var / '.zarray').read_text())
            local_shape = tuple(local_meta['shape'])
        except Exception as e:
            issues.append(f'{label}: cannot read local {var}/.zarray: {e}')
            continue
        try:
            s3_meta = _fetch_s3_zarray(s3_path, var)
            s3_shape = tuple(s3_meta['shape'])
        except Exception as e:
            issues.append(f'{label}: cannot read s3 {var}/.zarray: {e}')
            continue
        if local_shape != s3_shape:
            issues.append(f'{label}: {var} shape local={local_shape} s3={s3_shape}')

    try:
        local_last = float(zarr.open(local_path, mode='r')['time'][-1])
        s3_last = _fetch_s3_time_last(s3_path)
        if np.isnan(local_last) or np.isnan(s3_last):
            issues.append(f'{label}: time[-1] is NaN (local={local_last}, s3={s3_last})')
        elif local_last != s3_last:
            issues.append(f'{label}: time[-1] local={local_last} s3={s3_last}')
    except Exception as e:
        issues.append(f'{label}: could not compare time[-1]: {e}')

    return issues


def _parse_iso(s: str) -> datetime:
    """Parse an ISO-8601 UTC string (with trailing Z) into an aware datetime."""
    if s.endswith('Z'):
        s = s[:-1] + '+00:00'
    return datetime.fromisoformat(s)


def check_sentinels() -> tuple[list[str], list[str], dict[str, bool]]:
    """Compare each store's local sentinel.json against its S3 counterpart.

    The sentinel schema has a single field `updated` (ISO UTC timestamp).
    After a successful Pattern-A upload S3's sentinel is an exact copy of
    local's, so nominal sync is just JSON equality; otherwise the `updated`
    ordering tells us which side is ahead.

    Returns (errors, warnings, local_ahead_flags).
    """
    errors: list[str] = []
    warnings: list[str] = []
    local_ahead: dict[str, bool] = {}

    for store in STORES:
        name = store['name']
        local = sentinels.read(store['sentinel'])
        s3 = _fetch_s3_sentinel(store['s3'])

        if local is None:
            errors.append(f'{name}: local sentinel missing at {sentinels.path_for(store["sentinel"])}')
            continue
        if s3 is None:
            # S3 has never had a sentinel written. Treat as first-run bootstrap
            # and proceed — the pipeline will upload one at the end.
            warnings.append(f'{name}: S3 sentinel missing — treating as first-run bootstrap')
            continue

        if local == s3:
            continue  # nominal: fully synced

        try:
            lu = _parse_iso(local['updated'])
            su = _parse_iso(s3['updated'])
        except Exception as e:
            errors.append(f'{name}: cannot parse sentinel updated field: {e} (local={local}, s3={s3})')
            continue

        if lu > su:
            warnings.append(
                f'{name}: local is ahead of S3 (local updated={lu.isoformat()}, '
                f's3 updated={su.isoformat()}) — pipeline will re-upload this run'
            )
            local_ahead[name] = True
        elif lu < su:
            errors.append(
                f'{name}: S3 is ahead of local (s3 updated={su.isoformat()}, '
                f'local updated={lu.isoformat()}) — probable external write. '
                f'Human must decide to accept S3 (e.g. run with --redownload-s3) '
                f'or investigate.'
            )
        else:
            errors.append(
                f'{name}: same updated timestamp but sentinel contents differ '
                f'(local={local}, s3={s3}) — race or corruption'
            )

    return errors, warnings, local_ahead


def check_for_init_files() -> None:
    """Ensure the init-state parquet matching the last hourly time step is
    present locally; download from S3 if not."""
    with xr.open_zarr(HOURLY_ZARR, consolidated=False) as ds:
        last_hourly_retro_time = pd.to_datetime(ds['time'][-1].values).strftime('%Y%m%d%H%M')
    expected_final_state_file = f'finalstate_{last_hourly_retro_time}.parquet'

    states = natsorted(glob(os.path.join(FINAL_STATES_DIR, '*', 'finalstate*.parquet')))
    for state in states:
        if os.path.basename(state) != expected_final_state_file:
            os.remove(state)

    states = natsorted(glob(os.path.join(FINAL_STATES_DIR, '*', 'finalstate*.parquet')))
    if len(states) == len(list(glob(os.path.join(CONFIGS_DIR, '*')))):
        return

    cmd = f's5cmd --no-sign-request cp "{S3_FINAL_STATES_DIR}/*/{expected_final_state_file}" {FINAL_STATES_DIR}/'
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        cl.error(f'Error running s5cmd copy to get init files: {result.stderr}')
        raise RuntimeError

    states = natsorted(glob(os.path.join(FINAL_STATES_DIR, '*', 'finalstate*.parquet')))
    if len(states) != len(list(glob(os.path.join(CONFIGS_DIR, '*')))):
        cl.error('s5cmd to fetch inits succeeded but expected files are not present')
        raise RuntimeError


def _bool01(s: str) -> bool:
    if s in ('0', '1'):
        return s == '1'
    raise argparse.ArgumentTypeError(f'expected 0 or 1, got {s!r}')


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument(
        '--local-is-truth',
        type=_bool01,
        default=False,
        help='when 1, skip S3 freshness and shape comparisons; trust the '
             'local copy as long as it passes internal validation',
    )
    args = ap.parse_args()

    cl = CloudLog()
    try:
        if args.local_is_truth:
            cl.log('Pre-flight validation starting (--local-is-truth: skipping S3 comparisons)')
        else:
            cl.log('Pre-flight validation starting')

        errors: list[str] = []
        warnings: list[str] = []
        local_ahead: dict[str, bool] = {}

        if not args.local_is_truth:
            sentinel_errors, sentinel_warnings, local_ahead = check_sentinels()
            errors += sentinel_errors
            warnings += sentinel_warnings

        for store in STORES:
            cl.log(f'  validating {store["name"]}')
            label = f'{store["name"]} [local]'
            errors += validate_internal(store['local'], label)
            errors += validate_time(store['local'], label, freq=store['freq'])
            if not args.local_is_truth:
                s3_diff = compare_local_to_s3(store['local'], store['s3'], store['name'])
                # when local is known-ahead (unuploaded changes), shape/last-time
                # mismatches vs S3 are expected — demote to warnings
                if local_ahead.get(store['name']):
                    warnings += s3_diff
                else:
                    errors += s3_diff

        if warnings:
            cl.log(f'Pre-flight validation warnings ({len(warnings)}):')
            for w in warnings:
                cl.log(f'  - {w}')

        if errors:
            cl.error(f'Pre-flight validation FAILED with {len(errors)} error(s):')
            for e in errors:
                cl.error(f'  - {e}')
            exit(1)

        cl.log('All stores pass validation.')
        check_for_init_files()
        cl.log('Init state files verified.')
        exit(0)
    except Exception as e:
        cl.error(str(e))
        cl.error(traceback.format_exc())
        exit(1)
