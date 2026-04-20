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

from helpers.cloud_logger import CloudLog
from helpers.set_env_vars import (
    DAILY_ZARR, FINAL_STATES_DIR, HOURLY_ZARR, S3_HOURLY_ZARR, S3_DAILY_ZARR,
    S3_FINAL_STATES_DIR, CONFIGS_DIR,
    MONTHLY_TIMESTEPS_ZARR, MONTHLY_TIMESERIES_ZARR,
    S3_MONTHLY_TIMESTEPS_ZARR, S3_MONTHLY_TIMESERIES_ZARR,
)
from helpers.validators import validate_internal, validate_time

SENTINEL_FILENAME = 'sentinel.json'

STORES = [
    {
        'name': 'hourly',
        'local': HOURLY_ZARR,
        's3': S3_HOURLY_ZARR,
        'freq': 'uniform',
    },
    {
        'name': 'daily',
        'local': DAILY_ZARR,
        's3': S3_DAILY_ZARR,
        'freq': 'uniform',
    },
    {
        'name': 'monthly-timeseries',
        'local': MONTHLY_TIMESERIES_ZARR,
        's3': S3_MONTHLY_TIMESERIES_ZARR,
        'freq': 'monthly',
    },
    {
        'name': 'monthly-timesteps',
        'local': MONTHLY_TIMESTEPS_ZARR,
        's3': S3_MONTHLY_TIMESTEPS_ZARR,
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
    key = f'{s3_path.replace("s3://", "")}/{SENTINEL_FILENAME}'
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


def _parse_sentinel(data: dict, source: str) -> tuple[datetime | None, str | None]:
    """Validate sentinel dict shape and return (updated_datetime, error).
    Exactly one of the two is always non-None."""
    if not isinstance(data, dict):
        return None, f'{source}: sentinel is not a JSON object (got {type(data).__name__})'
    if 'updated' not in data:
        return None, f'{source}: sentinel is missing required "updated" key (got keys: {list(data.keys())})'
    if not isinstance(data['updated'], str):
        return None, f'{source}: "updated" must be a string, got {type(data["updated"]).__name__}'
    extras = set(data.keys()) - {'updated'}
    if extras:
        return None, f'{source}: sentinel has unexpected keys: {sorted(extras)}'
    try:
        return _parse_iso(data['updated']), None
    except Exception as e:
        return None, f'{source}: cannot parse "updated" as ISO-8601: {e} (value={data["updated"]!r})'


def _safe_read_local_sentinel(zarr_path: str, name: str) -> tuple[dict | None, Path, str | None]:
    """Return (parsed_json, sentinel_path, error). parsed_json is None if
    sentinel is missing OR unreadable; error is None iff read succeeded (or
    file was absent)."""
    path = Path(zarr_path) / SENTINEL_FILENAME
    if not path.exists():
        return None, path, None
    try:
        return json.loads(path.read_text()), path, None
    except Exception as e:
        return None, path, f'{name}: cannot parse local sentinel at {path}: {type(e).__name__}: {e}'


def _safe_fetch_s3_sentinel(s3_path: str, name: str) -> tuple[dict | None, str | None]:
    """Return (parsed_json, error). None json + None error means the S3 key
    doesn't exist (bootstrap case)."""
    try:
        data = _fetch_s3_sentinel(s3_path)
    except Exception as e:
        return None, f'{name}: cannot fetch S3 sentinel from {s3_path}: {type(e).__name__}: {e}'
    return data, None


def check_sentinels() -> tuple[list[str], list[str], dict[str, bool]]:
    """Compare each store's local sentinel.json against its S3 counterpart.

    The sentinel schema is a single-key dict `{"updated": "<ISO UTC>"}`.
    After a successful Pattern-A upload S3's sentinel is an exact copy of
    local's, so nominal sync is JSON equality; otherwise the `updated`
    ordering tells us which side is ahead.

    Returns (errors, warnings, local_ahead_flags).
    """
    errors: list[str] = []
    warnings: list[str] = []
    local_ahead: dict[str, bool] = {}

    for store in STORES:
        name = store['name']

        # 1. Read both sides, catching IO/parse errors explicitly
        local, local_path, local_read_err = _safe_read_local_sentinel(store['local'], name)
        if local_read_err:
            errors.append(local_read_err)
            continue
        if local is None:
            errors.append(f'{name}: local sentinel missing at {local_path}')
            continue

        s3, s3_read_err = _safe_fetch_s3_sentinel(store['s3'], name)
        if s3_read_err:
            errors.append(s3_read_err)
            continue

        # 2. Schema-validate both sides (shape + updated is parseable ISO)
        lu, local_schema_err = _parse_sentinel(local, f'{name} [local]')
        if local_schema_err:
            errors.append(local_schema_err)
            continue
        if s3 is None:
            warnings.append(f'{name}: S3 sentinel missing — treating as first-run bootstrap')
            continue
        su, s3_schema_err = _parse_sentinel(s3, f'{name} [s3]')
        if s3_schema_err:
            errors.append(s3_schema_err)
            continue

        # 3. Compare contents
        if local == s3:
            continue  # nominal: fully synced

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
        cl.add_message(f'Error running s5cmd copy to get init files: {result.stderr}')
        raise RuntimeError

    states = natsorted(glob(os.path.join(FINAL_STATES_DIR, '*', 'finalstate*.parquet')))
    if len(states) != len(list(glob(os.path.join(CONFIGS_DIR, '*')))):
        cl.add_message('s5cmd to fetch inits succeeded but expected files are not present')
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
            cl.add_message('Pre-flight validation starting (--local-is-truth: skipping S3 comparisons)')
        else:
            cl.add_message('Pre-flight validation starting')

        errors: list[str] = []
        warnings: list[str] = []
        local_ahead: dict[str, bool] = {}

        if not args.local_is_truth:
            sentinel_errors, sentinel_warnings, local_ahead = check_sentinels()
            errors += sentinel_errors
            warnings += sentinel_warnings

        for store in STORES:
            cl.add_message(f'  validating {store["name"]}')
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
            cl.add_message(
                f'Pre-flight validation warnings ({len(warnings)}):\n'
                + '\n'.join(f'  - {w}' for w in warnings)
            )

        if errors:
            cl.add_message(
                f'Pre-flight validation FAILED with {len(errors)} error(s):\n'
                + '\n'.join(f'  - {e}' for e in errors)
            )
            exit(1)

        cl.add_message('All stores pass validation.')
        check_for_init_files()
        cl.add_message('Init state files verified.')
        exit(0)
    except Exception as e:
        cl.add_message(str(e))
        cl.add_message(traceback.format_exc())
        exit(1)
    finally:
        cl.flush()
