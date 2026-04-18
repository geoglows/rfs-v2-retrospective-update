# mostly bot generated functions to check for internal consistency of a zarr v2
# Shared zarr-store consistency checks used by preflight_validation.py and append_discharge.py.
# Each function returns a list of human-readable issue strings.
# An empty list means the check passed.

import json
from pathlib import Path

import numpy as np
import zarr


def validate_internal(zarr_path: str, label: str) -> list[str]:
    """Internal consistency of a zarr store: `.zmetadata` vs `.zarray`, and
    every time-dim array's length matches the time coord."""
    issues: list[str] = []
    try:
        z = zarr.open(zarr_path, mode='r')
    except Exception as e:
        return [f'{label}: cannot open zarr: {type(e).__name__}: {e}']

    zmeta_path = Path(zarr_path) / '.zmetadata'
    if zmeta_path.exists():
        try:
            zmeta = json.loads(zmeta_path.read_text())
            for key, val in zmeta['metadata'].items():
                if not key.endswith('/.zarray'):
                    continue
                arr_name = key.rsplit('/', 1)[0]
                try:
                    live_shape = tuple(int(x) for x in z[arr_name].shape)
                except KeyError:
                    issues.append(f'{label}: {arr_name} listed in .zmetadata but missing on disk')
                    continue
                cons_shape = tuple(val['shape'])
                if live_shape != cons_shape:
                    issues.append(
                        f'{label}: {arr_name} shape mismatch — .zarray={live_shape} '
                        f'vs .zmetadata={cons_shape}'
                    )
        except Exception as e:
            issues.append(f'{label}: .zmetadata unparseable: {e}')

    array_names = list(z.array_keys())
    if 'time' in array_names:
        t_len = int(z['time'].shape[0])
        for arr_name in array_names:
            a = z[arr_name]
            dims = list(a.attrs.get('_ARRAY_DIMENSIONS', []))
            if 'time' in dims:
                ax = dims.index('time')
                if int(a.shape[ax]) != t_len:
                    issues.append(
                        f'{label}: {arr_name} dim[{ax}]={a.shape[ax]} but '
                        f'time coord length={t_len}'
                    )

    return issues


def _decode_time(t_raw: np.ndarray, units: str) -> np.ndarray:
    """Decode a raw zarr time array into datetime64[ns] per its CF units."""
    unit, _, epoch_str = units.partition(' since ')
    epoch = np.datetime64(epoch_str.strip())
    factor_ns = {
        'seconds': int(1e9),
        'hours':   int(3600e9),
        'days':    int(86400e9),
    }[unit.strip()]
    return epoch + (t_raw * factor_ns).astype('timedelta64[ns]')


def validate_time(zarr_path: str, label: str, freq: str = 'uniform') -> list[str]:
    """Time coord integrity: no NaT/NaN, no duplicates, and regular spacing.

    freq:
      'uniform' — every diff must be identical (hourly, daily stores).
      'monthly' — diffs must be 28-31 days and monotonically increasing
                  (month-start stores, where day-length varies).
    """
    issues: list[str] = []
    try:
        z = zarr.open(zarr_path, mode='r')
    except Exception as e:
        return [f'{label}: cannot open zarr: {type(e).__name__}: {e}']

    if 'time' not in list(z.array_keys()):
        return issues

    t = z['time'][:]
    nan_count = int(np.isnan(t).sum())
    if nan_count:
        issues.append(f'{label}: time has {nan_count} NaN/NaT values')

    valid = t[~np.isnan(t)]
    if len(valid) != len(np.unique(valid)):
        dups = len(valid) - len(np.unique(valid))
        issues.append(f'{label}: time has {dups} duplicate value(s)')

    if len(valid) < 2:
        return issues

    if freq == 'uniform':
        diffs = np.diff(valid)
        uniq = np.unique(diffs)
        if len(uniq) > 1:
            issues.append(f'{label}: time deltas not uniform (unique diffs: {uniq.tolist()})')
    elif freq == 'monthly':
        units = z['time'].attrs.get('units', '')
        try:
            decoded = _decode_time(valid, units)
        except Exception as e:
            issues.append(f'{label}: could not decode time for monthly check: {e}')
            return issues
        diffs_days = np.diff(decoded) / np.timedelta64(1, 'D')
        if np.any(diffs_days <= 0):
            issues.append(f'{label}: time not monotonically increasing')
        bad = diffs_days[(diffs_days < 28) | (diffs_days > 31)]
        if len(bad):
            issues.append(
                f'{label}: {len(bad)} time gap(s) outside 28-31 days '
                f'(sample: {bad[:5].tolist()})'
            )
    else:
        issues.append(f'{label}: unknown freq={freq!r} passed to validate_time')

    return issues
