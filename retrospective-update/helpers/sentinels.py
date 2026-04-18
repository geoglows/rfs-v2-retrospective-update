"""
Sentinel state for each zarr store — kept INSIDE the store at `<store>/sentinel.json`.

Schema:
  {
    "updated": "<ISO UTC>"   # when the current state was last written locally
  }

The sentinel travels with the data: it's uploaded alongside the zarr using
Pattern A (zarr body first, sentinel last as the upload "commit"). S3's
sentinel then mirrors local's sentinel after a successful upload, and
preflight can compare them by exact JSON equality and by `updated` ordering
with no time tolerances.

CLI:
    python sentinels.py set-updated hourly daily
"""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from set_env_vars import (
    DAILY_ZARR,
    HOURLY_ZARR,
    MONTHLY_TIMESERIES_ZARR,
    MONTHLY_TIMESTEPS_ZARR,
)

SENTINEL_FILENAME = 'sentinel.json'

# configs are static and tracked by an unconditional s5cmd sync, not a sentinel
STORE_ROOTS = {
    'hourly':             HOURLY_ZARR,
    'daily':              DAILY_ZARR,
    'monthly-timeseries': MONTHLY_TIMESERIES_ZARR,
    'monthly-timesteps':  MONTHLY_TIMESTEPS_ZARR,
}


def path_for(name: str) -> Path:
    return Path(STORE_ROOTS[name]) / SENTINEL_FILENAME


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


def read(name: str) -> dict | None:
    p = path_for(name)
    if not p.exists():
        return None
    return json.loads(p.read_text())


def set_updated(name: str) -> None:
    """Record the current local state with a fresh timestamp; call after any
    successful local write."""
    data = {'updated': _now_iso()}
    p = path_for(name)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(data, indent=2))


def _cli() -> int:
    if len(sys.argv) < 3:
        print(__doc__, file=sys.stderr)
        return 2
    cmd = sys.argv[1]
    names = sys.argv[2:]
    if cmd == 'set-updated':
        for n in names:
            set_updated(n)
    else:
        print(f'unknown command: {cmd}', file=sys.stderr)
        return 2
    return 0


if __name__ == '__main__':
    sys.exit(_cli())
