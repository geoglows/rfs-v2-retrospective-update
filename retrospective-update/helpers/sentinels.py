"""
Each zarr store has a `sentinel.json` at its root: a one-field dict
`{"updated": "<ISO UTC>"}`. Written after every successful local change and
uploaded last (Pattern A) so its presence on S3 commits a synced state.

Reads store paths from env vars directly — no cross-module imports — so
this file works the same whether it's imported or run as a script.

CLI:
    python -m helpers.sentinels hourly daily
"""
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

SENTINEL_FILENAME = 'sentinel.json'

_ENV_VAR = {
    'hourly':             'HOURLY_ZARR',
    'daily':              'DAILY_ZARR',
    'monthly-timeseries': 'MONTHLY_TIMESERIES_ZARR',
    'monthly-timesteps':  'MONTHLY_TIMESTEPS_ZARR',
}


def path_for(name: str) -> Path:
    return Path(os.environ[_ENV_VAR[name]]) / SENTINEL_FILENAME


def read(name: str) -> dict | None:
    p = path_for(name)
    return json.loads(p.read_text()) if p.exists() else None


if __name__ == '__main__':
    if len(sys.argv) < 2:
        sys.exit('usage: python -m helpers.sentinels <name> [<name>...]')
    now = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    content = json.dumps({'updated': now}, indent=2)
    for name in sys.argv[1:]:
        p = path_for(name)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(content)