"""
Roll back zarr stores to a safe "known good" end time.

For each configured store the script:
  1. decodes the on-disk `time` array and finds the index whose value
     equals the configured target timestamp
  2. for every data variable with a `time` axis:
     - resizes the array to the new length (metadata-only, instant)
     - deletes chunk files that fall entirely past the new boundary (parallel)
     - rewrites the one boundary chunk per river-group so that rows past
       the new local boundary are set to fill_value (parallel)
  3. consolidates `.zmetadata` so `.zarray` and `.zmetadata` agree

The ordering (resize → delete orphans → rewrite boundary → consolidate) keeps
the store readable at every step: after step 1 the shape already hides any
stale bytes past the new boundary, and if the script is interrupted mid-run
the worst case is leftover orphan files on disk, never invalid data.

Chunks that belong to time-chunk 0 are never touched (they contain only the
long historical record). For hourly / daily / monthly-timeseries those live
only on S3 (synced with `--exclude "*Q/0.*"`), so they can't be touched even
if we wanted to.

After a successful run:
  - `cp -al` a backup or snapshot before re-running is cheap insurance
  - upload the local stores back to S3 with `s5cmd cp "<dir>/*" "s3://…/"`
    (cp, not sync — we do NOT want to --delete the Q/0.* historical chunks
    on S3 that aren't local)

Usage:
    python rollback_zarrs.py                              # dry run (default)
    python rollback_zarrs.py --only hourly                # one store, dry run
    python rollback_zarrs.py --execute                    # mutate, all stores
    python rollback_zarrs.py --execute --workers 20       # tune parallelism
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from itertools import product
from multiprocessing import Pool
from pathlib import Path

import numcodecs
import numpy as np
import zarr


WORK_DIR = Path('/Users/Shared/workdirs/rfs-v2-retrospective-update')


# The LAST time value to keep (inclusive). New array length = index-of-target + 1.
# Monthly stores currently end at 2026-02, which is the correct target, so they're no-ops.
TARGETS: dict[str, tuple[Path, str]] = {
    'hourly':             (WORK_DIR / 'hourly.zarr',             '2026-04-11T23:00:00'),
    'daily':              (WORK_DIR / 'daily.zarr',              '2026-04-11T00:00:00'),
    'monthly-timeseries': (WORK_DIR / 'monthly-timeseries.zarr', '2026-03-01T00:00:00'),
    'monthly-timesteps':  (WORK_DIR / 'monthly-timesteps.zarr',  '2026-03-01T00:00:00'),
}


def decode_time(raw_values: np.ndarray, units: str) -> np.ndarray:
    unit, _, epoch_str = units.partition(' since ')
    epoch = np.datetime64(epoch_str.strip())
    factor_ns = {
        'seconds': int(1e9),
        'hours':   int(3600e9),
        'days':    int(86400e9),
    }[unit.strip()]
    return epoch + (raw_values * factor_ns).astype('timedelta64[ns]')


def find_cut_length(decoded: np.ndarray, target_ts: str) -> int:
    target = np.datetime64(target_ts)
    matches = np.where(decoded == target)[0]
    if len(matches) == 0:
        return -1
    return int(matches[-1]) + 1


_WORKER_CTX: dict = {}


def _init_worker(ctx: dict) -> None:
    _WORKER_CTX.clear()
    _WORKER_CTX.update(ctx)


def _rewrite_boundary_chunk(chunk_path_str: str) -> str | None:
    """Decompress a chunk, set rows >= boundary to fill_value, recompress, rewrite."""
    chunks_shape = _WORKER_CTX['chunks_shape']
    dtype = np.dtype(_WORKER_CTX['dtype_str'])
    compressor_config = _WORKER_CTX['compressor_config']
    fill_value = _WORKER_CTX['fill_value']
    boundary_local_row = _WORKER_CTX['boundary_local_row']

    comp = numcodecs.get_codec(compressor_config) if compressor_config else None
    p = Path(chunk_path_str)
    try:
        raw = p.read_bytes()
        buf = comp.decode(raw) if comp else raw
        arr = np.frombuffer(buf, dtype=dtype).reshape(chunks_shape).copy()
        arr[boundary_local_row:] = fill_value
        out = comp.encode(arr) if comp else arr.tobytes()
        # write to a temp file then atomic rename so a crash mid-write can't
        # leave a half-written chunk
        tmp = p.with_suffix(p.suffix + '.tmp')
        tmp.write_bytes(out)
        os.replace(tmp, p)
        return None
    except Exception as e:
        return f'{chunk_path_str}: {type(e).__name__}: {e}'


def _delete_file(path_str: str) -> str | None:
    try:
        os.remove(path_str)
        return None
    except FileNotFoundError:
        return None
    except Exception as e:
        return f'{path_str}: {type(e).__name__}: {e}'


def _iter_chunk_paths(store: Path, arr_name: str, shape, chunks, old_t_chunks: int):
    """Yield (t_idx, other_coord_tuple, path) for every chunk that exists on disk."""
    var_dir = store / arr_name
    other_ranges = [
        range((shape[i] + chunks[i] - 1) // chunks[i]) for i in range(1, len(shape))
    ]
    other_combos = list(product(*other_ranges)) if other_ranges else [()]
    for t_idx in range(old_t_chunks):
        for other in other_combos:
            name_parts = [str(t_idx)] + [str(o) for o in other]
            p = var_dir / '.'.join(name_parts)
            if p.exists():
                yield t_idx, other, p


def rollback_one_zarr(name: str, store: Path, target_ts: str,
                      workers: int, execute: bool) -> bool:
    print(f'\n=== {name} ===  store={store}')
    if not store.exists():
        print(f'  SKIP: store does not exist')
        return True

    root = zarr.open(store, mode='r+' if execute else 'r')

    time_arr = root['time'][:]
    units = dict(root['time'].attrs).get('units', 'seconds since 1970-01-01')
    decoded = decode_time(time_arr, units)
    old_len = int(decoded.shape[0])
    new_len = find_cut_length(decoded, target_ts)

    print(f'  target: {target_ts}')
    print(f'  current: length={old_len}, range=[{decoded[0]} .. {decoded[-1]}]')

    if new_len == -1:
        print(f'  ERROR: target timestamp not found in time coord')
        return False
    if new_len == old_len:
        print(f'  no-op: already at target length')
        return True
    if new_len > old_len:
        print(f'  ERROR: new_len {new_len} > old_len {old_len}')
        return False

    print(f'  rollback: new length={new_len}, last kept value={decoded[new_len-1]}')
    print(f'  dropping {old_len - new_len} time steps')

    arrays_with_time = []
    for arr_name in sorted(root.array_keys()):
        a = root[arr_name]
        dims = list(a.attrs.get('_ARRAY_DIMENSIONS', []))
        if 'time' not in dims:
            continue
        if dims.index('time') != 0:
            print(f'  SKIP {arr_name}: time not on axis 0')
            continue
        arrays_with_time.append(arr_name)

    total_delete = 0
    total_rewrite = 0
    for arr_name in arrays_with_time:
        a = root[arr_name]
        shape = tuple(a.shape)
        chunks = tuple(a.chunks)
        t_chunk = chunks[0]
        old_t_chunks = (shape[0] + t_chunk - 1) // t_chunk
        new_t_chunks = (new_len + t_chunk - 1) // t_chunk
        boundary_chunk_idx = new_t_chunks - 1
        boundary_local_row = new_len - boundary_chunk_idx * t_chunk
        exact_fit = (new_len % t_chunk == 0)

        print(f'\n  -- {arr_name}: shape={shape} chunks={chunks}')
        print(f'     time-chunks: old={old_t_chunks} -> new={new_t_chunks}')
        print(f'     boundary chunk index: {boundary_chunk_idx}')
        print(f'     keep {boundary_local_row}/{t_chunk} rows in boundary chunk '
              f'(exact fit: {exact_fit})')

        rewrite_paths: list[str] = []
        delete_paths: list[str] = []
        for t_idx, _other, p in _iter_chunk_paths(store, arr_name, shape, chunks, old_t_chunks):
            if t_idx < boundary_chunk_idx:
                continue
            if t_idx == boundary_chunk_idx:
                if not exact_fit:
                    rewrite_paths.append(str(p))
            else:
                delete_paths.append(str(p))

        print(f'     chunks to rewrite: {len(rewrite_paths)}')
        print(f'     chunks to delete:  {len(delete_paths)}')
        total_rewrite += len(rewrite_paths)
        total_delete += len(delete_paths)

        if not execute:
            continue

        new_shape = (new_len,) + shape[1:]
        a.resize(new_shape)
        print(f'     resized {arr_name} -> {new_shape}')

        if delete_paths:
            t0 = time.time()
            errors = []
            with Pool(workers) as pool:
                for err in pool.imap_unordered(_delete_file, delete_paths, chunksize=128):
                    if err:
                        errors.append(err)
            print(f'     deleted {len(delete_paths) - len(errors)} chunks in {time.time()-t0:.1f}s'
                  + (f' ({len(errors)} errors)' if errors else ''))
            for e in errors[:5]:
                print(f'       ! {e}')

        if rewrite_paths:
            t0 = time.time()
            fill_value = a.fill_value
            if fill_value is None:
                fill_value = np.zeros(1, dtype=a.dtype)[0]
            comp_cfg = a.compressor.get_config() if a.compressor is not None else None
            ctx = {
                'chunks_shape': tuple(int(x) for x in chunks),
                'dtype_str': a.dtype.str,
                'compressor_config': comp_cfg,
                'fill_value': fill_value,
                'boundary_local_row': int(boundary_local_row),
            }
            errors = []
            n_done = 0
            total = len(rewrite_paths)
            with Pool(workers, initializer=_init_worker, initargs=(ctx,)) as pool:
                for err in pool.imap_unordered(_rewrite_boundary_chunk, rewrite_paths, chunksize=32):
                    n_done += 1
                    if err:
                        errors.append(err)
                    if n_done % 5000 == 0 or n_done == total:
                        el = time.time() - t0
                        rate = n_done / el if el else 0
                        eta = (total - n_done) / rate if rate else 0
                        print(f'       rewrote {n_done}/{total}  rate={rate:.0f}/s  eta={eta:.0f}s')
            print(f'     rewrote {total - len(errors)} chunks in {time.time()-t0:.1f}s'
                  + (f' ({len(errors)} errors)' if errors else ''))
            for e in errors[:5]:
                print(f'       ! {e}')
            if errors:
                return False

    print(f'\n  summary for {name}: would delete {total_delete}, would rewrite {total_rewrite}')

    if execute:
        print(f'  consolidating metadata...')
        t0 = time.time()
        zarr.consolidate_metadata(root.store)
        print(f'  consolidated in {time.time()-t0:.1f}s')

        # verification pass
        root2 = zarr.open(store, mode='r')
        t2 = root2['time'][:]
        dec2 = decode_time(t2, dict(root2['time'].attrs).get('units', units))
        print(f'  VERIFY: new length={t2.shape[0]}, last time={dec2[-1]}')
        for arr_name in arrays_with_time:
            a2 = root2[arr_name]
            if a2.shape[0] != new_len:
                print(f'  *** VERIFY FAILED: {arr_name} shape[0]={a2.shape[0]} != {new_len}')
                return False
        print(f'  ok.')

    return True


def main() -> int:
    ap = argparse.ArgumentParser(
        description='Parallel rollback of zarr stores to a target end time.',
    )
    ap.add_argument('--workers', type=int, default=os.cpu_count() or 8,
                    help='number of worker processes (default: cpu_count)')
    ap.add_argument('--only', nargs='*', default=None, choices=list(TARGETS.keys()),
                    help='subset of zarr stores to process')
    ap.add_argument('--execute', action='store_true',
                    help='actually mutate stores (default: dry run)', default=False)
    args = ap.parse_args()

    if not args.execute:
        print('*** DRY RUN — add --execute to actually modify zarrs ***')
    print(f'workers: {args.workers}')

    overall_ok = True
    overall_t0 = time.time()
    for name, (path, target) in TARGETS.items():
        if args.only and name not in args.only:
            continue
        ok = rollback_one_zarr(name, path, target, args.workers, args.execute)
        overall_ok = overall_ok and ok

    print(f'\n{"="*60}')
    print(f'total elapsed: {time.time() - overall_t0:.1f}s')
    print(f'status: {"OK" if overall_ok else "FAILED"}')

    if args.execute and overall_ok:
        print('\nNext steps:')
        print('  1. verify locally by opening each zarr (e.g., run explore_s3_copies.py)')
        print('  2. upload the rolled-back stores to S3 with cp (NOT sync --delete,')
        print('     because the historical Q/0.* chunks live only on S3):')
        print('       s5cmd --credentials-file "$AWS_CREDENTIALS_FILE" cp \\')
        print('         /Users/Shared/workdirs/rfs-v2-retrospective-update/hourly.zarr/\\* \\')
        print('         s3://geoglows-v2/retrospective/hourly.zarr/')
        print('     (repeat for daily.zarr; monthly-* were no-ops so no upload needed)')
        print('  3. clear transient work dirs and re-run main.sh normally')

    return 0 if overall_ok else 1


if __name__ == '__main__':
    sys.exit(main())
