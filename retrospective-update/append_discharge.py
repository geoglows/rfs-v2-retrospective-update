import os
import traceback
from glob import glob

import numpy as np
import xarray as xr
import zarr
from dask.distributed import Client, LocalCluster
from natsort import natsorted

from helpers.cloud_logger import CloudLog
from helpers.set_env_vars import (
    DAILY_ZARR, HOURLY_ZARR, DISCHARGE_DIR,
    DASK_N_WORKERS, DASK_THREADS_PER_WORKER, DASK_MEMORY_LIMIT,
)
from helpers.validators import validate_internal, validate_time


def _resize_time_arrays(zarr_path: str, new_len: int) -> None:
    """Resize every array whose first dim is `time` to the given length. Metadata-only."""
    zroot = zarr.open(zarr_path, mode='r+')
    for vname in zroot.array_keys():
        a = zroot[vname]
        dims = list(a.attrs.get('_ARRAY_DIMENSIONS', []))
        if dims and dims[0] == 'time':
            a.resize((new_len,) + a.shape[1:])


def _write_time_values(zarr_path: str, start: int, values: np.ndarray) -> None:
    """Encode datetime64 `values` per the existing zarr time array's `units` and write them at [start:start+N].

    xarray's region write leaves dim coords alone; we overwrite explicitly so
    the resize fill (NaT / NaN) doesn't survive.
    """
    zroot = zarr.open(zarr_path, mode='r+')
    tarr = zroot['time']
    units = tarr.attrs.get('units', '')
    unit, _, epoch_str = units.partition(' since ')
    epoch = np.datetime64(epoch_str.strip())
    factor_ns = {
        'seconds': int(1e9),
        'hours': int(3600e9),
        'days': int(86400e9),
    }[unit.strip()]
    ns_since = (values.astype('datetime64[ns]') - epoch).astype('int64')
    encoded = (ns_since / factor_ns).astype(tarr.dtype)
    tarr[start:start + len(values)] = encoded


def append(new_ds: xr.Dataset, zarr_path: str) -> None:
    earliest_date = np.datetime_as_string(new_ds.time[0].values, unit="h")
    latest_date = np.datetime_as_string(new_ds.time[-1].values, unit="h")

    with xr.open_zarr(zarr_path, consolidated=False) as existing:
        if new_ds.time.values[0] in existing.time.values:
            cl.error(f'Time steps for {earliest_date} to {latest_date} already in zarr. Needs human intervention.')
            raise RuntimeError
        if new_ds.river_id.shape != existing.river_id.shape:
            cl.error(f'River id shape mismatch. Probably corrupt or missing netcdfs.')
            raise RuntimeError
        n_old = int(existing.sizes['time'])

    n_add = int(new_ds.sizes['time'])
    n_new = n_old + n_add

    # match dask chunks to 100 zarr river-chunks per dask chunk so that one
    # dask task rewrites 100 adjacent zarr chunks (minimises task overhead
    # while keeping per-task data small enough to stream)
    zq = zarr.open(zarr_path, mode='r')['Q']
    river_chunk = int(zq.chunks[1])
    dask_river_chunk = river_chunk * 100

    # drop any variables (e.g. river_id) that don't have `time` as a dim —
    # region writes require every variable to cover the region dim
    region_ds = new_ds.drop_vars([v for v in new_ds.variables if 'time' not in new_ds[v].dims])

    cl.log(f'Appending time steps {earliest_date} to {latest_date} on {zarr_path}')
    _resize_time_arrays(zarr_path, n_new)
    try:
        (
            region_ds
            .chunk({'time': -1, 'river_id': dask_river_chunk})
            # scatter the loaded numpy data out to the workers once, so the
            # task graph sent per-chunk references futures instead of embedding
            # the full ~2 GB inline (eliminates the "large graph" warning and
            # avoids the repeated serialization cost)
            .persist()
            .to_zarr(
                zarr_path,
                region={'time': slice(n_old, n_new)},
                mode='r+',
                # forces reads off .zarray — .zmetadata is stale until we consolidate at the end of the run
                consolidated=False,
                # append is not a full time-chunk length so xarray safe_chunks will reject it
                # the write regions were chosen based on known chunk sizes so skip this safely
                safe_chunks=False,
            )
        )
        # We only appended to Q, need to specifically fill in the time values also.
        _write_time_values(zarr_path, n_old, new_ds.time.values)
    except Exception:
        # on any failure, try to put back the shape back so a subsequent run could succeed
        _resize_time_arrays(zarr_path, n_old)
        raise
    cl.log(f'Finished appending to zarr: {zarr_path}')
    return


def concatenate_outputs() -> None:
    # for each unique start date, sorted in order, open/merge the files from all vpus and append to the zarr
    vpu_outputs = natsorted(glob(os.path.join(DISCHARGE_DIR, '*')))
    unique_outputs = [os.path.basename(f) for f in natsorted(glob(os.path.join(vpu_outputs[0], '*')))]
    if not unique_outputs:
        cl.error(f"No Qout files found in {DISCHARGE_DIR}")
        raise FileNotFoundError

    for unique_output in unique_outputs:
        discharges = list(natsorted(glob(os.path.join(DISCHARGE_DIR, '*', unique_output))))
        if not len(discharges) == len(vpu_outputs):
            cl.error(f"Discharge not found for {unique_output}")
            raise FileNotFoundError

        with xr.open_mfdataset(discharges, combine='nested', concat_dim='river_id') as new_ds:
            # load all 125 VPU netcdfs up-front in parallel — measured to be
            # faster than leaving it lazy and letting dask re-read during append
            new_ds.load()
            append(new_ds=new_ds, zarr_path=HOURLY_ZARR)
            new_ds = new_ds.resample(time='1D').mean('time')
            append(new_ds=new_ds, zarr_path=DAILY_ZARR)
    return


def verify_concatenated_outputs(zarr_path) -> None:
    # Note: only Q's 2nd time-chunk is synced locally so checks only apply to that range
    cl.log(f'Verifying {zarr_path} zarr after appending')
    q_time_chunk = int(zarr.open(zarr_path, mode='r')['Q'].chunks[0])
    with xr.open_zarr(zarr_path, consolidated=False) as ds:
        q_vals = ds.isel(river_id=1, time=slice(q_time_chunk, None))['Q'].values
        if np.isnan(q_vals).any():
            cl.error(f'{zarr_path} contains NaNs in Q chunk 1')
            raise RuntimeError

        times = ds['time'].values
        if not np.all(np.diff(times) == times[1] - times[0]):
            cl.error(f'Time dimension of {zarr_path} zarr is not correct')
            raise RuntimeError


def _assert_stores_consistent(label: str) -> None:
    """Run the shared internal + time checks; raise if any issue is found."""
    issues: list[str] = []
    for z in [HOURLY_ZARR, DAILY_ZARR]:
        issues += validate_internal(z, f'{label} {z}')
        issues += validate_time(z, f'{label} {z}')
    if issues:
        for issue in issues:
            cl.error(f'  - {issue}')
        raise RuntimeError(f'{label}: {len(issues)} store validation issue(s)')


if __name__ == '__main__':
    cl = CloudLog()
    cluster = LocalCluster(
        n_workers=DASK_N_WORKERS,
        threads_per_worker=DASK_THREADS_PER_WORKER,
        memory_limit=DASK_MEMORY_LIMIT,
    )
    client = Client(cluster)
    try:
        cl.log(f'Dask client: {client.dashboard_link}')
        # fail fast if the stores are somehow corrupt before we start writing
        _assert_stores_consistent('pre-append')
        cl.log('Appending new discharge to zarr files')
        concatenate_outputs()
        for z in [DAILY_ZARR, HOURLY_ZARR]:
            verify_concatenated_outputs(z)
            zarr.consolidate_metadata(z)
        # re-check after consolidation — especially looking for .zarray and .zmetadata to agree
        _assert_stores_consistent('post-consolidate')
        cl.log('Discharge zarrs updated successfully')
    except Exception as e:
        cl.error(str(e))
        cl.error(traceback.format_exc())
        exit(1)
    finally:
        client.close()
        cluster.close()
