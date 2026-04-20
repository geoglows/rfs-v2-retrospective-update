import glob
import os
import traceback
from datetime import datetime

import cdsapi
import numpy as np
import pandas as pd
import xarray as xr
from dateutil import relativedelta
from natsort import natsorted

from helpers.cloud_logger import CloudLog
from helpers.set_env_vars import (
    HOURLY_ZARR, ERA5_DIR, MIN_LAG_TIME_DAYS
)


def download_era5() -> None:
    """
    1. determines the last simulated date in the daily zarr
    2. determines the list of days needed to download between the last simulation and today minus the lag time
    3. downloads the era5 data
    4. checks the era5 for covering the expected time range
    5. checks the era5 for compatible time steps with the existing discharge zarrs
    """
    last_date = xr.open_zarr(HOURLY_ZARR)['time'].values[-1]
    os.makedirs(ERA5_DIR, exist_ok=True)

    today = pd.to_datetime(datetime.now().date())
    last_date = pd.to_datetime(last_date)
    first_date_to_simulate = last_date + relativedelta.relativedelta(hours=1)
    final_date_to_simulate = today - pd.DateOffset(days=MIN_LAG_TIME_DAYS)

    if pd.to_datetime(last_date + np.timedelta64(MIN_LAG_TIME_DAYS, 'D')) > datetime.now():
        cl.add_message(f'Last date {last_date} is within {MIN_LAG_TIME_DAYS} (minimum lag time). Stopping.')
        raise EnvironmentError
    date_range = pd.date_range(start=first_date_to_simulate, end=final_date_to_simulate, freq='D', )

    if not len(date_range):
        cl.add_message(f'No dates to download between {first_date_to_simulate} and {final_date_to_simulate}')
        raise RuntimeError

    # make a list of unique year and month combinations in the list, in order. CDS API wants single month retrievals
    downloads = []
    year_month_combos = {(d.year, d.month) for d in date_range}
    year_month_combos = natsorted(year_month_combos)
    for year, month in year_month_combos:
        download_dates = [d for d in date_range if d.year == year and d.month == month]
        days = natsorted([d.day for d in download_dates])
        expected_file_name = os.path.join(ERA5_DIR, date_to_file_name(year, month, days))
        if not os.path.exists(expected_file_name):
            downloads.append((year, month, days, expected_file_name))

    for year, month, days, file in downloads:
        retrieve_data(year=year, month=month, days=days, file=file)

    downloaded_files = natsorted(glob.glob(os.path.join(ERA5_DIR, '*.nc')))
    if not downloaded_files:
        cl.add_message("No ERA5 files were downloaded. Unexpected logic error determining dates to download")
        raise RuntimeError

    for downloaded_file in downloaded_files:
        with xr.load_dataset(downloaded_file, chunks={'time': 'auto', 'lat': 'auto', 'lon': 'auto'}, ) as ds:
            # cdsapi does not validate dates, we need to validate that we got what we expected
            if ds['valid_time'].shape[0] == 0:
                os.remove(downloaded_file)
                continue

            # Make sure that the last timestep is T23:00 (i.e., a full day)
            if ds.valid_time[-1].values != np.datetime64(f'{ds.valid_time[-1].values.astype("datetime64[D]")}T23:00'):
                # Remove timesteps on partial days
                ds = ds.sel(valid_time=slice(None, np.datetime64(
                    f'{ds.valid_time[-1].values.astype("datetime64[D]")}') - np.timedelta64(1, 'h')))
                # If there is no more time after removing partial days, skip this file
                if len(ds.valid_time) == 0:
                    os.remove(downloaded_file)
                    continue
                ds.to_netcdf(downloaded_file)

    downloaded_files = list(natsorted(glob.glob(os.path.join(ERA5_DIR, '*.nc'))))
    if not downloaded_files:
        cl.add_message("No usable runoff data obtained from ERA5 download")
        raise RuntimeError

    with xr.open_mfdataset(downloaded_files) as ds, xr.open_zarr(HOURLY_ZARR) as hourly_ds:
        # Check the time dimension
        ro_time = ds['valid_time'].values
        retro_time = hourly_ds['time'].values
        total_time = np.concatenate((retro_time, ro_time))
        diffs = np.diff(total_time)
        if not np.all(diffs == diffs[0]):
            cl.add_message("Time dimension of ERA5 not consistent with routed discharge")
            raise RuntimeError

        # Check that there are no nans
        if np.isnan(ds['ro'].values).any():
            cl.add_message("Invalid data found in nans")
            raise RuntimeError

    # now check all era5 files in the directory, not only the barely downloaded ones.
    # check for unique, non-overlapping, sequential time steps that are consistent with the existing zarr time steps.
    all_era5_files = natsorted(glob.glob(os.path.join(ERA5_DIR, '*.nc')))
    all_times = []
    for era5_file in all_era5_files:
        with xr.open_dataset(era5_file) as ds:
            all_times.append(ds['valid_time'].values)
    all_times = np.concatenate(all_times)
    diffs = np.diff(all_times)
    if not np.all(diffs == diffs[0]):
        cl.add_message("Time dimension of ERA5 files not consistent across files")
        cl.add_message(f"Found inconsistency between newly downloaded and already existing era5 files.")
        raise RuntimeError


def date_to_file_name(year: int, month: int, days: list[int]) -> str:
    # Creates a zero padded YYYYMMDD date string from integer ymd for era5 files
    padded_month = str(month).zfill(2)
    padded_day_0 = str(days[0]).zfill(2)
    padded_day_1 = str(days[-1]).zfill(2)
    return f'era5_{year}{padded_month}{padded_day_0}-{year}{padded_month}{padded_day_1}.nc'


def retrieve_data(year: int, month: int, days: list[int], file: str, ) -> None:
    if os.path.exists(file):
        return
    c = cdsapi.Client()
    c.retrieve(
        'reanalysis-era5-single-levels',
        {
            'product_type': ['reanalysis'],
            'download_format': 'unarchived',
            'data_format': 'netcdf',
            'variable': ['runoff'],
            'year': year,
            'month': str(month).zfill(2),
            'day': [str(day).zfill(2) for day in days],
            'time': [f'{x:02d}:00' for x in range(0, 24)],
        },
        target=file
    )


if __name__ == '__main__':
    cl = CloudLog()
    try:
        cl.add_message('Preparing ERA5 data')
        download_era5()
        exit(0)
    except Exception as e:
        cl.add_message(str(e))
        cl.add_message(traceback.format_exc())
        exit(1)
    finally:
        cl.add_message('Finished preparing ERA5 data')
