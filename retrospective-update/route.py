import os
import traceback
from datetime import datetime
from glob import glob
from multiprocessing import Pool

import netCDF4 as nc
import numpy as np
import pandas as pd
import river_route as rr
import xarray as xr
from natsort import natsorted
from netCDF4 import Dataset, date2num
from tqdm import tqdm

from helpers.cloud_logger import CloudLog
from helpers.set_env_vars import (
    FINAL_STATES_DIR, CONFIGS_DIR, DISCHARGE_DIR, FORECAST_INITS_DIR, ERA5_DIR, HOURLY_ZARR
)


def _write_outflows_fixed(df: pd.DataFrame, outflow_file: str, runoff_file: str) -> None:
    # Workaround for river_route 1.3.0 + numpy 2.x: the library writes
    # `df.index.values - df.index.values[0]` directly into a netCDF variable,
    # which yields a timedelta64 array that netCDF4 rejects with
    # "cannot include dtype 'm' in a buffer". Cast to int seconds first.
    with nc.Dataset(outflow_file, mode='w', format='NETCDF4') as ds:
        ds.createDimension('time', size=df.shape[0])
        ds.createDimension('river_id', size=df.shape[1])

        time_var = ds.createVariable('time', 'f8', ('time',))
        time_var.units = f'seconds since {df.index[0].strftime("%Y-%m-%d %H:%M:%S")}'
        time_var[:] = (df.index.values - df.index.values[0]).astype('timedelta64[s]').astype(np.int64)

        id_var = ds.createVariable('river_id', 'i4', ('river_id',))
        id_var[:] = df.columns.values

        flow_var = ds.createVariable('Q', 'f4', ('time', 'river_id'))
        flow_var[:] = df.values
        flow_var.long_name = 'Discharge at catchment outlet'
        flow_var.standard_name = 'discharge'
        flow_var.aggregation_method = 'mean'
        flow_var.units = 'm3 s-1'


def route_vpu(args):
    config_dir, era5_files, init_timestamp, final_timestamp = args
    vpu = os.path.basename(config_dir)
    params_file = os.path.join(config_dir, 'routing_parameters.parquet')
    weight_table = os.path.join(config_dir, f'gridweights_ERA5_{vpu}.nc')
    connectivity_file = os.path.join(config_dir, 'connectivity.parquet')
    initial_state_file = os.path.join(FINAL_STATES_DIR, vpu, f'finalstate_{init_timestamp}.parquet')
    final_state_file = os.path.join(FINAL_STATES_DIR, vpu, f'finalstate_{final_timestamp}.parquet')

    if not os.path.exists(params_file):
        raise FileNotFoundError(f"Routing parameters file not found: {params_file}")
    if not os.path.exists(connectivity_file):
        raise FileNotFoundError(f"Connectivity file not found: {connectivity_file}")
    if not os.path.exists(weight_table):
        raise FileNotFoundError(f"Weight table file not found: {weight_table}")
    if not os.path.exists(initial_state_file):
        raise FileNotFoundError(f"Initial state file not found: {initial_state_file}")

    outdir = os.path.join(DISCHARGE_DIR, vpu)
    os.makedirs(outdir, exist_ok=True)

    output_files = [
        os.path.join(outdir, os.path.basename(era5_file).replace('era5_', 'Q_')) for era5_file in era5_files
    ]

    if os.path.exists(final_state_file):
        return

    (
        rr
        .Muskingum(
            routing_params_file=params_file,
            connectivity_file=connectivity_file,
            runoff_depths_file=era5_files,
            weight_table_file=weight_table,
            var_t='valid_time',
            var_x='longitude',
            var_y='latitude',
            outflow_file=output_files,
            initial_state_file=initial_state_file,
            final_state_file=final_state_file,
            progress_bar=False,
            log=False,
        )
        .set_write_outflows(_write_outflows_fixed)
        .route()
    )


def drop_coords(ds: xr.Dataset, qout: str = 'Q'):
    """
    Helps load faster, gets rid of variables/dimensions we do not need (lat, lon, etc.)

    Parameters:
        ds (xr.Dataset): The input dataset.
        qout (str): The variable name to keep in the dataset.

    Returns:
        xr.Dataset: The modified dataset with only the specified variable.
    """
    return ds[[qout, ]].reset_coords(drop=True)


def make_rapid_style_inits(args) -> None:
    vpu, final_timestamp = args
    date_value = datetime.strptime(final_timestamp, '%Y%m%d%H%M')

    config_file = os.path.join(vpu, "routing_parameters.parquet")

    # Load data
    rivid = pd.read_parquet(config_file)["river_id"].values.astype(np.int32)
    lat = np.zeros_like(rivid, dtype=np.float64)
    lon = np.zeros_like(rivid, dtype=np.float64)

    expected_qinit_file = os.path.join(FINAL_STATES_DIR, os.path.basename(vpu), f'finalstate_{final_timestamp}.parquet')
    qinit = np.asarray(pd.read_parquet(expected_qinit_file)["Q"].values).copy()
    qinit[qinit < 0] = 0

    # Time info
    time_units = f"seconds since {date_value.strftime('%Y-%m-%d %H:%M:%S')}"
    calendar = "gregorian"
    time_num = date2num(date_value, units=time_units, calendar=calendar)

    # Create output dataset
    output_basedir = os.path.join(
        FORECAST_INITS_DIR,
        f"Qinit_{date_value.strftime('%Y%m%d00')}",
        vpu.split("=")[-1]
    )
    os.makedirs(output_basedir, exist_ok=True)
    output_full_path = os.path.join(output_basedir, f"Qinit_{date_value.strftime('%Y%m%d00')}.nc")
    with Dataset(output_full_path, "w", format="NETCDF4") as nc:
        nc.Conventions = "CF-1.6"
        nc.featureType = "timeSeries"

        # Dimensions
        nc.createDimension("time", 1)
        nc.createDimension("rivid", len(rivid))

        # Variables
        Qout_var = nc.createVariable("Qout", "f8", ("time", "rivid"))
        Qout_var.long_name = "instantaneous river water discharge downstream of each river reach"
        Qout_var.units = "m3 s-1"
        Qout_var.coordinates = "lon lat"
        Qout_var.grid_mapping = "crs"
        Qout_var.cell_methods = "time: point"

        rivid_var = nc.createVariable("rivid", "i4", ("rivid",))
        rivid_var.long_name = "unique identifier for each river reach"
        rivid_var.units = "1"
        rivid_var.cf_role = "timeseries_id"

        time_var = nc.createVariable("time", "i4", ("time",))
        time_var.long_name = "time"
        time_var.standard_name = "time"
        time_var.units = time_units
        time_var.axis = "T"
        time_var.calendar = calendar

        lon_var = nc.createVariable("lon", "f8", ("rivid",))
        lon_var.long_name = "longitude of a point related to each river reach"
        lon_var.standard_name = "longitude"
        lon_var.units = "degrees_east"
        lon_var.axis = "X"

        lat_var = nc.createVariable("lat", "f8", ("rivid",))
        lat_var.long_name = "latitude of a point related to each river reach"
        lat_var.standard_name = "latitude"
        lat_var.units = "degrees_north"
        lat_var.axis = "Y"

        crs_var = nc.createVariable("crs", "i4")
        crs_var.grid_mapping_name = "latitude_longitude"
        crs_var.epsg_code = "EPSG:4326"
        crs_var.semi_major_axis = 6378137.0
        crs_var.inverse_flattening = 298.257223563

        # Assign values
        rivid_var[:] = rivid
        lat_var[:] = lat
        lon_var[:] = lon
        time_var[0] = time_num
        Qout_var[0, :] = qinit
        crs_var.assignValue(0)


if __name__ == '__main__':
    cl = CloudLog()
    try:
        # determine the first and last time step that will come out of routing by reading the era5 files
        era5_data = natsorted(glob(os.path.join(ERA5_DIR, 'era5_*.nc')))
        init_timestamp = pd.to_datetime(xr.open_zarr(HOURLY_ZARR).time[-1].values).strftime('%Y%m%d%H%M')
        final_timestamp = pd.to_datetime(xr.open_dataset(era5_data[-1]).valid_time[-1].values).strftime('%Y%m%d%H%M')

        init_timestamp_era5 = pd.to_datetime(xr.open_dataset(era5_data[0]).valid_time[0].values) - pd.Timedelta(hours=1)
        init_timestamp_era5 = init_timestamp_era5.strftime('%Y%m%d%H%M')
        if init_timestamp != init_timestamp_era5:
            cl.add_message('Last time step in the zarr timeseries is different than the first era5')
            raise RuntimeError
        vpus = natsorted(glob(os.path.join(CONFIGS_DIR, '*')))

        with Pool(os.cpu_count()) as p:
            cl.add_message('Routing')
            list(
                tqdm(
                    p.imap_unordered(route_vpu, [(vpu, era5_data, init_timestamp, final_timestamp) for vpu in vpus]),
                    total=len(vpus), desc='Routing VPUs'
                ),
            )

            cl.add_message('Making Forecast Inits')
            list(
                tqdm(
                    p.imap_unordered(make_rapid_style_inits, [(vpu, final_timestamp) for vpu in vpus]),
                    total=len(vpus), desc='Making forecast inits'
                )
            )
        exit(0)
    except Exception as e:
        cl.add_message(str(e))
        cl.add_message(traceback.format_exc())
        exit(1)
    finally:
        cl.flush()
