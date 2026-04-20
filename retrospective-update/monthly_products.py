import os
import traceback

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
import rasterio.features
import rasterio.transform
import xarray as xr
from natsort import natsorted

from helpers.cloud_logger import CloudLog
from helpers.set_env_vars import (
    DAILY_ZARR, MONTHLY_TIMESERIES_ZARR, MONTHLY_TIMESTEPS_ZARR, HYDROSOS_DIR,
    HYDROSOS_ID_PAIRS, HYDROSOS_THRESHOLDS, HYDROSOS_BASINS,
    WEBHOOK_LOG_SILENT,
)


def update_monthly_products(cl: CloudLog) -> None:
    # check which months are on the monthly datasets
    with xr.open_zarr(MONTHLY_TIMESTEPS_ZARR) as ds:
        months_calculated = set(ds.time.dt.strftime('%Y-%m-01').values)

    # check which months are in the local daily zarr
    daily_zarr = xr.open_zarr(DAILY_ZARR)
    dates_available = pd.to_datetime(daily_zarr.time.values)
    months_available = set(dates_available.strftime('%Y-%m-01'))

    # check if the last month in the daily zarr has the last day of the month available
    if dates_available[-1].day != dates_available[-1].days_in_month:
        months_available.remove(dates_available[-1].strftime('%Y-%m-01'))

    # determine which months are missing and should be computed
    months_to_compute = set(months_available) - set(months_calculated)
    months_to_compute = natsorted(list(months_to_compute))
    if not months_to_compute:
        cl.add_message("No months to compute, all monthly products are up to date.")
        return
    cl.add_message(f"Months to compute: {months_to_compute}")

    # filter the dataset to only the months that need to be computed, average them, and append to the monthly zarrs
    daily_zarr = (
        daily_zarr
        .isel(time=dates_available.strftime('%Y-%m-01').isin(months_to_compute))
        .resample(time='MS')  # MS -> Month Start
        .mean()
    )
    # remove all the chunks encoding
    daily_zarr.encoding.pop('chunks', None)
    daily_zarr.Q.encoding.pop('chunks', None)
    cl.add_message('Appending to monthly timeseries')
    (
        daily_zarr
        .chunk({'time': 1020, 'river_id': 250})
        .to_zarr(MONTHLY_TIMESERIES_ZARR, mode='a', append_dim='time', consolidated=True, zarr_format=2)
    )
    cl.add_message('Appending to monthly timesteps')
    (
        daily_zarr
        .chunk({'time': 1, 'river_id': 2_500_000})
        .to_zarr(MONTHLY_TIMESTEPS_ZARR, mode='a', append_dim='time', consolidated=True, zarr_format=2)
    )

    # for each month in months_to_compute, also make a hydrosos geotiff
    cl.add_message('Preparing HydroSOS COGs')
    id_pairs = pd.read_parquet(HYDROSOS_ID_PAIRS)
    thresholds = pd.read_parquet(HYDROSOS_THRESHOLDS)
    basins = gpd.read_parquet(HYDROSOS_BASINS)
    hydrosos_color_map = {
        1: '#cd233f',
        2: '#ffa885',
        3: '#e7e2bc',
        4: '#8eceee',
        5: '#2c7dcd',
    }
    color_to_rgb = lambda hex_color: tuple(int(hex_color[i:i + 2], 16) for i in (1, 3, 5))
    hydrosos_rgb_map = {k: color_to_rgb(v) for k, v in hydrosos_color_map.items()}
    with xr.open_zarr(MONTHLY_TIMESTEPS_ZARR) as ds:
        discharges = (
            ds
            .sel(river_id=id_pairs['LINKNO'].unique(), time=months_to_compute)
            ['Q']
            .to_dataframe()
            .reset_index()
            .pivot(columns='time', index='river_id', values='Q')
            .reset_index()
            .merge(id_pairs, how='inner', left_on='river_id', right_on='LINKNO')
            .drop(columns=['river_id', 'LINKNO'])
            .groupby('HYBAS_ID')
            .sum()
        )
        discharges.columns = pd.to_datetime(discharges.columns).strftime('%Y-%m-01')
    # convert the columns with dates to classes
    for month in months_to_compute:
        # select the rows where the month part of the multiindex is equal to the month we are iterating on
        discharges = discharges.merge(
            thresholds.xs(key=int(month.split('-')[1]), level='month'),
            left_index=True, right_index=True, how='inner',
        )
        discharges['class'] = 1
        discharges.loc[discharges[month] >= discharges['0.1'], 'class'] = 2
        discharges.loc[discharges[month] >= discharges['0.25'], 'class'] = 3
        discharges.loc[discharges[month] >= discharges['0.75'], 'class'] = 4
        discharges.loc[discharges[month] >= discharges['0.9'], 'class'] = 5
        discharges[month] = discharges['class']
        discharges = discharges.drop(columns=['class', '0.1', '0.25', '0.75', '0.9'])

    # create a cloud optimized geotiff for all the basin polygons
    basins = basins.merge(discharges, on='HYBAS_ID', how='inner')
    for month in months_to_compute:
        output_path = os.path.join(HYDROSOS_DIR, f'{month[:-3]}.tif')
        resolution = 0.05
        shape = (int(180 / resolution), int(360 / resolution))
        transform = rasterio.transform.from_origin(-180, 90, resolution, resolution)
        dtype = 'uint8'
        write_kwargs = {
            'driver': 'GTiff',
            'height': shape[0],
            'width': shape[1],
            'count': 3,
            'dtype': dtype,
            'crs': basins.crs,
            'transform': transform,
            'tiled': True,
            'blockxsize': 512,
            'blockysize': 512,
            'compress': 'deflate',
            'interleave': 'band',
            'nodata': 0,
        }
        with rasterio.open(output_path, 'w', **write_kwargs) as dst:
            # get the red, green, blue colors by converting hex characters to integers
            for band in (1, 2, 3):  # for band in r, g, b
                colors = basins[month].map(lambda x: hydrosos_rgb_map[x][band - 1]).astype(np.uint8)
                dst.write(
                    rasterio.features.rasterize(
                        [(geom, value) for geom, value in zip(basins.geometry, colors)],
                        out_shape=shape,
                        transform=transform,
                        dtype=dtype,
                        fill=0,
                        all_touched=True,
                    ),
                    band
                )


if __name__ == '__main__':
    cl = CloudLog(WEBHOOK_LOG_SILENT, step='monthly products')
    try:
        update_monthly_products(cl)
        exit(0)
    except Exception as e:
        cl.add_message(str(e))
        cl.add_message(traceback.format_exc())
        exit(1)
    finally:
        cl.flush()
