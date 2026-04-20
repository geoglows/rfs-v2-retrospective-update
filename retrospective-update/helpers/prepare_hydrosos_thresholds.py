import pandas as pd
import xarray as xr

start_date = '1990-01-01'
end_date = '2019-12-31'
id_pairs = pd.read_parquet('./hybas_linkno_pairs.parquet')

quantiles = [0.1, 0.25, 0.75, 0.9]
df = (
    xr
    .open_zarr('./monthly-timeseries.zarr')
    .sel(time=slice(start_date, end_date), river_id=id_pairs['LINKNO'].values)
    .assign_coords(HYBAS_ID=('river_id', id_pairs['HYBAS_ID'].values))
    .groupby('HYBAS_ID')
    .sum()
    .groupby('time.month')
    .quantile(quantiles)
    .to_dataframe()
    .reset_index()
    .pivot(columns='quantile', index=['month', 'HYBAS_ID'], values='Q')
)

df.columns = df.columns.astype(str)
quantiles = ['0.1', '0.25', '0.75', '0.9']

for index, quantile in enumerate(quantiles[:-1]):
    q1 = quantile
    q2 = quantiles[index + 1]

    if (df[q1] < df[q2]).all():
        continue

    mask = df[q1] >= df[q2]
    df.loc[mask, q2] = df.loc[mask, q1] + 0.001

    # Validate after fix
    if (df[q1] >= df[q2]).any():
        print(f'Quantile {q1} is still invalid after adjustment')

df.to_parquet('./thresholds.parquet')
