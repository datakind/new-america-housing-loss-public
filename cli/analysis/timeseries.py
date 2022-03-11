import datetime
import typing as T

import pandas as pd
from matplotlib import collections
from matplotlib import pyplot as plt

from cli.loggy import log_machine


@log_machine
def create_timeseries(
    input_df: pd.DataFrame, date_column: str, series_label: str
) -> T.Union[T.Tuple[collections.PathCollection, str], T.Tuple[None, None]]:
    """Plot time series counts of the different data types (eviction, foreclosure, etc.)."""

    # Check for empty input
    if input_df is None:
        return None

    input_df[date_column] = pd.to_datetime(
        input_df[date_column], infer_datetime_format=True
    )

    # Get counts of housing loss by day and turn into a sorted df
    agg_by_date_series = input_df[date_column].value_counts()
    agg_by_date_df = agg_by_date_series.to_frame(name='counts')
    agg_by_date_df.index = agg_by_date_df.index.set_names(['dates'])
    agg_by_date_df = agg_by_date_df.sort_index()
    agg_by_date_df = agg_by_date_df.reset_index()

    # Extract into a new column the month and year of each date
    agg_by_date_df['YearMonth'] = pd.to_datetime(agg_by_date_df['dates']).dt.to_period(
        'M'
    )

    # Aggregate (groupby) this new month variable and form a df for visualization
    by_month_df = agg_by_date_df.groupby('YearMonth')
    results_ser = by_month_df['counts'].sum()
    results_df = results_ser.to_frame(name='mysum')
    results_df.index = results_df.index.astype('datetime64[ns]')

    # Fill in zero counts when data not present in given month
    first_month = results_df.index[0]
    last_month = results_df.index[-1]

    my_months = (
        pd.date_range(first_month, last_month, freq='MS').strftime("%Y-%m-%d").tolist()
    )

    fill_df = pd.DataFrame({'YearMonth': my_months})
    fill_df.set_index('YearMonth', inplace=True)
    fill_df.index = fill_df.index.astype('datetime64[ns]')
    results_df = results_df.merge(
        fill_df, left_index=True, right_index=True, how='right'
    )
    results_df.fillna(0, inplace=True)

    # fig = plt.figure()
    # fig.set_size_inches(9, 6.5)
    s = plt.scatter(results_df.index, results_df.mysum, s=100, label=series_label)
    plt.plot(results_df.index, results_df.mysum)

    # plot_title = series_label + ' timeseries'
    # fig_name = series_label + '_timeseries.png'

    plt.title('housing loss timeseries by type', size=30)
    plt.xlabel('date', size=30)
    plt.ylabel('monthly counts', size=30)
    plt.xticks(size=20)
    plt.yticks(size=30)
    plt.grid(True)

    return s
