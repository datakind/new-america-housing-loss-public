import datetime
import math
import typing as T
from functools import reduce

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from dateutil.relativedelta import *

# need below to suppress warnings associated with fake geoid code block - unnecessary for production code
pd.options.mode.chained_assignment = None  # default='warn'


def summarize_housing_loss(
    data_df: pd.DataFrame, pop_df: pd.DataFrame
) -> T.Tuple[T.Union[pd.DataFrame, None], T.Union[int,None]]:
    """Summarize housing loss data from various geocoded dataframes."""
    # Check for empty inputs
    if data_df is None:
        return None, None
    if pop_df is None:
        print('\u2326  No ACS data available to calculate housing loss rates!')
        return None, None

    # read in test data (this has same format as raw data incoming from partner and assumes the format of the delivered dates
    # can be successfully inferred by datetime
    data_df['date'] = pd.to_datetime(
        data_df['date'], infer_datetime_format=True
    )

    summ_all = None

    for loss_type in data_df['data_type'].unique():
        #subset to type
        data_df_type = data_df[data_df['data_type'] == loss_type]

        # get number of unique geoids in the geocoded data
        num_geoid = len(data_df_type.geoid.unique())

        # dataframes for individual geoids to pull housing loss counts from
        df_by_geoid = {}

        # series of unique geoids in the geocoded data
        geoid_ser = data_df_type.geoid.unique()

        #dataframe of unique geoids in the geocoded data
        geoid_df = pd.DataFrame({'geoid': geoid_ser})

        # merge in population data (aka the geoid and 'total-renter-occupied-households' and  'total-owner-occupied-households')
        geoid_df = geoid_df.merge(pop_df, left_on='geoid', right_on='GEOID', how='left')

        hhs_by_geoid = geoid_df['total-renter-occupied-households']

        nyrs_ev = 0

        #get year range present in the data
        frst_yr_ev = min(data_df_type['date']).year
        last_yr_ev = max(data_df_type['date']).year

        yrs_ev = list(range(frst_yr_ev, last_yr_ev + 1))
        nyrs_ev = len(yrs_ev)

        # matrix that will store housing loss counts by year and geoid
        arr_ev = [[0 for x in range(num_geoid)] for y in range(nyrs_ev)]

        # for total counts across all years
        all_ev = {}

        for i in range(0, num_geoid):

            geoid = geoid_ser[i]

            #create a copy of the data and subset to the current geoid 
            df_by_geoid[i] = data_df_type.copy()
            df_by_geoid[i] = df_by_geoid[i][df_by_geoid[i]['geoid'] == geoid]

            #get number of housing loss events by year
            len_ct_ev = len(
                df_by_geoid[i]['date']
                .groupby(df_by_geoid[i]['date'].dt.year)
                .agg('count')
            )

            all_ev[i] = 0

            for j in range(0, len_ct_ev):
                year = (
                    df_by_geoid[i]['date']
                    .groupby(df_by_geoid[i]['date'].dt.year)
                    .agg('count')
                    .index[j]
                )
                pos = year - frst_yr_ev

                ev_ct = (
                    df_by_geoid[i]['date']
                    .groupby(df_by_geoid[i]['date'].dt.year)
                    .agg('count')
                    .array[j]
                )

                arr_ev[pos][i] = ev_ct
                all_ev[i] += ev_ct

        # make output summary table column headings by year present in the data
        yrs_str_ev = [str(int) for int in yrs_ev]

        string = loss_type + '_filings'
        col_nms_ev = list(map(lambda orig_string: orig_string + string, yrs_str_ev))

        # here I'm calculating N / pop / # years.  so it's a rate averaged over the number of years we have data for.  could also easily calculate a per-year rate.
        ev_rate = pd.Series(all_ev) / pd.Series(hhs_by_geoid) / nyrs_ev

        # build the dictionary containg the lists of geoids, years and corresponding housing loss counts and output it to a csv
        summ_dict = {'geoid': geoid_ser}

        for i in range(0, nyrs_ev):
            tmp_dict = {col_nms_ev[i]: arr_ev[i]}
            summ_dict.update(tmp_dict)
            tmp_dict.clear()

        tmp_dict = {
            loss_type + 'filings_all_years': pd.Series(all_ev),
            loss_type + 'filings_all_years': ev_rate,
        }
        summ_dict.update(tmp_dict)
    
        summ_df = pd.DataFrame(summ_dict)

        #merge to the summary dataframe
        if summ_all is None:
            summ_all = summ_df
        else:
            summ_all = summ_all.merge(summ_df, on='geoid', how='outer')

    summ_all['total_housing_loss'] = summ_all['evictionsfilings_all_years']

    return summ_all, nyrs_ev
