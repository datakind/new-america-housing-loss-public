import datetime
import math
import typing as T
from functools import reduce

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from dateutil.relativedelta import *

from loggy import log_machine

# need below to suppress warnings associated with fake geoid code block - unnecessary for production code
pd.options.mode.chained_assignment = None  # default='warn'


@log_machine
def summarize_housing_loss(
    data_df: pd.DataFrame, pop_df: pd.DataFrame, type: str
) -> T.Union[pd.DataFrame, None]:
    """Summarize housing loss data from various geocoded dataframes."""
    # Check for empty inputs
    if data_df is None:
        return None
    if pop_df is None:
        print('\u2326  No ACS data available to calculate housing loss rates!')
        return None

    # read in test data (this has same format as raw data incoming from partner and assumes the format of the delivered dates
    # can be successfully inferred by datetime
    if type == 'evic':
        data_df['eviction_filing_date'] = pd.to_datetime(
            data_df['eviction_filing_date'], infer_datetime_format=True
        )
        # need a better way to check for judgment dates
        try:
            data_df['eviction_judgment_Date'] = pd.to_datetime(
                data_df['eviction_judgment_Date'], infer_datetime_format=True
            )
        except:
            print('no judgment data')

    num_geoid = len(data_df.geoid.unique())

    # dataframes for individual geoids to pull housing loss counts from
    df_by_geoid = {}

    geoid_ser = data_df.geoid.unique()

    geoid_df = pd.DataFrame({'geoid': geoid_ser})
    geoid_df = geoid_df.merge(pop_df, left_on='geoid', right_on='GEOID', how='left')

    hhs_by_geoid = geoid_df.households_by_geoid

    nyrs_ev = 0

    # get year range present in the data
    if type == 'evic':
        frst_yr_ev = min(data_df['eviction_filing_date']).year
        last_yr_ev = max(data_df['eviction_filing_date']).year

        yrs_ev = list(range(frst_yr_ev, last_yr_ev + 1))
        nyrs_ev = len(yrs_ev)

        # matrix that will store housing loss counts by year and geoid
        arr_ev = [[0 for x in range(num_geoid)] for y in range(nyrs_ev)]

        # for total counts across all years
        all_ev = {}

        if 'eviction_judgment_date' in data_df.columns:
            frst_yr_jd = min(data_df['Eviction_Judgment_Date']).year
            last_yr_jd = max(data_df['Eviction_Judgment_Date']).year

            yrs_jd = list(range(frst_yr_jd, last_yr_jd + 1))
            nyrs_jd = len(yrs_jd)

            arr_jd = [[0 for x in range(num_geoid)] for y in range(nyrs_jd)]

            all_jd = {}

        for i in range(0, num_geoid):

            geoid = geoid_ser[i]

            df_by_geoid[i] = data_df.copy()
            df_by_geoid[i] = df_by_geoid[i][df_by_geoid[i]['geoid'] == geoid]

            len_ct_ev = len(
                df_by_geoid[i]['eviction_filing_date']
                .groupby(df_by_geoid[i]['eviction_filing_date'].dt.year)
                .agg('count')
            )

            all_ev[i] = 0

            for j in range(0, len_ct_ev):
                year = (
                    df_by_geoid[i]['eviction_filing_date']
                    .groupby(df_by_geoid[i]['eviction_filing_date'].dt.year)
                    .agg('count')
                    .index[j]
                )
                pos = year - frst_yr_ev

                ev_ct = (
                    df_by_geoid[i]['eviction_filing_date']
                    .groupby(df_by_geoid[i]['eviction_filing_date'].dt.year)
                    .agg('count')
                    .array[j]
                )

                arr_ev[pos][i] = ev_ct
                all_ev[i] += ev_ct

            if 'eviction_judgment_date' in data_df.columns:

                all_jd[i] = 0

                len_ct_jd = len(
                    df_by_geoid[i]['Eviction_Judgment_Date']
                    .groupby(df_by_geoid[i]['Eviction_Judgment_Date'].dt.year)
                    .agg('count')
                )
                for j in range(0, len_ct_jd):
                    year = (
                        df_by_geoid[i]['Eviction_Judgment_Date']
                        .groupby(df_by_geoid[i]['Eviction_Judgment_Date'].dt.year)
                        .agg('count')
                        .index[j]
                    )
                    pos = year - frst_yr_jd

                    jd_ct = (
                        df_by_geoid[i]['Eviction_Judgment_Date']
                        .groupby(df_by_geoid[i]['Eviction_Judgment_Date'].dt.year)
                        .agg('count')
                        .array[j]
                    )

                    arr_jd[pos][i] = jd_ct
                    all_jd[i] += jd_ct

        # make output summary table column headings by year present in the data
        yrs_str_ev = [str(int) for int in yrs_ev]

        string = '_eviction_filings'
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
            'total_filings': pd.Series(all_ev),
            'avg_eviction_filing_rate': ev_rate,
        }
        summ_dict.update(tmp_dict)

        if 'eviction_judgment_date' in data_df.columns:

            yrs_str_jd = [str(int) for int in yrs_jd]

            string = '_eviction_judgments'
            col_nms_jd = list(map(lambda orig_string: orig_string + string, yrs_str_jd))

            jd_rate = pd.Series(all_jd) / pd.Series(hhs_by_geoid) / nyrs_jd

            for i in range(0, nyrs_jd):
                tmp_dict = {col_nms_jd[i]: arr_jd[i]}
                summ_dict.update(tmp_dict)
                tmp_dict.clear()

            tmp_dict = {
                'total_judgements': pd.Series(all_jd),
                'avg_eviction_judgment_rate': jd_rate,
            }
            summ_dict.update(tmp_dict)

    else:
        col_name = ''
        if type == 'mort':
            col_name = 'foreclosure_sale_date'
        if type == 'tax':
            col_name = 'tax_lien_sale_date'

        frst_yr = min(data_df[col_name]).year
        last_yr = max(data_df[col_name]).year

        yrs = list(range(frst_yr, last_yr + 1))
        nyrs = len(yrs)

        # matrix that will store housing loss counts by year and geoid
        arr = [[0 for x in range(num_geoid)] for y in range(nyrs)]

        # for total counts across all years
        all_ct = {}

        for i in range(0, num_geoid):

            geoid = geoid_ser[i]

            df_by_geoid[i] = data_df.copy()
            df_by_geoid[i] = df_by_geoid[i][df_by_geoid[i]['geoid'] == geoid]

            len_ct = len(
                df_by_geoid[i][col_name]
                .groupby(df_by_geoid[i][col_name].dt.year)
                .agg('count')
            )

            all_ct[i] = 0

            for j in range(0, len_ct):
                year = (
                    df_by_geoid[i][col_name]
                    .groupby(df_by_geoid[i][col_name].dt.year)
                    .agg('count')
                    .index[j]
                )
                pos = year - frst_yr

                my_ct = (
                    df_by_geoid[i][col_name]
                    .groupby(df_by_geoid[i][col_name].dt.year)
                    .agg('count')
                    .array[j]
                )

                arr[pos][i] = my_ct
                all_ct[i] += my_ct

        # make output summary table column headings by year present in the data
        yrs_str = [str(int) for int in yrs]

        string = ''

        if type == 'mort':
            string = '_mortgage_foreclosures'

        if type == 'tax':
            string = '_tax_liens'

        col_nms = list(map(lambda orig_string: orig_string + string, yrs_str))

        # here I'm calculating N / pop / # years.  so it's a rate averaged over the number of years we have data for.  could also easily calculate a per-year rate.
        rate = pd.Series(all_ct) / pd.Series(hhs_by_geoid) / nyrs

        # build the dictionary containg the lists of geoids, years and corresponding housing loss counts and output it to a csv
        summ_dict = {'geoid': geoid_ser}

        for i in range(0, nyrs):
            tmp_dict = {col_nms[i]: arr[i]}
            summ_dict.update(tmp_dict)
            tmp_dict.clear()

        stringT = ''
        stringR = ''

        if type == 'mort':
            stringT = 'total_mortgage_foreclosures'
            stringR = 'rate_mortgage_foreclosures'

        if type == 'tax':
            stringT = 'total_tax_liens'
            stringR = 'rate_tax_liens'

        tmp_dict = {stringT: pd.Series(all_ct), stringR: rate}
        summ_dict.update(tmp_dict)

    summ_df = pd.DataFrame(summ_dict)
    # Include number of years for eviction data to base housing loss index on
    if type == 'evic':
        summ_df['nyears_evic_data'] = nyrs_ev

    return summ_df
