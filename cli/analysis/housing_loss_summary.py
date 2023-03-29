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
) -> T.Union[pd.DataFrame, None]:
    """Summarize housing loss data from various geocoded dataframes."""
    # Check for empty inputs
    if data_df is None:
        return None
    if pop_df is None:
        print('\u2326  No ACS data available to calculate housing loss rates!')
        return None
    
    #Create dataframe of unique geoids in the geocoded data, along with the sum of the total-renter-occupied-households and total-owner-occupied-households, and count of records by data_type, year and geoid
    data_df['date'] = pd.to_datetime(
        data_df['date'], infer_datetime_format=True
    )
    data_df['year'] = data_df['date'].dt.year
    #create a column for data_type and year for the pivot table below
    data_df['data_type_year']  = data_df['data_type'] + '_' + data_df['year'].astype(str)
    data_df['value'] = 1

    #get counts by geoid and keep only value and rename to total
    geoid_df = data_df.groupby('geoid').count()[['value']].rename(columns={'value':'total'}).reset_index()

    #pivot table to get count by geoid and type
    geoid_df_type = pd.pivot_table(data_df, index=['geoid'], columns='data_type', values = 'value', aggfunc = 'size', fill_value=0).reset_index()

    #pivot table to get count of records by geoid, data_type and year
    geoid_df_type_year = pd.pivot_table(data_df, index=['geoid'], columns='data_type_year', values = 'value', aggfunc = 'size', fill_value=0).reset_index()

    #merge the tables
    geoid_df = geoid_df.merge(geoid_df_type, on='geoid')
    geoid_df = geoid_df.merge(geoid_df_type_year, on='geoid')

    #Add in the number of years of data for each data_type
    #create a dictonary with each data_type and a subdictionary for each with the min year and max year
    geoid_df_min_max_year = pd.pivot_table(data_df, index=['data_type'], values = 'year', aggfunc = [min, max])
    #range of years
    geoid_df_min_max_year['range'] = geoid_df_min_max_year['max'] - geoid_df_min_max_year['min'] + 1
    #convert to dictionary 
    geoid_df_min_max_year = geoid_df_min_max_year.to_dict('index')

    #add a column for each data_type with the range of years
    for data_type in geoid_df_min_max_year.keys():
        geoid_df[data_type + '_nyears'] = geoid_df_min_max_year[data_type][('range','')]

    #overall range of years
    geoid_df['nyears'] = data_df['year'].max() - data_df['year'].min() + 1

    #merge in population data (aka the geoid and 'total-renter-occupied-households' and  'total-owner-occupied-households')
    geoid_df = geoid_df.merge(pop_df, left_on = 'geoid', right_on = 'GEOID')

    # here I'm calculating N / pop / # years.  so it's a rate averaged over the number of years we have data for.  could also easily calculate a per-year rate.
    for data_type in data_df['data_type'].unique():
        #if type includes the string 'eviction' then it is a renter type
        if 'eviction' in data_type:
            geoid_df[data_type + '_rate'] = geoid_df[data_type] / geoid_df['total-renter-occupied-households'] / geoid_df[data_type + '_nyears']
        else:
            geoid_df[data_type + '_rate'] = geoid_df[data_type] / geoid_df['total-owner-occupied-households'] / geoid_df[data_type + '_nyears']

    # Calculate the housing loss index - note this assumes complete records of evictions and foreclosures for all years present in eviction data
        geoid_df[data_type + 'index'] = (
            geoid_df[data_type]
            / (
                geoid_df['total-renter-occupied-households']
                + geoid_df['total-owner-occupied-households']
            )
            / geoid_df[data_type + '_nyears']
        )
    #Total Index
    geoid_df['total_index'] = (
        geoid_df['total']
        / (
            geoid_df['total-renter-occupied-households']
            + geoid_df['total-owner-occupied-households']
        )
        / geoid_df['nyears']
    )

    return geoid_df
