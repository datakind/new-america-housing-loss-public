"""
Created on Sat Sep 18 23:11:22 2021

@author: datakind
"""
import logging
import os
import sys
import typing as T
from functools import reduce
from pathlib import Path

import pandas as pd
import requests
from matplotlib import collections
from matplotlib import pyplot as plt

from analysis.acs_correlation import correlation_analysis
from analysis.acs_data import get_acs_data
from analysis.housing_loss_summary import summarize_housing_loss
from analysis.timeseries import create_timeseries
from collection.address_cleaning import remove_special_chars
from collection.address_geocoding import find_state_county_city, geocode_input_data
from collection.address_validation import (
    standardize_input_addresses,
    validate_address_data
)
from collection.tigerweb_api import (
    create_tigerweb_query,
    get_input_data_geometry,
    jprint,
    rename_baseline,
)
from const import (
    ACS_DATA_DICT_FILENAME,
    ACS_YEAR,
    GEOCODED_EVICTIONS_FILENAME,
    GEOCODED_FORECLOSURES_FILENAME,
    GEOCODED_TAX_LIENS_FILENAME,
    GIS_IMPORT_FILENAME,
    HOUSING_LOSS_SUMMARY_FILENAME,
    HOUSING_LOSS_TIMESERIES_FILENAME,
    MAX_YEAR,
    MIN_YEAR,
    OUTPUT_ALL_HOUSING_LOSS_PLOTS,
    OUTPUT_EVICTION_PLOTS,
    OUTPUT_FORECLOSURE_PLOTS,
    OUTPUT_PATH_GEOCODED_DATA,
    OUTPUT_PATH_GEOCODER_CACHE,
    OUTPUT_PATH_MAPS,
    OUTPUT_PATH_PLOTS,
    OUTPUT_PATH_PLOTS_DETAIL,
    OUTPUT_PATH_SUMMARIES,
    TRACT_BOUNDARY_FILENAME,
    EVIC_ADDRESS_ERR_FILENAME,
    MORT_ADDRESS_ERR_FILENAME,
    TAX_ADDRESS_ERR_FILENAME
)


def load_data(input_path) -> T.Tuple[pd.DataFrame,pd.DataFrame]:
    """Load evictions data from csv template
    Inputs
    ------
    input_path: Path to the input data file

    Outputs
    -------
    cleaned_df: Processed pandas dataframe for next step of geo matching
    duplicate_nan_df: Addresses that are dropped for being duplicates or nan for record keeping
    """

    # Create empty dataframe to hold data
    data = pd.DataFrame()
    print('Loading file: ', input_path)
    # Read in file depending on file format
    if str(input_path.lower()).endswith('.csv'):
        print(u'\u2713', 'File type: .csv')
        df = pd.read_csv(input_path, low_memory=False)
        print('First row of data:\n', df.iloc[0, :])
        data = df
    elif str(input_path.lower()).endswith(('.xls', '.xlsx')):
        print(u'\u2713', 'File type: .xls or .xlsx')
        df = pd.read_excel(input_path)
        print(u'\u2713', 'First row of data:\n', df.iloc[0, :])
        data = df
    else:
        print(f'Invalid file detected {str(f)}')
        return None, None

        
    # No files with readable extensions
    if len(data) == 0:
        print(
            u'\u2326',
            'File is not readable!',
            'Please make sure input file is CSV or xls or xlsx.',
        )
        return None, None
    
    print('\nProcessing duplicate and empty rows:')
    # Drop FULL duplicates
    rows = data.shape[0]
    print('You are starting with ', rows, ' rows in your data set.')
    # Convert columns names to lowercase and remove any special characters
    data.columns = [
        remove_special_chars(col.replace(' ', "_").lower().strip())
        for col in data.columns
    ]
    if ('street_address_1' in data.columns and 'city' in data.columns and 'state' in data.columns and 'zip_code' in data.columns and 'type' in data.columns):
        #select records that na for street_address_1 or all fields as df_dups_na
        df_dups_na = data[data['street_address_1'].isna()]
        df_dups_dups = data[data.duplicated()]
        df_dups_na['errors'] = 'NA'
        df_dups_dups['errors'] = 'Duplicate'
        df_dups_out_na = df_dups_na[['street_address_1','city', 'state', 'zip_code', 'errors', 'type']]
        df_dups_out_dups = df_dups_dups[['street_address_1', 'city', 'state', 'zip_code', 'errors', 'type']]
        df_dups_out = pd.concat([df_dups_out_na, df_dups_out_dups])
    else:
        print(
            'You are missing one of the following required column: street_address_1, city, state, zip_code, or type.'
        )
        return None, None
    data = data.drop_duplicates().dropna(how="all", axis=0)
    if ('street_address_1' in data.columns):
        data = data.dropna(subset=['street_address_1'])
    print(
        u'\u2326',
        'Dropping duplicates and null rows removed ',
        round(abs(100 * (data.shape[0] - rows) / rows), 1),
        '% of your rows.',
    )

    print('\nProcessing Date Column:')
    date_column = 'date'
    # Check to see if they have the correct date column
    has_date_column = date_column in data.columns
    if not has_date_column:
        print(
            u'\u2326',
            'Date column not found, please adjust your column headers.',
        )
        return None, None

    # If date column is null, tell user to fix the file
    if data[date_column].isnull().all():
        print(
            u'\u2326',
            'Date column is empty - please ensure date data is included.',
        )
        return None, None

    # If date column is not a datetime object, try to convert it
    if data[date_column].dtype == object:
        data[date_column] = pd.to_datetime(
            data[date_column], infer_datetime_format=True
        )
    # Deal with null dates
    if data[date_column].isnull().sum() / data.shape[0] > 0.25:
        print(
            'The percent of the date column that is null is: ',
            round(100 * data[date_column].isnull().sum() / data.shape[0], 1),
            '%, which can negatively affect the time-series analysis.',
        )

    data[date_column] = pd.to_datetime(data[date_column])
    data[date_column] = data[date_column].fillna(method='ffill')
    print(f'\nFiltering data to only >= {MIN_YEAR} values:')
    # Create year and month columns for later aggregation
    data['year'] = data[date_column].dt.year.astype(int)
    data['month'] = data[date_column].dt.to_period('M').astype(str)
    print(
        u'\u2713',
        'Data date range is from ',
        data[date_column].dt.date.min(),
        ' to ',
        data[date_column].dt.date.max(),
    )
    # Let user know how much older data will be be discarded
    if data.year.min() < MIN_YEAR:
        print(
            u'\u2326',
            'Data before',
            MIN_YEAR,
            'represents',
            round(
                100
                * data[(data.year < MIN_YEAR) | (data.year > MAX_YEAR)].shape[0]
                / data.shape[0],
                1,
            ),
            '% of data and cannot be used in this analysis.',
        )
    print(u'\u2713', 'Date column has been processed.\n')
    data = data[(data.year >= MIN_YEAR) & (data.year <= MAX_YEAR)]
    print(u'\u2713', 'Data loading complete. Address validation is next.')

    return data, df_dups_out


def write_df_to_disk(input_df: pd.DataFrame, write_path_filename: Path) -> None:
    """Simple helper function to write a dataframe to disk."""
    # Check for empty input
    if input_df is None:
        return None
    if write_path_filename.parent is None or not write_path_filename.parent.exists():
        print(f"Invalid output directory for input dataframe {input_df}")
        return None
    input_df.to_csv(str(write_path_filename), index=False)


def main(input_path: str) -> None:
    """This function is what it says it is. :)

    It takes in the input data path as an argument
    """

    # LOAD the data
    df, df_dups = load_data(input_path)

    if (df is None):
        print(
            'No data files matched our requirements for this analysis.'
            'Please check the file and restart.'
        )
        return None

    # STANDARDIZE THE INPUT DATA ADDRESSES
    df_standardized, df_parse_err, avail_cols = standardize_input_addresses(df)

    # CREATE TIME SERIES PLOTS
    plt.rcParams['figure.figsize'] = [25, 10]
    #for each unique type of housing loss, create a timeseries plot
    for loss_type in df_standardized['type'].unique():
        df_standardized_type = df_standardized[df_standardized.type == loss_type]
        fig = create_timeseries(df_standardized_type, 'date', loss_type)
        # Create the directories to output the plots to
        plot_write_path = Path(input_path).parent / OUTPUT_PATH_PLOTS
        plot_write_path.mkdir(parents=True, exist_ok=True)
        # Save the plots to this directory
        plt.legend(prop={'size': 20})
        SAVE_PATH = loss_type + '_' + HOUSING_LOSS_TIMESERIES_FILENAME
        plt.savefig(str(plot_write_path / SAVE_PATH))
        print(
            '*** Created housing loss timeseries image '
            + str(plot_write_path / SAVE_PATH)
        )

    # GEOCODE THE CLEANED/STANDARDIZED DATA AND WRITE GEOCODED DATASETS TO DISK
    # Create the directories to output the geocoder cache to
    geocoder_cache_write_path = Path(input_path).parent / OUTPUT_PATH_GEOCODER_CACHE
    geocoder_cache_write_path.mkdir(parents=True, exist_ok=True)
    # Create the directories to output the raw geocoded datasets to
    geocoded_file_write_path = Path(input_path).parent / OUTPUT_PATH_GEOCODED_DATA
    geocoded_file_write_path.mkdir(parents=True, exist_ok=True)

    df_geocoded_final = None

    for loss_type in df_standardized.type.unique():
        df_standardized_type = df_standardized[df_standardized.type == loss_type]
        if df_standardized is not None:
            df_geocoded_final_type = geocode_input_data(
                df_standardized_type, avail_cols, loss_type, geocoder_cache_write_path
            )
            df_geocoded_final_type['type'] = loss_type
            #append the geocoded data to the final geocoded dataframe
            df_geocoded_final = pd.concat([df_geocoded_final, df_geocoded_final_type], axis=0)
   
    # Save the full set of geocoded data 
    loss_geocode_path = GEOCODED_EVICTIONS_FILENAME
    write_df_to_disk(
        df_geocoded_final, geocoded_file_write_path / loss_geocode_path
    )

    # Get the most likely state/county FIPS codes and city from geocoded data
    state_fips, county_fips, city_str, state_str = find_state_county_city(
        df_geocoded_final
    )

    # GRAB ACS DATA; used in housing loss summary and demographic correlation search
    print("\nPreparing to get ACS data...")
    acs_df = pd.DataFrame()
    for county in county_fips:
        acs_df_county, acs_data_dict = get_acs_data(state_fips, str(county), ACS_YEAR)
        acs_df = pd.concat([acs_df, acs_df_county], axis=0)
    if acs_df is None:
        print(
            '\u2326  Insufficient geography information to retrieve ACS Data!',
            'Please input valid state and county FIPS codes.',
        )
        return None

    # Create the directories to output the ACS data and summary files to
    summary_write_path = Path(input_path).parent / OUTPUT_PATH_SUMMARIES
    summary_write_path.mkdir(parents=True, exist_ok=True)

    # Write the ACS data dictionary to a file for reference
    if acs_data_dict is not None:
        pd.DataFrame(acs_data_dict).transpose().reset_index().rename(
            columns={"index": "variable"}
        ).to_csv(str(summary_write_path / ACS_DATA_DICT_FILENAME), index=False)
        print(
            '*** Created '
            + str(summary_write_path / ACS_DATA_DICT_FILENAME)
            + ' - inspect for ACS variable definitions and reference'
        )
        

    ### Grab the renter and home owner total count estimates we'll use
    ### later for housing loss rate calculations
    hhs = acs_df[['GEOID', 'total-renter-occupied-households', 'total-owner-occupied-households']].copy()
   
    #Get the exceptions from geocoded data- those that dont merge with the acs data
    df_geo_errors = None

    if df_geocoded_final is not None:
        geoid_noacs = df_geocoded_final.merge(hhs, left_on='geoid', right_on='GEOID', how = 'left', indicator=True)
        geoid_noacs = geoid_noacs[geoid_noacs['_merge'] == 'left_only'][['street_address_1', 'city', 'state', 'zip_code']]
        no_geoid = df_geocoded_final[df_geocoded_final['geoid'].isna()][['street_address_1', 'city', 'state', 'zip_code']]
        no_geoid['errors'] = 'Unable to find a match in the census geocoder'
        df_geo_errors = no_geoid.copy()

    #Here on out is FEAT specific

    # CREATE HOUSING LOSS SUMMARIES
    summ, nyears_evic_data = summarize_housing_loss(df_geocoded_final, hhs)
    summ['nyears_evic_data'] = nyears_evic_data

    # Create the summary dataframe, add the ACS variables and get all housing loss events
    df_summ_mrg = pd.merge(summ, acs_df, left_on='geoid', right_on='GEOID')

    # Calculate the housing loss index - note this assumes complete records of evictions and foreclosures for all years present in eviction data
    df_summ_mrg['housing-loss-index'] = (
        df_summ_mrg['total_housing_loss']
        / (
            df_summ_mrg['total-renter-occupied-households']
            + df_summ_mrg['total-owner-occupied-households']
        )
        / df_summ_mrg['nyears_evic_data']
    )

    # Save the summary file to this directory
    write_df_to_disk(df_summ_mrg, summary_write_path / HOUSING_LOSS_SUMMARY_FILENAME)
    print(
        '*** Created ' + str(summary_write_path / HOUSING_LOSS_SUMMARY_FILENAME)
    )

    #Create summary of the errors and output to file
    if (df_parse_err is not None or df_geo_errors is not None or df_dups is not None):
        df_errors = pd.concat([df_parse_err, df_geo_errors, df_dups])
        write_df_to_disk(
            df_errors, summary_write_path / EVIC_ADDRESS_ERR_FILENAME
        )


    # Prepare subdirectories to store correlation analysis results

    all_housing_loss_write_path = (
        Path(input_path).parent / OUTPUT_ALL_HOUSING_LOSS_PLOTS
    )
    all_housing_loss_write_path.mkdir(parents=True, exist_ok=True)

    all_housing_loss_plot_detail_write_path = (
        Path(input_path).parent
        / OUTPUT_ALL_HOUSING_LOSS_PLOTS
        / OUTPUT_PATH_PLOTS_DETAIL
    )
    all_housing_loss_plot_detail_write_path.mkdir(parents=True, exist_ok=True)

    evictions_write_path = Path(input_path).parent / OUTPUT_EVICTION_PLOTS
    evictions_write_path.mkdir(parents=True, exist_ok=True)

    evictions_plot_detail_write_path = (
        Path(input_path).parent / OUTPUT_EVICTION_PLOTS / OUTPUT_PATH_PLOTS_DETAIL
    )
    evictions_plot_detail_write_path.mkdir(parents=True, exist_ok=True)

    foreclosure_write_path = Path(input_path).parent / OUTPUT_FORECLOSURE_PLOTS
    foreclosure_write_path.mkdir(parents=True, exist_ok=True)

    foreclosure_plot_detail_write_path = (
        Path(input_path).parent / OUTPUT_FORECLOSURE_PLOTS / OUTPUT_PATH_PLOTS_DETAIL
    )
    foreclosure_plot_detail_write_path.mkdir(parents=True, exist_ok=True)

    # # RUN CORRELATION ANALYSIS WITH ACS VARIABLES
    # target_var options:
    # evictions --> 'total_filings'
    # mortgage foreclosures including tax liens --> 'total_foreclosures'
    # all housing loss events --> 'housing-loss-index'
    plt.rcParams['figure.figsize'] = [15, 10]

    try:
        correlation_analysis(
            acs_df, df_summ_mrg, 'housing-loss-index', all_housing_loss_write_path
        )
    except KeyError:
        print('Unable to create correlations for housing-loss-index')

    try:
        correlation_analysis(acs_df, df_summ_mrg, 'total_filings', evictions_write_path)
    except KeyError:
        print('Unable to create correlations for total_filings')

    try:
        correlation_analysis(
            acs_df, df_summ_mrg, 'total_foreclosures', foreclosure_write_path
        )
    except KeyError:
        print('Unable to create correlations for total_foreclosures')

    # Create the directories to output the mapping files to
    mapping_write_path = Path(input_path).parent / OUTPUT_PATH_MAPS
    mapping_write_path.mkdir(parents=True, exist_ok=True)

    # GET GEOMETRY DATA FROM CENSUS TIGERWEB API
    print("\nRetrieving geography data from Census TIGERweb API...")
    geojson_gdf = get_input_data_geometry(
        state_fips, county_fips, str(mapping_write_path / TRACT_BOUNDARY_FILENAME)
    )
    print('*** Created ' + str(mapping_write_path / TRACT_BOUNDARY_FILENAME))
    # Merge the geometry dataframe with the housing + ACS data summary, but avoid
    #   duplicate column names (since they are non-case sensitive in databases)
    # First drop the 'index' column also, since the `censusdata.censusgeo.censusgeo` datatype
    #   throws an error in the .gpkg file creation
    df_summ_mrg.drop(columns='index', inplace=True)
    if 'geoid' in df_summ_mrg.columns and 'GEOID' in df_summ_mrg.columns:
        df_summ_mrg.drop(columns='GEOID', inplace=True)
    elif 'GEOID' in df_summ_mrg.columns:
        df_summ_mrg.rename(columns={'GEOID': 'geoid'}, inplace=True)
    df_summ_mrg['geoid'] = df_summ_mrg['geoid'].astype('str').str.zfill(11)
    merged_gdf = geojson_gdf.merge(df_summ_mrg, how='left', on='geoid')
    merged_gdf.to_file(str(mapping_write_path / GIS_IMPORT_FILENAME), driver='GPKG')
    print('*** Created ' + str(mapping_write_path / GIS_IMPORT_FILENAME))

    # Now that we have got through the entire process, delete the cached geocoded files
    for f in geocoder_cache_write_path.iterdir():
        if f.is_file():
            f.unlink()
    geocoder_cache_write_path.rmdir()

    return None


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s: %(message)s"
    )
    dir_path = sys.argv[1]
    main(dir_path)
