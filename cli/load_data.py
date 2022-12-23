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
    validate_address_data,
    verify_input_directory,
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


def load_data(sub_directories: T.List, data_category) -> T.Tuple[pd.DataFrame,pd.DataFrame]:
    """Load evictions data from csv template
    Inputs
    ------
    sub_directories: list of sub-directories
    data_category: 'evictions', 'mortgage_foreclosures', 'tax_lien_foreclosures'
    parameters: If necessary, parameters to determine narrow down timeframe
      or columns of evictions data to return
    Outputs
    -------
    cleaned_df: Processed pandas dataframe for next step of geo matching
    duplicate_nan_df: Addresses that are dropped for being duplicates or nan for record keeping
    """
    for data_dir in sub_directories:
        # If this sub directory does not match the data_category, skip it:
        if data_category not in str(data_dir):
            continue
        # If this is right subdirectory, list the files in the directory
        data_files = os.listdir(data_dir)
        # Alert user if there are no files in the relevant subdirectory
        if len(data_files) == 0:
            print('\n\u2326', 'Empty sub directory ', data_dir, ' - nothing to process')
            return None, None
        else:
            print(
                '\nSubdirectory of ',
                data_dir,
                ' has ',
                len(data_files),
                ' files in it: ',
                data_files,
            )
        data = pd.DataFrame()
        # Loop through the files
        for f in data_files:
            print('Loading file: ', f, ' of ', data_files)
            # Read in file depending on file format
            if str(f.lower()).startswith(data_category):
                if str(f.lower()).endswith('.csv'):
                    print(u'\u2713', 'File type: .csv')
                    df = pd.read_csv(data_dir / f, low_memory=False)
                    print('First row of data:\n', df.iloc[0, :])
                elif str(f.lower()).endswith(('.xls', '.xlsx')):
                    print(u'\u2713', 'File type: .xls or .xlsx')
                    df = pd.read_excel(data_dir / f)
                    print(u'\u2713', 'First row of data:\n', df.iloc[0, :])
                else:
                    print(f'Invalid file detected {str(f)}')
            else:
                # Let user know about invalid files
                print(
                    u'\u2326',
                    'A file that was not labeled as ',
                    data_category,
                    ' was found and will be ignored.',
                )
                print(
                    u'\u2326',
                    'Please name each type of file according to the guidelines.',
                )
                continue
            # Join multiple files together if there are multple, otherwise move on
            if len(df) == 0:
                data = df
            else:
                data = pd.concat([data, df], ignore_index=True)
        # No files with readable extensions
        if len(data) == 0:
            print(
                u'\u2326',
                'No readable files found in sub-directory!',
                'Please make sure input file is CSV.',
            )
            return None, None
        else:
            print(
                u'\u2713',
                'You have at least one data file to process of type: ',
                data_category,
            )
        print('\nProcessing duplicate and empty rows:')
        # Drop FULL duplicates
        rows = data.shape[0]
        print('You are starting with ', rows, ' rows in your data set.')
        # Convert columns names to lowercase and remove any special characters
        data.columns = [
            remove_special_chars(col.replace(' ', "_").lower().strip())
            for col in data.columns
        ]
        if ('street_address_1' in data.columns and 'city' in data.columns and 'state' in data.columns and 'zip_code' in data.columns):
            df_dups_na = data[data.duplicated or data.isna()]
            df_dups_na['errors'] = 'Duplicate or NA'
            df_dups_out = df_dups_na[['street_address_1', 'city', 'state', 'zip_code', 'errors']]
        else:
            print(
                'You are missing one of the following required column: street_address_1, city, state, or zip_code.'
            )
            return None, None
        data = data.drop_duplicates().dropna(how="all", axis=0)
        print(
            u'\u2326',
            'Dropping duplicates and null rows removed ',
            round(abs(100 * (data.shape[0] - rows) / rows), 1),
            '% of your rows.',
        )

        print('\nProcessing Date Columns:')
        if data_category == 'evictions':
            date_column = 'eviction_filing_date'
        elif data_category == 'mortgage_foreclosures':
            date_column = 'foreclosure_sale_date'
        elif data_category == 'tax_lien_foreclosures':
            date_column = 'tax_lien_sale_date'
        # Check to see if they have the correct date column
        # but otherwise use the first 'date' column found:
        has_date_column = date_column in data.columns
        if not has_date_column:
            print(u'\u2326', 'Expected date column is missing in the input file')
            date_columns_found = [
                item for item in data.columns if 'date' in item.lower()
            ]
            if len(date_columns_found) > 0:
                print(
                    u'\u2713',
                    'Process will use ',
                    date_columns_found[0],
                    ' as the ',
                    date_column,
                )
                data[date_column] = data[date_columns_found[0]].copy()
            else:
                print(
                    u'\u2326',
                    'Date column not found, please adjust your column headers.',
                )
                return None, None
        else:
            print(f'\u2713  Process will use {date_column}.')
        # If date column is null, tell user to fix the file

        if data[date_column].isnull().all():
            print(
                u'\u2326',
                'Date column is empty - please ensure date data is included.',
            )
            return None, None

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

    print(
        u'\u2326',
        'No data found in correct format to process. ',
        'Please review FLH Partner Site Data Collection Template!',
    )
    return None, None


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
    # LOOK FOR CORRECT SUBDIRECTORY STRUCTURE
    sub_directories = verify_input_directory(input_path)
    # If the input_directory fails, the main function should abort:
    if sub_directories is None:
        return "The path provided does not have the expected subdirectory structure."

    # LOAD ALL 3 TYPES OF DATA (AS AVAILABLE)
    df_evic, df_evic_dups = load_data(sub_directories, 'evictions')
    df_mort, df_mort_dups = load_data(sub_directories, 'mortgage_foreclosures')
    df_tax, df_tax_dups = load_data(sub_directories, 'tax_lien_foreclosures')

    if (df_evic is None) and (df_mort is None) and (df_tax is None):
        print(
            'No data files matched our requirements for this analysis.'
            'Please check the files and restart.'
        )
        return None

    # STANDARDIZE THE INPUT DATA ADDRESSES
    df_evic_standardized, df_evic_parse_err, evic_avail_cols = standardize_input_addresses(
        df_evic, 'eviction'
    )
    df_mort_standardized, df_mort_parse_err, mort_avail_cols = standardize_input_addresses(
        df_mort, 'foreclosure'
    )
    df_tax_standardized, df_tax_parse_err, tax_avail_cols = standardize_input_addresses(
        df_tax, 'tax lien'
    )



    # CREATE TIME SERIES PLOTS
    plt.rcParams['figure.figsize'] = [25, 10]
    fig1 = create_timeseries(df_evic_standardized, 'eviction_filing_date', 'Evictions')
    fig2 = create_timeseries(
        df_mort_standardized, 'foreclosure_sale_date', 'Foreclosures'
    )
    fig3 = create_timeseries(df_tax_standardized, 'tax_lien_sale_date', 'Tax_Liens')
    # Create the directories to output the plots to
    plot_write_path = Path(input_path).parent / OUTPUT_PATH_PLOTS
    plot_write_path.mkdir(parents=True, exist_ok=True)
    # Save the plots to this directory
    plt.legend(prop={'size': 20})
    plt.savefig(str(plot_write_path / HOUSING_LOSS_TIMESERIES_FILENAME))
    print(
        '*** Created housing loss timeseries image '
        + str(plot_write_path / HOUSING_LOSS_TIMESERIES_FILENAME)
    )

    # GEOCODE THE CLEANED/STANDARDIZED DATA AND WRITE GEOCODED DATASETS TO DISK
    # Create the directories to output the geocoder cache to
    geocoder_cache_write_path = Path(input_path).parent / OUTPUT_PATH_GEOCODER_CACHE
    geocoder_cache_write_path.mkdir(parents=True, exist_ok=True)

    df_evic_geocoded_final = None
    df_mort_geocoded_final = None
    if df_evic_standardized is not None:
        df_evic_geocoded_final = geocode_input_data(
            df_evic_standardized, evic_avail_cols, 'eviction', geocoder_cache_write_path
        )
    if df_mort_standardized is not None:
        df_mort_geocoded_final = geocode_input_data(
            df_mort_standardized,
            mort_avail_cols,
            'foreclosure',
            geocoder_cache_write_path,
        )
    df_tax_geocoded_final = geocode_input_data(
        df_tax_standardized, tax_avail_cols, 'tax lien', geocoder_cache_write_path
    )

    # Create the directories to output the raw geocoded datasets to
    geocoded_file_write_path = Path(input_path).parent / OUTPUT_PATH_GEOCODED_DATA
    geocoded_file_write_path.mkdir(parents=True, exist_ok=True)

    write_df_to_disk(
        df_evic_geocoded_final, geocoded_file_write_path / GEOCODED_EVICTIONS_FILENAME
    )
    write_df_to_disk(
        df_mort_geocoded_final,
        geocoded_file_write_path / GEOCODED_FORECLOSURES_FILENAME,
    )
    write_df_to_disk(
        df_tax_geocoded_final, geocoded_file_write_path / GEOCODED_TAX_LIENS_FILENAME
    )

    # Get the most likely state/county FIPS codes and city from geocoded data
    state_fips, county_fips, city_str, state_str = find_state_county_city(
        df_evic_geocoded_final
    )
    if state_fips is None or county_fips is None:
        # If we can't find FIPS codes from evictions data, try it from the mortgage foreclosure data
        state_fips, county_fips, city_str_2, state_str_2 = find_state_county_city(
            df_mort_geocoded_final
        )
    #Get the exceptions from geocoded data
    df_evic_errors = None
    df_mort_errors = None
    df_tax_errors = None
    if df_evic_geocoded_final is not None:
        df_evic_errors = df_evic_geocoded_final[df_evic_geocoded_final['is_match'] == 'No_Match'][['street_address_1', 'city', 'state', 'zip_code', 'is_match']]
        df_evic_errors.rename(columns = {'is_match': 'errors'}, inplace = True)
    if df_mort_geocoded_final is not None:
        df_mort_errors = df_mort_geocoded_final[df_mort_geocoded_final['is_match'] == 'No_Match'][['street_address_1', 'city', 'state', 'zip_code', 'is_match']]
        df_mort_errors.rename(columns = {'is_match': 'errors'}, inplace = True)
    if df_tax_geocoded_final is not None:
        df_tax_errors = df_tax_geocoded_final[df_tax_geocoded_final['is_match'] == 'No_Match'][['street_address_1', 'city', 'state', 'zip_code', 'is_match']]
        df_tax_errors.rename(columns = {'is_match': 'errors'}, inplace = True)

    # GRAB ACS DATA; used in housing loss summary and demographic correlation search
    print("\nPreparing to get ACS data...")
    acs_df, acs_data_dict = get_acs_data(state_fips, county_fips, ACS_YEAR)
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
    renter_hhs = acs_df[['GEOID', 'total-renter-occupied-households']].copy()
    owner_hhs = acs_df[['GEOID', 'total-owner-occupied-households']].copy()
    # Standardize column name
    renter_hhs.rename(
        columns={'total-renter-occupied-households': 'households_by_geoid'},
        inplace=True,
    )
    owner_hhs.rename(
        columns={'total-owner-occupied-households': 'households_by_geoid'}, inplace=True
    )

    # CREATE HOUSING LOSS SUMMARIES
    evic_summ = summarize_housing_loss(df_evic_geocoded_final, renter_hhs, 'evic')
    write_df_to_disk(evic_summ, summary_write_path / 'evic_summ_test.csv')
    mort_summ = summarize_housing_loss(df_mort_geocoded_final, owner_hhs, 'mort')
    tax_summ = summarize_housing_loss(df_tax_geocoded_final, owner_hhs, 'tax')

    # Stack the data together for summarization while counting all housing loss events for the housing loss index calculation
    coll_dfs = []
    msg = ' with data from'
    if evic_summ is not None:
        coll_dfs.append(evic_summ)
        msg += ' evictions;'
    if mort_summ is not None:
        coll_dfs.append(mort_summ)
        msg += ' mortgage foreclosures;'
    if tax_summ is not None:
        coll_dfs.append(tax_summ)
        msg += ' tax liens'

    # Create the summary dataframe, add the ACS variables and get all housing loss events
    df_summ_mrg = reduce(
        lambda left, right: pd.merge(left, right, on='geoid'), coll_dfs
    )
    df_summ_mrg = pd.merge(df_summ_mrg, acs_df, left_on='geoid', right_on='GEOID')

    if mort_summ is not None:
        if tax_summ is not None:
            df_summ_mrg['total_housing_loss'] = (
                df_summ_mrg['total_filings']
                + df_summ_mrg['total_mortgage_foreclosures']
                + df_summ_mrg['total_tax_liens']
            )
            df_summ_mrg['total_foreclosures'] = (
                df_summ_mrg['total_mortgage_foreclosures']
                + df_summ_mrg['total_tax_liens']
            )
        else:
            df_summ_mrg['total_housing_loss'] = (
                df_summ_mrg['total_filings']
                + df_summ_mrg['total_mortgage_foreclosures']
            )
            df_summ_mrg['total_foreclosures'] = df_summ_mrg[
                'total_mortgage_foreclosures'
            ]
    else:
        if tax_summ is not None:
            df_summ_mrg['total_housing_loss'] = (
                df_summ_mrg['total_filings'] + df_summ_mrg['total_tax_liens']
            )
            df_summ_mrg['total_foreclosures'] = df_summ_mrg['total_tax_liens']
        else:
            df_summ_mrg['total_housing_loss'] = df_summ_mrg['total_filings']

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
        '*** Created ' + str(summary_write_path / HOUSING_LOSS_SUMMARY_FILENAME) + msg
    )

    #Create summary of the errors and output to file
    if (df_evic_parse_err is not None or df_evic_errors is not None or df_evic_dups is not None):
        df_evic_errors = pd.concat([df_evic_parse_err, df_evic_errors, df_evic_dups])
        write_df_to_disk(
            df_evic_errors, summary_write_path / EVIC_ADDRESS_ERR_FILENAME
        )
    if (df_mort_parse_err is not None or df_mort_errors is not None or df_mort_dups is not None):
        df_mort_errors = pd.concat([df_mort_parse_err,df_mort_errors, df_mort_dups])
        write_df_to_disk(
            df_mort_errors, summary_write_path / MORT_ADDRESS_ERR_FILENAME
        )
    if (df_tax_parse_err is not None or df_tax_errors is not None or df_tax_dups is not None):
        df_tax_errors = pd.concat([df_tax_parse_err, df_tax_errors, df_tax_dups])
        write_df_to_disk(
            df_tax_errors, summary_write_path / TAX_ADDRESS_ERR_FILENAME
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
