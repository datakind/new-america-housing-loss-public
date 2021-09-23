"""
Created on Sat Sep 18 23:11:22 2021

@author: robotallie
"""
import logging
import os
import typing as T
import sys
from pathlib import Path

import pandas as pd

REQUIRED_SUB_DIRECTORIES = [
    'evictions',
    'mortgage_foreclosures',
    'tax_lien_foreclosures',
]

REQUIRED_ADDRESS_COLUMNS = ['street_address_1', 'city', 'state', 'zip_code']
MIN_YEAR = 2017
MAX_YEAR = 2021


# HELPER FUNCTIONS
def remove_special_chars(text):
    """Remove special characters from text
    Inputs
    ------
    text: a string-valued row of a column or a single text string
    Outputs
    -------
    text: a clean string with no special characters
    """
    for special_chars in [
        '\\',
        '`',
        '*',
        '{',
        '}',
        '[',
        ']',
        '(',
        ')',
        '>',
        '#',
        '^',
        '*',
        '@',
        '!',
        '+',
        '.',
        '%',
        '!',
        '$',
        '&',
        ':',
        '\'',
    ]:
        if special_chars in text:
            text = text.replace(special_chars, "")
    return text


# Requires input_path
def verify_input_directory(input_path: str) -> T.List:
    """Parse the command line input and determine a directory path."""
    directory_contents = [x for x in Path(input_path).iterdir()]
    sub_directories = set(
        [
            str(f).replace(input_path, '').lower()
            for f in directory_contents
            if f.is_dir()
        ]
    )

    if len(directory_contents) == 0:
        print('Directory is empty!')
        return None
    if len(sub_directories) == 0:
        print('No sub-directories present in input directory')
        return None
    if len(sub_directories.intersection(REQUIRED_SUB_DIRECTORIES)) == 0:
        print('Required sub-directories missing from input directory')
        return None
    print('Required sub-directories found in input directory!')
    return [Path(input_path) / sd for sd in sub_directories]


def load_data(sub_directories: T.List, data_category) -> pd.DataFrame:
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
    """
    for data_dir in sub_directories:
        # If this sub directory does not match the data_category, skip it:
        if data_category not in str(data_dir):
            continue
        # If this is right subdirectory, list the files in the directory
        data_files = os.listdir(data_dir)
        # Alert user if there are no files in the relevant subdirectory
        if len(data_files) == 0:
            print(u'\u2326', 'Empty sub directory - nothing to process')
            return None
        else:
            print(
                'Subdirectory of ',
                data_dir,
                ' has ',
                len(data_files),
                ' files in it: ',
                data_files,
            )
        data = pd.DataFrame()
        # Loop through the files
        for f in data_files:
            print('\nLoading file: ', f, ' of ', data_files)
            # Read in file depending on file format
            if str(f.lower()).startswith(data_category):
                if str(f.lower()).endswith('.csv'):
                    print(u'\u2713', 'File type: .csv')
                    df = pd.read_csv(data_dir / f, error_bad_lines=False)
                    print(u'\u2713', 'First row of data:\n', df.iloc[0, :])
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
            return None
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
        data = data.drop_duplicates().dropna(how="all", axis=0)
        print(
            u'\u2326',
            'Dropping duplicates and null rows removed ',
            round(abs(100 * (data.shape[0] - rows) / rows), 1),
            '% of your rows.',
        )
        # Convert columns names to lowercase and remove any special characters
        data.columns = [
            remove_special_chars(col.replace(' ', "_").lower()) for col in data.columns
        ]
        # data.columns = data.columns.str.lower().str.strip().str.replace(' ', "_")

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
            date_columns_found = [item for item in data.columns if 'date' in item]
            if len(date_columns_found) > 0:
                print(
                    u'\u2713',
                    'Process will use ',
                    date_columns_found[0],
                    ' as the ',
                    date_column,
                )
                data[date_column] = data[date_columns_found[0]].values
            else:
                print(
                    u'\u2326',
                    'Date column not found, please adjust your column headers.',
                )
                return None
        else:
            print(u'\u2713', 'Process will use ', date_column, '.')
        # If date column is null, tell user to fix the file

        if data[date_column].isnull().all():
            print(
                u'\u2326',
                'Date column is empty - please ensure date data is included.',
            )
            return None
        # If date column exists,
        # remove any special characters and change to datetime object
        data[date_column] = data[date_column].apply(remove_special_chars)
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

        data[date_column] = pd.to_datetime(data[date_column], utc=True)
        data[date_column] = data[date_column].fillna(method='ffill')
        print('\nFiltering Data to Only 2017-2021 Values:')
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
                'Data before ',
                MIN_YEAR,
                ' represents ',
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

        return data
    print(
        u'\u2326',
        'No data found in correct format to process. ',
        'Please review FLH Partner Site Data Collection Form!',
    )
    return None


def validate_address_data(data) -> pd.DataFrame:
    """Validates and cleans the address data
    Creates indicator variables to assign GEOIDs
    Inputs
    ------
    data: loaded data that already has been cleaned
    Outputs
    -------
    validated_data: cleaned and validated data with columns that
    indicate which geoid methods can be used 1 to 4
    """
    # Checking for required address columns (even if the columns are empty):
    print('\nProcessing Address Columns:')
    has_address_columns = set(REQUIRED_ADDRESS_COLUMNS).issubset(set(data.columns))
    if has_address_columns:
        print(u'\u2713', 'Data has all the required column headings for addresses.')
    else:
        print(
            u'\u2326',
            'Some required address columns are missing ',
            'in the input file - ',
        )
        print(
            u'\u2326',
            'Please add all required column names ',
            'to file, even if the column is empty.',
        )

    avail_cols = {
        'has_street': False,
        'has_city': False,
        'has_state': False,
        'has_county': False,
        'has_zip': False,
        'has_lat': False,
        'has_long': False,
        'has_census_tract': False,
        'has_geoid': False,
    }
    # Look for census_tract + state_code + county_code / GEOID columns
    print('Looking for Census_Tract / GEOID column:')
    if 'geoid' in data.columns:
        if data['geoid'].isnull().all():
            print(u'\u2326', 'GEOID column is empty and will not be used.')
        else:
            avail_cols['has_geoid'] = True
            print(u'\u2713', 'Process found the GEOID column.')
    else:
        geoid_column = [item for item in data.columns if 'geoid' in item]
        if len(geoid_column) == 0:
            print(u'\u2326', 'No GEOID column was found.')
        else:
            data['geoid'] = data[geoid_column[0]]
            if data['geoid'].isnull().all() is False:
                avail_cols['has_geoid'] = True
                print(
                    u'\u2713',
                    'Process will use ',
                    data['geoid'][0],
                    ' as the GEOID column.',
                )
            else:
                print(
                    u'\u2326',
                    'GEOID column',
                    data['geoid'][0],
                    ' is empty and will not be used.',
                )

    if 'census_tract' in data.columns:
        if data['census_tract'].isnull().all():
            print(u'\u2326', 'Census_Tract column is empty and will not be used.')
        else:
            avail_cols['has_census_tract'] = True
            print(u'\u2713', 'Process found the Census_Tract column.')
    else:
        census_tract_columns = [item for item in data.columns if 'census' in item]
        if len(census_tract_columns) == 0:
            print(u'\u2326', 'No Census_Tract column was found.')
        else:
            data['census_tract'] = data[census_tract_columns[0]]
            if data['census_tract'].isnull().all() is False:
                avail_cols['has_census_tract'] = True
                print(
                    u'\u2713',
                    'Process will use ',
                    census_tract_columns[0],
                    ' as the City columns. ',
                )
            else:
                print(
                    u'\u2326',
                    'Census tract column',
                    census_tract_columns[0],
                    ' is empty and will not be used.',
                )

    # Process for street address column:
    print('Looking for Street_Address_1 column:')
    if 'street_address_1' in data.columns:
        if data['street_address_1'].isnull().all() is False:
            print(u'\u2326', 'Street_Address_1 column is empty and will not be used.')
        else:
            avail_cols['has_street'] = True
            print(u'\u2713', 'Process found the Street_Address_1 column.')
    else:
        street_columns = [
            item for item in data.columns if 'street' in item or item == 'address'
        ]
        if len(street_columns) == 0:
            print(u'\u2326', 'No Street_Address column was found.')
        else:
            data['street_address_1'] = data[street_columns[0]]
            if len(data['street_address_1']) > 0:
                avail_cols['has_street'] = True
                print(
                    u'\u2713',
                    'Process will use ',
                    street_columns[0],
                    ' as the Street_Address_1 column. ',
                )

    # Look for zip code column:
    print('Looking for Zip_Code column:')
    if 'zip_code' in data.columns:
        if data['zip_code'].isnull().all():
            print(u'\u2326', 'Zip_Code column is empty and will not be used.')
        else:
            avail_cols['has_zip'] = True
            print(u'\u2713', 'Process found the Zip_Code column.')
    else:  # Find a column that looks like zip
        zip_columns = [item for item in data.columns if 'zip' in item or 'post' in item]
        if len(zip_columns) == 0:
            print(u'\u2326', 'No Zip_Code column was found.')
        else:
            data['zip_code'] = data[zip_columns[0]]
            if data['zip_code'].isnull().all() is False:
                avail_cols['has_zip'] = True
                print(
                    u'\u2713',
                    'Process will use ',
                    zip_columns[0],
                    ' as the Zip_Code column. ',
                )

    # Identify state column
    print('Looking for State column:')
    if 'state' in data.columns:
        if data['state'].isnull().all():
            print(u'\u2326', 'State column is empty and will not be used.')
        else:
            avail_cols['has_state'] = True
            print(u'\u2713', 'Process found the State column.')
    else:
        state_columns = [
            item for item in data.columns if 'state' in item and 'code' not in item
        ]
        if len(state_columns) == 0:
            print(u'\u2326', 'No State column was found.')
        else:
            data['state'] = data[state_columns[0]]
            if data['state'].isnull().all() is False:
                avail_cols['has_state'] = True
                print(
                    u'\u2713',
                    'Process will use ',
                    state_columns[0],
                    ' as the State  column. ',
                )

    # Look for city column
    print('Looking for City column:')
    if 'city' in data.columns:
        if data['city'].isnull().all():
            print(u'\u2326', 'City column is empty and will not be used')
        else:
            avail_cols['has_city'] = True
            print(u'\u2713', 'Process found the City column.')
    else:
        city_columns = [item for item in data.columns if 'city' in item]
        if len(city_columns) == 0:
            print(u'\u2326', 'No City column was found.')
        else:
            data['city'] = data[city_columns[0]]
            if data['city'].isnull().all() is False:
                avail_cols['has_city'] = True
                print(
                    u'\u2713',
                    'Process will use ',
                    city_columns[0],
                    ' as the City column. ',
                )

    # Look for county column
    print('Looking for County column:')
    if 'county' in data.columns:
        if data['county'].isnull().all():
            print(u'\u2326', 'County column is empty and will not be used.')
        else:
            avail_cols['has_county'] = True
            print(u'\u2713', 'Process found the County column.')
    else:
        county_columns = [item for item in data.columns if 'county' in item]
        if len(county_columns) == 0:
            print(u'\u2326', 'No County column was found.')
        else:
            data['county'] = data[county_columns[0]]
            if data['county'].isnull().all() is False:
                avail_cols['has_county'] = True
                print(
                    u'\u2713',
                    'Process will use ',
                    county_columns[0],
                    ' as the County column.',
                )

    # Look for lat long
    print('Looking for Latitude/Longitude columns:')
    if 'latitude' in data.columns:
        if data['latitude'].isnull().all():
            print(u'\u2326', 'Latitude column is empty and will not be used.')
        else:
            avail_cols['has_lat'] = True
            print(u'\u2713', 'Process found the Latitude column.')
    else:
        lat_columns = [
            item for item in data.columns if 'latitude' in item or item == 'X'
        ]
        if len(lat_columns) == 0:
            print(u'\u2326', 'No Latitude column was found.')
        else:
            data['latitude'] = data[lat_columns[0]]
            if data['latitude'].isnull().all() is False:
                avail_cols['has_lat'] = True
                print(
                    u'\u2713',
                    'Process will use ',
                    lat_columns,
                    ' as the Latitude (X) columns. ',
                )
    # Look for longitude
    if 'longitude' in data.columns:
        if data['longitude'].isnull().all():
            print(u'\u2326', 'Longitude column is empty and will not be used.')
        else:
            avail_cols['has_long'] = True
            print(u'\u2713', 'Process found the Longitude column.')
    else:
        long_columns = [
            item for item in data.columns if 'longitude' in item or item == 'Y'
        ]
        if len(long_columns) == 0:
            print(u'\u2326', 'No Longitude column was found.')
        else:
            data['longitude'] = data[long_columns[0]]
            if len(data['longitude']) > 0:
                avail_cols['has_long'] = True
                print(
                    u'\u2713',
                    'Process will use ',
                    long_columns,
                    ' as the Longitude (Y) columns. ',
                )

    # DETERMINE WHICH GEOID MATCHING METHOD WILL BE PRIMARY METHOD
    address_cols_avail = []
    data['use_geoid'] = 0
    data['use_street'] = 0
    data['use_zip'] = 0
    data['use_census'] = 0
    method = []
    # Set dictionary to determine which address to clean/use for matching
    if avail_cols['has_geoid']:
        data['use_geoid'] = 1
        address_cols_avail.append('geoid')
        method.append('GEOID matching')
    if avail_cols['has_street'] and avail_cols['has_city']:
        data['use_street'] = 1
        address_cols_avail.append('street_address_1')
        address_cols_avail.append('city')
        method.append('Street Address with City matching')
    if avail_cols['has_street'] and avail_cols['has_zip']:
        data['use_street'] = 1
        address_cols_avail.append('street_address_1')
        address_cols_avail.append('zip_code')
        method.append('Street Address with Zip matching')
    if avail_cols['has_zip']:
        data['use_zip'] = 1
        address_cols_avail.append('zip_code')
        method.append('Zip to Census Tract method')
    if avail_cols['has_census_tract']:
        address_cols_avail.append('census_tract')
        if avail_cols['has_state'] and avail_cols['has_county']:
            data['use_census'] = 1
            method.append('Census Tract + FIPS method')
    if avail_cols['has_county']:
        address_cols_avail.append('county')
    if avail_cols['has_lat']:
        address_cols_avail.append('latitude')
    if avail_cols['has_long']:
        address_cols_avail.append('longitude')

    print(
        'Based on the available address data,'
        'the process will use the following methods in order: \n',
        method,
    )
    # Deduplicate address_cols_avail
    address_cols_avail = list(set(address_cols_avail))

    # Remember which date_column we have in this data:
    if 'eviction_filing_date' in data.columns:
        print('has eviction filing date column')
        date_column = 'eviction_filing_date'
    elif 'mortgage_foreclosures' in data.columns:
        print('has foreclosure sale date column')
        date_column = 'foreclosure_sale_date'
    elif 'tax_lien_foreclosures' in data.columns:
        print('has tax lien sale date column')
        date_column = 'tax_lien_sale_date'
    else:
        date_columns = [item for item in data.columns if 'date' in item]
        date_column = date_columns[0]

    columns_to_return = [date_column, 'year', 'month'] + address_cols_avail

    data = data.loc[
        (data['year'] >= MIN_YEAR) & (data['year'] <= MAX_YEAR), columns_to_return
    ]

    return data, avail_cols


def main(config_path):
    """Processes code in order"""
    # Look for subdirectories
    sub_directories = verify_input_directory(config_path)
    # If the input_directory fails, the main function should abort:
    if sub_directories is None:
        return "The path provided does not have the expected subdirectory structure."

    # Load all 3 types of data
    df_evic = load_data(sub_directories, 'evictions')
    df_mort = load_data(sub_directories, 'mortgage_foreclosures')
    df_tax = load_data(sub_directories, 'tax_lien_foreclosures')

    if (df_evic is None) and (df_mort is None) and (df_tax is None):
        print(
            'No data files matched our requirements for this analysis.'
            'Please check the files and restart.'
        )
        return None

    if df_evic is not None:
        print('\nStarting eviction data address validation...')
        df_evic, evic_avail_cols = validate_address_data(df_evic)
    if df_mort is not None:
        print('\nStarting mortgage foreclosure data address validation...')
        df_mort, mort_avail_cols = validate_address_data(df_mort)
    if df_tax is not None:
        print('\nStarting tax-lien foreclosure data address validation...')
        df_tax, tax_avail_cols = validate_address_data(df_tax)

    # Run Address Cleaning Function on Relevant Columns to Be Used in Matching
    # Still debugging as of Sun Sep 19 11 pm !!!

    return None  # Not sure what main usually returns


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s: %(message)s"
    )
    dir_path = sys.argv[1]
    main(dir_path)
