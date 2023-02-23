import typing as T
from pathlib import Path

import pandas as pd
import scourgify

from collection.address_cleaning import get_zipcode5
from const import MAX_YEAR, MIN_YEAR, REQUIRED_ADDRESS_COLUMNS, REQUIRED_SUB_DIRECTORIES

import debugpy



def search_dataframe_column(
    df_to_search: pd.DataFrame, col_to_find: str, alt_col_name: str
) -> T.Tuple[bool, T.Union[str, None]]:
    """Search for a column in a dataframe; if not found, return an alternate match if it exists."""
    print(f"Looking for '{col_to_find}' column:")
    # Initialize variables
    col_name_lower = col_to_find.lower()
    found_column = False
    use_alt_column = None
    # Begin search for the specified column (as per data format guide)
    if col_name_lower in df_to_search.columns:
        # Check if the column has all missing values
        if df_to_search[col_name_lower].notna().sum() == 0:
            print(f"\u2326 '{col_to_find}' column is empty and will not be used.")
        else:
            found_column = True
            print(f"\u2713 Process found the '{col_to_find}' column.")
    else:
        alt_columns_found = [
            col for col in df_to_search.columns if alt_col_name.lower() in col
        ]
        if len(alt_columns_found) == 0:
            print(
                f"\u2326 Process did not find the '{col_to_find}' column or an alternative to it."
            )
        else:
            # Currently this process is only searching for one alternative column
            # Could improve later to search more, although the likelihood of multiples is small
            use_alt_column = alt_columns_found[0]
            if df_to_search[use_alt_column].notna().sum() > 0:
                found_column = True
                print(
                    f"\u2713 Process will use '{use_alt_column}' as the '{col_to_find}' column."
                )
            else:
                print(f"\u2326 No alternative column to '{col_to_find}' was found.")

    return found_column, use_alt_column


def validate_address_data(data: pd.DataFrame) -> pd.DataFrame:
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
    # Converting all column names to lowercase
    data.columns = [col.lower() for col in data.columns]
    # Checking for required address columns (even if the columns are empty):
    print(f"\nProcessing address columns for data:")
    has_address_columns = set(REQUIRED_ADDRESS_COLUMNS).issubset(set(data.columns))
    if has_address_columns:
        print(u'\u2713', 'Data has all the required column headings for addresses.')
    else:
        print(
            u'\u2326',
            'Some required address columns are missing -',
            'Please add required columns, even if the column is empty.',
        )

    # SEARCH FOR NECESSARY COLUMNS IN THE DATA FOR FURTHER PROCESSING
    search_col_dict = {
        'Street_Address_1': 'Address',
        'City': 'City',
        'State': 'State',
        'County': 'County',
        'Zip_Code': 'ZIP',
        'GEOID': 'Census_Tract',
        'Parcel_ID': 'TAXPIN',
        'Latitude': 'YC',
        'Longitude': 'XC',
    }
    # Initialize some variables
    avail_columns = {'has_' + col.lower(): False for col in search_col_dict.keys()}
    usable_address_cols = []
    # Loop through the columns and try to find it (or an alternate) in the dataframe
    for search_col_name, alt_col_name in search_col_dict.items():
        is_found, alt_column = search_dataframe_column(
            data, search_col_name, alt_col_name
        )
        avail_columns['has_' + search_col_name.lower()] = is_found
        if is_found:
            usable_address_cols.append(search_col_name.lower())
        if alt_column is not None:
            data.rename(
                columns={alt_column.lower(): search_col_name.lower()}, inplace=True
            )

    # DETERMINE WHICH GEOID MATCHING METHOD WILL BE PRIMARY METHOD
    data['use_geoid'] = 0
    data['use_street'] = 0
    method = []
    # Set dictionary to determine which address to clean/use for matching
    if avail_columns['has_geoid']:
        data['use_geoid'] = 1
        method.append('GEOID matching')
    if (
        avail_columns['has_street_address_1']
        and avail_columns['has_city']
        and avail_columns['has_state']
    ):
        data['use_street'] = 1
        method.append('Street Address with City/State matching')
    if avail_columns['has_street_address_1'] and avail_columns['has_zip_code']:
        data['use_street'] = 1
        method.append('Street Address with Zip matching')

    print(
        'Based on the available address data,'
        'the process will use the following methods in order: \n\t',
        method,
    )

    date_column = "date"
    columns_to_return = [date_column, 'year', 'month'] + usable_address_cols

    data = data.loc[
        (data['year'] >= MIN_YEAR) & (data['year'] <= MAX_YEAR), columns_to_return
    ]

    return data, columns_to_return


def get_clean_address(input_address: str) -> T.Union[T.Dict, None]:
    """Given an address string, return a dict of standardized address tags.

    Inputs
    ------
    input_address: A raw/semi-clean address

    Outputs
    -------
    clean_address: A cleaned address based on the address tags produced by the usaddress-scourgify library
    """
    if pd.isna(input_address):
        return None
    try:
        # Get address tags for the input address using the usaddress-scourgify library
        clean_address_tags = scourgify.normalize_address_record(input_address)
        # Extract the cleaned address from the address_line_1 field
        return clean_address_tags['address_line_1']
    except Exception as e:
        print(f'\t\u2326  ERROR parsing address "{input_address}": {e}')
        # For now just create a blank record (and drop it), but could think about better error handling later
        return f'ERROR parsing address "{input_address}": {e}'


def standardize_input_addresses(input_df: pd.DataFrame) -> T.Union[T.Tuple[pd.DataFrame, pd.DataFrame, list], T.Tuple[None, None, None]]:
    """Standardize the address column(s) and the ZIP code in a dataframe."""
    if input_df is None:
        return (None, None, None)
    # Standardize the addresses using the usaddress-scourgify library
    output_df, df_avail_cols = validate_address_data(input_df)
    if 'street_address_1' in df_avail_cols:
        print(f"\nStandardizing {data_type} data addresses for geocoding...")
        df_all_addresses = output_df
        df_all_addresses['street_address_1_clean'] = output_df['street_address_1'].apply(
            get_clean_address
        )
        df_errors = df_all_addresses[df_all_addresses['street_address_1_clean'].str.contains('ERROR')][['street_address_1', 'city', 'state', 'zip_code', 'street_address_1_clean']]
        df_errors.rename(columns = {'street_address_1_clean': 'errors'}, inplace = True)
        output_df = df_all_addresses[~df_all_addresses['street_address_1_clean'].str.contains('ERROR')]
        print(
            f"\u2713  {output_df['street_address_1_clean'].notna().sum() / len(output_df) * 100:.1f}% of",
            'input records were successfully cleaned and standardized for geocoding.',
        )
        df_avail_cols.append('street_address_1_clean')
    # Also standardize the zip codes to 5-character strings
    if 'zip_code' in df_avail_cols:
        output_df['zip_code_clean'] = output_df['zip_code'].apply(get_zipcode5)
        df_avail_cols.append('zip_code_clean')

    return output_df, df_errors, df_avail_cols
