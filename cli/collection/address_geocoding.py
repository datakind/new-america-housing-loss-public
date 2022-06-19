"""
A set of functions for geocoding data, either using the census geocoding API, or ZIP-Tract mapping
"""

import io
import json
import logging
import math
import typing as T
from pathlib import Path

import numpy as np
import pandas as pd
import requests
from tqdm import tqdm

# Global Variables
from const import (
    GEOCODE_CACHE_SIZE,
    GEOCODE_CHUNK_SIZE,
    GEOCODE_PAYLOAD,
    GEOCODE_RESPONSE_HEADER,
    GEOCODE_URL,
    GEOCODER_CACHE_FILE_PREFIX,
    HUD_XWALK_RESPONSE_BASE,
    PDR_ACCESS_TOKEN,
    RANDOM_SEED,
)

np.random.seed(RANDOM_SEED)

from collection.address_cleaning import get_zipcode5


def format_data_for_geocoding(input_df: pd.DataFrame) -> T.Union[pd.DataFrame, None]:
    """Given an input dataframe of clean addresses, format it to match Census batch geocoder specs."""
    # Check for empty input
    if input_df is None:
        return None
    # Store the record index to ensure we can match geocoded records back to the originals
    output_df = input_df.reset_index().copy()
    geocode_cols = [
        "index",
        "street_address_1_clean",
        "city",
        "state",
        "zip_code_clean",
    ]
    # Check that all needed columns are present and identify any absent ones
    cols_present = geocode_cols.copy()
    cols_absent = []
    for gcol in geocode_cols:
        if gcol not in output_df.columns:
            cols_present.remove(gcol)
            cols_absent.append(gcol)
    # Subset the input dataframe to the right column set
    df_geocode_cols = output_df[cols_present].copy()
    # Add any columns that were absent
    if len(cols_absent) > 0:
        for c_abs in cols_absent:
            df_geocode_cols.loc[:, c_abs] = [None] * len(df_geocode_cols)
    # Rename the columns appropriately
    df_geocode_cols.rename(
        columns={
            "index": "Unique ID",
            "street_address_1_clean": "Street address",
            "city": "City",
            "state": "State",
            "zip_code_clean": "ZIP",
        },
        inplace=True,
    )
    # Ensure the correct order of columns for geocoding input - Incorrect ordering happens if
    # some of the columns are originally missing, the code above just adds them at the end
    correct_column_order = ["Unique ID", "Street address", "City", "State", "ZIP"]

    return df_geocode_cols[correct_column_order]


def generate_geocode_chunks(df_geocode_cols: pd.DataFrame) -> pd.DataFrame:
    """Generate chunks of large files already formatted for the Census batch geocoder API"""
    full_row_count = len(df_geocode_cols)
    # If the dataset size is <= the chunk size, just yield it and return afterwards
    if full_row_count <= GEOCODE_CHUNK_SIZE:
        yield df_geocode_cols
        return
    chunk_start_row = 0
    while chunk_start_row < full_row_count:
        chunk_end_row = min(full_row_count, chunk_start_row + GEOCODE_CHUNK_SIZE - 1)
        # print(chunk_start_row, chunk_end_row)
        yield df_geocode_cols.loc[chunk_start_row:chunk_end_row, :]
        chunk_start_row += GEOCODE_CHUNK_SIZE


def census_geocode_records(df_chunk: pd.DataFrame) -> pd.DataFrame:
    """Geocode a given chunk of data using the census batch geocoding API

    Inputs
    -------
    df_chunk: A dataframe chunk of cleaned address ready for geocoding

    Outputs
    -------
    geocoded_df: geocoded response of the input dataset
    """
    text_df = df_chunk.to_csv(index=False, header=None)
    files = {"addressFile": ("chunk.csv", text_df, "text/csv")}
    r = requests.post(GEOCODE_URL, files=files, data=GEOCODE_PAYLOAD)

    geocoded_df = pd.read_csv(
        io.StringIO(r.text), names=GEOCODE_RESPONSE_HEADER, low_memory=False
    )
    # Split out the lat/long coordinates into different fields, BUT...
    # First check whether ALL coordinates are empty (i.e., EVERY record in the chunk returned "No Match")
    if geocoded_df['coordinates'].isna().all():
        geocoded_df[["long", "lat"]] = [(np.nan, np.nan)] * len(geocoded_df)
    else:
        geocoded_df[["long", "lat"]] = (
            geocoded_df["coordinates"].astype("str").str.split(",", expand=True)
        )

    return geocoded_df


def census_geocode_full_dataset(
    input_df: pd.DataFrame, data_type: str, cache_filepath: str
) -> T.Union[pd.DataFrame, None]:
    """Given an input dataframe with address data, geocode all the records in it."""
    # Check for error condition
    if input_df is None:
        return None

    # Initialize some variables
    output_df = pd.DataFrame()
    total_geocoded_chunks = 0
    cache_every_n_chunks = math.ceil(GEOCODE_CACHE_SIZE / GEOCODE_CHUNK_SIZE)
    cache_filename = (
        str(cache_filepath) + "/" + GEOCODER_CACHE_FILE_PREFIX + data_type + ".csv"
    )

    # Check to see if cached data are available; if so, use them
    cached_df = None
    if Path(cache_filename).is_file():
        print("Found cached data, resuming geocoding from the previous cache point...")
        cached_df = pd.read_csv(cache_filename)
        # Use the `id` column to find the cached record IDs from the original dataframe
        cached_ids = cached_df["id"].unique()
        # Remove those record IDs from the original dataframe and geocode the rest
        input_df = input_df[~input_df.index.isin(cached_ids)]

    # Format the dataframe for geocoding with Census Batch Geocoder API
    df_geocode_cols = format_data_for_geocoding(input_df)

    # Loop through the dataframe chunks and geocode them
    for chunk in tqdm(
        generate_geocode_chunks(df_geocode_cols),
        desc="Geocoding progress",
        total=math.ceil(len(df_geocode_cols) / GEOCODE_CHUNK_SIZE),
    ):
        geocoded_chunk = census_geocode_records(chunk)
        if len(output_df) == 0:
            output_df = geocoded_chunk
        else:
            output_df = pd.concat([output_df, geocoded_chunk], ignore_index=True)

        # Check how many chunks we have geocoded and cache if it is time to do so
        total_geocoded_chunks += 1
        if total_geocoded_chunks % cache_every_n_chunks == 0:
            # If we have a cache available, append to the geocoded data, assuming process resumed
            if cached_df is not None:
                output_df = pd.concat([cached_df, output_df], ignore_index=True)
            output_df.to_csv(cache_filename, index=False)

    # If we have a cache available, append to the geocoded data, assuming process resumed
    if cached_df is not None:
        output_df = pd.concat([cached_df, output_df], ignore_index=True)

    return output_df


def append_census_geocode_data(
    address_df: pd.DataFrame, data_type: str, cache_filepath: str
) -> T.Union[pd.DataFrame, None]:
    """Append census geocoder data to the dataframe containing raw/standardized addresses."""
    if address_df is None:
        return None, None

    # Geocode all the records and get back the data
    geocoder_data = census_geocode_full_dataset(address_df, data_type, cache_filepath)
    if geocoder_data.empty:
        return None, None
    # The return dataframe has an `id` column
    # This is the index of the input dataframe, use it for dedup and joining back
    geocoder_data.drop_duplicates(subset="id", inplace=True)
    geocoder_data.set_index("id", inplace=True)

    output_geocoded_df = address_df.merge(
        geocoder_data, how="left", left_index=True, right_index=True
    )
    # Create a census geoid column
    state_fips = [
        str(int(x)).zfill(2) if pd.notna(x) else ""
        for x in output_geocoded_df["state_fips"]
    ]
    county_fips = [
        str(int(x)).zfill(3) if pd.notna(x) else ""
        for x in output_geocoded_df["county_fips"]
    ]
    tract_id = [
        str(int(x)).zfill(6) if pd.notna(x) else "" for x in output_geocoded_df["tract"]
    ]

    output_geocoded_df["geoid"] = [
        x[0] + x[1] + x[2] for x in zip(state_fips, county_fips, tract_id)
    ]
    # Replace blanks by NaN/missing
    output_geocoded_df["geoid"].replace("", np.nan, inplace=True)

    success_record_count = output_geocoded_df["state_fips"].notna().sum()
    # NOTE: Very strange, success rate varies by run... the same record sometimes gets geocoded, sometimes not!

    return output_geocoded_df, success_record_count


def zip_to_tract_lookup(zipcode: str, data_year: int = 2020) -> T.List[T.Dict[str, float]]:
    """Retrieve census tract data for a given zipcode using the HUD crosswalk API.
    
    Inputs
    ------
    zipcode: 5-digit string (zero-padded, if necessary) representing the zipcode
    data_year: The year for which data is to be retrieved
    
    Outputs
    -------
    results: Data for all the census tracts contained in or overlapping this zipcode
    """
    # Check for empty input
    if pd.isna(zipcode):
        return None
    # Prepare the query string
    query_string = HUD_XWALK_RESPONSE_BASE + f"&year={data_year}&query={zipcode}"
    # Get the API response
    response = requests.get(
        query_string, headers={"Authorization": "Bearer " + PDR_ACCESS_TOKEN}
    )
    # Check for a successful response
    if response.status_code != 200:
        print(
            f"\t\u2326  ERROR in Zip-Tract mapping for zip {zipcode}, Status code {response.status_code}"
        )
        return None

    response_data = response.json()
    results = response_data["data"]["results"]
   
    return results
    

def get_probabilistic_zip_geoid(
        lookup_zip: str,
        zip_geoids_map: T.Dict[str, T.List[T.Dict[str, float]]]
    ) -> str:
    """Probabilistically look up GEOID for a zipcode.

    Inputs
    ------
    lookup_zip: The zipcode for which we want to probabilistically determine GEOID
    zip_geoids_map: A dictionary/JSON with the fractions of business, residential,
        other and total addresses in each GEOID that is contained in or overlaps
        that zipcode; we will use the total fraction, because in the input
        evictions/foreclosures data, we do not have a reliable way to distinguish
        business vs. residential addresses

    We use the following logic for probabilistic lookup:
    1. Generate a random number between 0.0 and 1.0
    2. Loop through the results and when the cumulative sum first exceeds
       the generated random number, assign that geoid to that zipcode
    3. If we are at the last result in the loop, just return that one (this
       handles the minor edge case that the fractions do not always add up
       exactly to 1.0 and therefore could possibly never exceed the generated
       random number if it is also very close to 1.0)

    Outputs
    -------
    geoid: A probabilistically determined GEOID for the lookup zipcode
    """
    # Generate a random number
    rand_num = np.random.random()
    # Initialize sum
    cumul_tract_sum = 0.0
    zip_results = zip_geoids_map[lookup_zip]
    # If there is no data for that zipcode, return None
    if zip_results is None:
        return None
    # Loop through the results and when the cumulative sum first exceeds the
    # generated random number, assign that geoid to that zipcode 
    for idx, result in enumerate(zip_results):
        # Return the last result if we are at that point
        if idx == len(zip_results) - 1:
            return result["geoid"]
        # Otherwise do the loop and check for the condition
        cumul_tract_sum += result["tot_ratio"]
        if rand_num < cumul_tract_sum:
            return result["geoid"]


def append_zip_to_tract_data(
    addr_geocode_df: pd.DataFrame,
) -> T.Union[pd.DataFrame, None]:
    """Append ZIP to census tract data to the dataframe containing geocoded addresses (IF ANY)."""
    if ("zip_code" not in addr_geocode_df.columns) and (
        "zip_code_clean" not in addr_geocode_df.columns
    ):
        print("\u2326", "No zip code data found for additional geocoding")
        return None
    # If a clean 5-character zip code is not available, create that column
    if ("zip_code" in addr_geocode_df.columns) and (
        "zip_code_clean" not in addr_geocode_df.columns
    ):
        addr_geocode_df["zip_code_clean"] = addr_geocode_df["zip_code"].apply(
            get_zipcode5
        )
    # If no `geoid` column exists, create a blank one
    if "geoid" not in addr_geocode_df.columns:
        addr_geocode_df["geoid"] = [None] * len(addr_geocode_df)
    # Check which geoids are missing and use zip-tract lookup to fill it in, but create df copy first
    output_geocoded_df = addr_geocode_df.copy()
    # Find which records have missing GEOIDs and get the unique zipcodes in these records
    missing_geoids_zipcodes =\
        output_geocoded_df.loc[pd.isna(output_geocoded_df["geoid"])]["zip_code_clean"].unique()
    # Create a zip-geoid map using zip-to-tract lookup
    zip_geoid_map = {
        lookup_zip: zip_to_tract_lookup(lookup_zip) for lookup_zip in missing_geoids_zipcodes
    }
    # For missing GEOIDs, fill them in by applying this map to respective zipcodes
    output_geocoded_df.loc[pd.isna(output_geocoded_df["geoid"]), "geoid"] =\
        output_geocoded_df.loc[
            pd.isna(output_geocoded_df["geoid"]), "zip_code_clean"
        ].apply(get_probabilistic_zip_geoid, args=(zip_geoid_map,))

    success_record_count = output_geocoded_df["geoid"].notna().sum()

    return output_geocoded_df, success_record_count


def geocode_input_data(
    input_df: pd.DataFrame, df_avail_cols: T.List, data_type: str, cache_filepath: str
) -> T.Union[pd.DataFrame, None]:
    """Main method for geocoding raw/standardized data.

    Also defines the path logic for geocoding, depending on which columns are available.
    """
    # Check for empty input
    if input_df is None:
        return None

    # If a geoid column is available, just return the df as is
    if "geoid" in df_avail_cols:
        output_geocoded_df = input_df.copy()
        # However, standardize geoid column first to avoid merge issues later
        output_geocoded_df["geoid"] = [
            str(int(x)).zfill(11) if pd.notna(x) else ""
            for x in output_geocoded_df["geoid"]
        ]
    # If a street address is available, use the census geocoder to geocode
    elif "street_address_1" in df_avail_cols:
        print(f"\nStarting geocoding of {data_type} data...")
        addr_geocoded_df, addr_success_record_count = append_census_geocode_data(
            input_df, data_type, cache_filepath
        )
        if addr_geocoded_df is None:
            print("Unable to collect geocode information on dataset")
            return None
        print(
            f"\u2713  Address geocoding successfully geocoded",
            f"{addr_success_record_count / len(input_df) * 100:.1f}% of input records",
        )
        # If zip code is also available, use that to geocode the missing records
        if "zip_code" in df_avail_cols:
            print("Using Zip-To-Census-Tract lookup for additional geocoding...")
            output_geocoded_df, zip_success_record_count = append_zip_to_tract_data(
                addr_geocoded_df
            )
            print(
                f"\u2713  Zip-to-Census-Tract lookup successfully geocoded an additional",
                f"{(zip_success_record_count - addr_success_record_count) / len(input_df) * 100:.1f}% of input records",
            )
        else:
            output_geocoded_df = addr_geocoded_df.copy()
    # If a zip code column is available, use the zip-tract lookup
    elif "zip_code" in df_avail_cols:
        print(f"\nGeocoding {data_type} data with Zip-to-Census-Tract lookup...")
        output_geocoded_df, zip_success_record_count = append_zip_to_tract_data(
            input_df
        )
        print(
            f"\u2713  Zip-to-Census-Tract lookup successfully geocoded",
            f"{zip_success_record_count / len(input_df) * 100:.1f}% of input records",
        )
    else:
        output_geocoded_df = input_df.copy()

    return output_geocoded_df


def find_state_county_city(geocoded_df: pd.DataFrame) -> T.Tuple[str, str, str, str]:
    """Given a geocoded dataframe, determine the most likely state, county and city from it."""
    # Check for empty dataframe
    if geocoded_df is None:
        return (None, None, None, None)

    # If geoid is present use that to determine state and county FIPS
    if "geoid" in geocoded_df.columns:
        # Get the relevant column and drop empty rows
        only_geo_df = geocoded_df[["geoid"]].copy()
        only_geo_df.dropna(inplace=True)
        # Make sure it is a string value, not a numeric value
        if np.issubdtype(only_geo_df["geoid"].dtype, np.number):
            only_geo_df["geoid"] = [str(int(x)).zfill(11) for x in only_geo_df["geoid"]]
        # Now extract the value counts and get the highest one (sorted DESC by default)
        # Can get more sophisticated for low sample / higher ambiguity data, but leave for later
        most_likely_state_fips = (
            only_geo_df["geoid"].str.slice(stop=2).value_counts().index[0]
        )
        most_likely_county_fips = (
            only_geo_df["geoid"].str.slice(start=2, stop=5).value_counts().index[0]
        )
    # Otherwise check if state_fips and county_fips are present in the data
    elif ("state_fips" in geocoded_df.columns) and (
        "county_fips" in geocoded_df.columns
    ):
        # Likely these are raw outputs from the census geocoder and numeric by default
        most_likely_state_fips = geocoded_df["state_fips"].value_counts().index[0]
        most_likely_county_fips = geocoded_df["county_fips"].value_counts().index[0]
        if isinstance(most_likely_state_fips, (int, float)):
            most_likely_state_fips = str(int(most_likely_state_fips)).zfill(2)
        if isinstance(most_likely_county_fips, (int, float)):
            most_likely_county_fips = str(int(most_likely_county_fips)).zfill(3)
    else:
        most_likely_state_fips = None
        most_likely_county_fips = None

    # Find the city
    if "city" in geocoded_df.columns:
        most_likely_city_str = geocoded_df["city"].value_counts().index[0]
    else:
        most_likely_city_str = None

    # Find the state (string)
    if "state" in geocoded_df.columns:
        most_likely_state_str = geocoded_df["state"].value_counts().index[0]
    else:
        most_likely_state_str = None

    return (
        most_likely_state_fips,
        most_likely_county_fips,
        most_likely_city_str,
        most_likely_state_str,
    )
