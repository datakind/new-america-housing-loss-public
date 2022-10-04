import csv
import datetime
import os
import typing as T

from census import Census
import sys

import pandas as pd

# line below suppresses annoying SettingWithCopyWarning
pd.options.mode.chained_assignment = None

CENSUS_API_KEY = (
    ''  # Get a census API key from census.gov and put between quotation marks
)
sig_results = {}
all_results = {}


def load_census_data(census_raw_data: pd.DataFrame, census_cols: dict) -> pd.DataFrame:
    """Load the ACS data and generate relevant columns.

    Parameters
    ----------
    census_raw_data : pandas df
        Pandas df of raw ACS data
    vars: dictionary
        Dictionary of column mappings from census id to human friendly

    Returns
    -------
    census_df : pandas df
        Dataframe containing renamed ACS data
    """
    census_df = census_raw_data.rename(columns=census_cols)

    # some derived ACS quantities are below
    census_df["median-income-diff-male-vs-female"] = (
        census_df["median-income-male-worker"]
        - census_df["median-income-female-worker"]
    )

    census_df["pct-renter-occupied"] = (
        census_df["total-renter-occupied-households"] / census_df["total-households"]
    ) * 100

    census_df["pct-owner-occupied"] = (
        census_df["total-owner-occupied-households"] / census_df["total-households"]
    ) * 100

    census_df["pct-owner-occupied-mortgage"] = (
        census_df["total-owner-occupied-households-mortgage"]
        / census_df["total-households"]
    ) * 100

    census_df["pct-owner-occupied-without-mortgage"] = (
        (
            census_df["total-owner-occupied-households"]
            - census_df["total-owner-occupied-households-mortgage"]
        )
        / census_df["total-households"]
        * 100
    )

    census_df["median-year-structure-built"] = pd.to_numeric(census_df["median-year-structure-built"])
    census_df["median-house-age"] = (
        datetime.datetime.now().year - census_df["median-year-structure-built"]
    )

    census_df["pct-without-health-insurance"] = (
        census_df["without-health-insurance"]
        / (census_df["without-health-insurance"] + census_df["with-health-insurance"])
        * 100
    )

    census_df["pct-non-white"] = 100 - census_df["pct-white"]

    return census_df.drop(
        [
            "without-health-insurance",
            "with-health-insurance",
        ],
        axis=1,
    )

def get_acs_data(
    state_fips: str, county_fips: str, year: int = 2019 # The max here is determined by 'censusdata' package
) -> T.Union[T.Tuple[pd.DataFrame, T.Dict], T.Tuple[None, None]]:
    """Main function to get ACS data from the census API."""
    if state_fips is None or county_fips is None:
        return (None, None)

    vars = {
        "dataprofile": {
            "DP03_0051E": "total-households",
            "DP04_0047E": "total-renter-occupied-households",
            "DP04_0046E": "total-owner-occupied-households",
            "DP03_0062E": "median-household-income",
            "DP05_0037PE": "pct-white",
            "DP05_0038PE": "pct-af-am",
            "DP05_0039PE": "pct-am-in",
            "DP05_0044PE": "pct-asian",
            "DP05_0052PE": "pct-nh-pi",
            "DP05_0057PE": "pct-other-race",
            "DP05_0058PE": "pct-multiple-race",
            "DP05_0071PE": "pct-hispanic",
            "DP03_0119PE": "pct-below-poverty-level",
            "DP03_0099E": "without-health-insurance",
            "DP03_0096E": "with-health-insurance",
            "DP05_0001E": "pop-total",
            "DP03_0002PE": "pct-pop-in-labor-force",
            "DP02_0003PE": "pct-households-married-with-own-children",
            "DP02_0007PE": "pct-male-single-parent-household",
            "DP02_0011PE": "pct-female-single-parent-household",
            "DP02_0009PE": "pct-male-older-adult-living-alone",
            "DP02_0013PE": "pct-female-older-adult-living-alone",
            "DP02_0014PE": "pct-households-with-children",
            "DP02_0015PE": "pct-households-with-elderly",
            "DP02_0053PE": "pct-enrolled-in-school",
            "DP02_0059E": "education-attained",
            "DP02_0060E": "level-of-education-less-than-9th",
            "DP02_0113PE": "pct-non-english-spoken-in-home",
            "DP02_0114PE": "pct-english-fluency-not-great",
            "DP02_0152PE": "pct-own-computer",
            "DP02_0153PE": "pct-broadband-internet",
            "DP03_0009PE": "unemployment-rate",
            "DP03_0011PE": "pct-women-in-labor-force",
            "DP03_0025E": "mean-commute-time",
            "DP03_0028PE": "pct-service-occupations",
            "DP03_0021PE": "pct-public-transport-to-work",
            "DP03_0074PE": "pct-with-snap-benefits",
            "DP03_0088E": "per-capita-income",
            "DP03_0093E": "median-income-male-worker",
            "DP03_0094E": "median-income-female-worker",
            "DP03_0022PE": "pct-walk-to-work",
            "DP04_0003PE": "pct-vacant-properties",
            "DP04_0058PE": "pct-no-vehicles-available",
            "DP04_0073PE": "pct-incomplete-plumbing",
            "DP04_0077PE": "pct-one-or-less-occupants-per-room",
            "DP04_0014PE": "pct-mobile-homes",
            "DP05_0018E": "median-population-age",
            "DP02_0069PE": "pct-veterans",
            "DP02_0094PE": "pct-foreign-born",
            "DP02_0096PE": "pct-not-us-citizen",
            "DP02_0072PE": "pct-disability"
        },
        "subject": {
            "S2506_C01_039E": "median-monthly-housing-cost",
            "S2506_C01_001E": "total-owner-occupied-households-mortgage",
        },
        "detail":{
            "B19083_001E": "gini-index",
            "B25035_001E": "median-year-structure-built",
            "B25064_001E": "median-gross-rent",
            "B25077_001E": "median-property-value"
        }
    }

    c = Census(CENSUS_API_KEY, year=year)
    dfs = []

    var_list = list(vars['dataprofile'].keys())
    df = pd.DataFrame(c.acs5dp.state_county_tract((var_list), state_fips, county_fips, Census.ALL))
    df.drop(['state', 'county', 'tract'], axis=1, inplace=True)
    dfs.append(df)

    var_list = list(vars['subject'].keys())
    df = pd.DataFrame(c.acs5st.state_county_tract((var_list), state_fips, county_fips, Census.ALL))
    df.drop(['state', 'county', 'tract'], axis=1, inplace=True)
    dfs.append(df)

    var_list = list(vars['detail'].keys())
    df = pd.DataFrame(c.acs5.state_county_tract((var_list), state_fips, county_fips, Census.ALL))
    dfs.append(df)

    data = pd.concat( dfs, axis=1).reset_index()
    data_dict=pd.DataFrame()

    census_cols = vars['dataprofile']
    census_cols.update(vars['subject'])
    census_cols.update(vars['detail'])

    census_df = load_census_data(data, census_cols)
    census_df["GEOID"] = census_df['state'].astype(str) + census_df['county'].astype(str) + census_df['tract'].astype(str)

    return census_df, data_dict
