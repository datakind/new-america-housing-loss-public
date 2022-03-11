import csv
import datetime
import os
import typing as T

import censusdata
import pandas as pd

from cli.loggy import log_machine

# line below suppresses annoying SettingWithCopyWarning
pd.options.mode.chained_assignment = None

CENSUS_API_KEY = (
    ''  # Get a census API key from census.gov and put between quotation marks
)
sig_results = {}
all_results = {}


@log_machine
def load_census_data(census_raw_data: pd.DataFrame) -> pd.DataFrame:
    """Load the ACS data and generate relevant columns.

    Parameters
    ----------
    census_raw_data : pandas df
        Pandas df of raw ACS data

    Returns
    -------
    census_df : pandas df
        Dataframe containing renamed ACS data
    """

    census_cols = {
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
        "DP02_0072PE": "pct-disability",
        "B19083_001E": "gini-index",
        "B25035_001E": "median-year-structure-built",
        "B25064_001E": "median-gross-rent",
        "B25077_001E": "median-property-value",
        "S2506_C01_039E": "median-monthly-housing-cost",
        "S2506_C01_001E": "total-owner-occupied-households-mortgage",
    }

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


@log_machine
def censusgeo2fips(cg: censusdata.censusgeo) -> str:
    """Helper function to convert a CensusData geography object into a FIPS code string.

    Parameters
    ----------
    cg : censusdata.censusgeo.censusgeo
        A Census Geography object from the CensusData library.

    Returns
    -------
    str
        The FIPS code as a string
    """
    param_dict = {r: v for (r, v) in cg.params()}
    return f"{param_dict['state']}{param_dict['county']}{param_dict.get('tract', '')}"


@log_machine
def fetch_metadata(
    source: str, year: int, tablenames: T.List, variable_subset: T.List = None
) -> T.Dict:
    """Function to get metadata for the variables in a list of tables.
    Can be optionally filtered to a subset of variables in these tables.

    Parameters
    ----------
    source : str
        The source for the tables in the CensusData package, e.g. "acs5"
    year : int
        The year for the source, e.g. 2016
    tablenames : list
        A list of tablenames (as strings) to get the variables from.
    variable_subset : list, optional
        A subset of variables to restrict the results to.

    Returns
    -------
    dict
        A dictionary of {variable: {metadata_fields}}
    """
    variables_dict = {}
    for t in tablenames:
        dt = censusdata.censustable(source, year, t)
        for v in dt.keys():
            if variable_subset == None or v in variable_subset:
                variables_dict[v] = dt[v]
    return variables_dict


@log_machine
def fetch_data(
    source: str,
    year: int,
    geo: censusdata.censusgeo,
    variables_dict: T.Dict,
    table_type: str,
    api_key: str,
) -> pd.DataFrame:
    """Fetches data from Census Bureau API on a list of variables,
    and returns a Pandas DataFrame.

    Parameters
    ----------
    source : str
        The source for the tables in the CensusData package, e.g. "acs5"
    year : int
        The year for the source, e.g. 2016
    geo : CensusData geography object
        Geography level to get data at.
    variables_dict : dict
        Dictionary of variables to request from API.
    table_type : str
        CensusData name for table containing variables, e.g. 'detail'
    api_key : str
        API key for the Census API

    Returns
    -------
    Pandas DataFrame
        Variables as columns, geography as rows
    """
    return censusdata.download(
        source,
        year,
        geo,
        [v for v in variables_dict.keys()],
        tabletype=table_type,
        key=api_key,
    )


@log_machine
def get_region_geo(state_id: str, county_id: str) -> censusdata.censusgeo:
    """Creates the appropriate Census Geography object for each FPR study region.

    Parameters
    ----------
    region : string
        Region/county/city to study

    Returns
    -------
    geo : censusgeo object
        geography object to pass for further ACS data fetching

    """
    geo = censusdata.censusgeo(
        [('state', state_id), ('county', county_id), ('tract', '*')]
    )

    return geo


@log_machine
def get_data_for_region(
    state_id: str,
    county_id: str,
    source: str,
    year: int,
    dataprofile_tables: T.List = None,
    subject_tables: T.List = None,
    detail_tables: T.List = None,
    dataprofile_filter: T.List = [],
) -> T.Tuple:
    """Creates data and data dictionary from Census API for a region.

    Parameters:
    -----------
    region : str
        An FPR study region, e.g. "national" or "forsyth"
    source : str
        The Census source survey, e.g. "acs5"
    year : int
        The year for the Census source product, e.g. 2016
    dataprofile_tables : list, optional
        A list of Data Profile tables to fetch from the API, e.g. ["DP02"]
    subject_tables : list, optional
        A list of Subject tables to fetch from the API, e.g. ["S1701_C01"]
    detail_tables : list, optional
        A list of Detail tables to fetch from the API, e.g. ["B25003"]
    dataprofile_filter : list, optional
        A list of Data Profile variables to filter from the fetched tables, e.g. ["DP02_0003E"]

    Returns
    -------
    tuple
        (DataFrame, dict) -> the data and data dictionary respectively
    """
    geo = get_region_geo(state_id, county_id)
    data_dictionary = {}
    retrieved_data = []

    if dataprofile_tables != None:
        dataprofile_variables = fetch_metadata(
            source, year, dataprofile_tables, variable_subset=dataprofile_filter
        )
        retrieved_data.append(
            fetch_data(
                source, year, geo, dataprofile_variables, "profile", CENSUS_API_KEY
            )
        )
        data_dictionary.update(dataprofile_variables)

    if subject_tables != None:
        subject_variables = fetch_metadata(source, year, subject_tables)
        retrieved_data.append(
            fetch_data(source, year, geo, subject_variables, "subject", CENSUS_API_KEY)
        )
        data_dictionary.update(subject_variables)

    if detail_tables != None:
        detail_variables = fetch_metadata(source, year, detail_tables)
        retrieved_data.append(
            fetch_data(source, year, geo, detail_variables, "detail", CENSUS_API_KEY)
        )
        data_dictionary.update(detail_variables)

    # Combine data into a single DataFrame
    all_data = pd.concat(retrieved_data, axis=1).reset_index()
    all_data["GEOID"] = all_data["index"].apply(censusgeo2fips)

    return all_data, data_dictionary


@log_machine
def get_acs_data(
    state_fips: str, county_fips: str, year: int = 2019
) -> T.Union[T.Tuple[pd.DataFrame, T.Dict], T.Tuple[None, None]]:
    """Main function to get ACS data from the census API."""
    if state_fips is None or county_fips is None:
        return (None, None)

    dataprofile_tables = ["DP02", "DP03", "DP04", "DP05"]

    fpr_variables_wishlist = [
        "DP02_0003PE",
        "DP02_0007PE",
        "DP02_0009PE",
        "DP02_0011PE",
        "DP02_0013PE",
        "DP02_0014PE",
        "DP02_0015PE",
        "DP02_0033E",
        "DP02_0053PE",
        "DP02_0059E",
        "DP02_0060E",
        "DP02_0069PE",
        "DP02_0072PE",
        "DP02_0094PE",
        "DP02_0096PE",
        "DP02_0113PE",
        "DP02_0114PE",
        "DP02_0152PE",
        "DP02_0153PE",
        "DP03_0001E",
        "DP03_0002PE",
        "DP03_0009PE",
        "DP03_0011PE",
        "DP03_0021PE",
        "DP03_0022PE",
        "DP03_0025E",
        "DP03_0028PE",
        "DP03_0051E",
        "DP03_0054E",
        "DP03_0062E",
        "DP03_0074PE",
        "DP03_0088E",
        "DP03_0093E",
        "DP03_0094E",
        "DP03_0096E",
        "DP03_0099E",
        "DP03_0119PE",
        "DP04_0003PE",
        "DP04_0014PE",
        "DP04_0046E",
        "DP04_0047E",
        "DP04_0058PE",
        "DP04_0073PE",
        "DP04_0077PE",  # look to ~0140s for rent
        "DP05_0001E",
        "DP05_0018E",
        "DP05_0033E",
        "DP05_0037PE",
        "DP05_0038PE",
        "DP05_0039PE",
        "DP05_0044PE",
        "DP05_0052PE",
        "DP05_0057PE",
        "DP05_0058PE",
        "DP05_0071PE",
    ]
    subject_tables = [
        # Poverty status in past 12 months
        "S1701_C01",
        # Demographic characteristics for occupied housing units, for all + owner-occupied + renter-occupied
        "S2502_C01",
        "S2502_C02",
        "S2502_C03",
        # Physical housing characteristics for occupied housing units, for all + owner-occupied + renter-occupied
        "S2504_C01",
        "S2504_C02",
        "S2504_C03",
        # Physical housing characteristics, mortgaged houses only
        "S2506_C01",
    ]
    # Additional detail tables relevant to housing
    detail_tables = [
        "B01003",  #
        "B19083",  # Income inequality?
        "B25003",  # Tenure
        "B25003B",
        "B25004",  # Vacancy status
        "B25035",  # Median year structure built
        "B25056",  # Contract rent
        "B25057",  # Lower quartile contract rent
        "B25058",  # Median contract rent
        "B25059",  # Upper quartile contract rent
        "B25061",  # Rent asked
        "B25063",  # Gross rent
        "B25064",  # Median gross rent
        "B25070",  # Gross rent as a percentage of household income in past 12 months
        "B25074",  # Household income by gross rent as a percentage of household income in past 12 months
        "B25075",  # Value
        "B25076",  # Lower value quartile
        "B25077",  # Median value quartile
        "B25078",  # Upper value quartile,
        "B25087",  # Mortgage status and selected monthly owner costs by mortgage status
        "B25088",  # Median selected monthly owner costs by mortgage status
        "B25092",  # Median selected monthly owner costs as a % of household income
    ]

    data, data_dict = get_data_for_region(
        state_fips,
        county_fips,
        "acs5",
        year,
        dataprofile_tables=dataprofile_tables,
        subject_tables=subject_tables,
        detail_tables=detail_tables,
        dataprofile_filter=fpr_variables_wishlist,
    )

    census_df = load_census_data(data)

    return census_df, data_dict
