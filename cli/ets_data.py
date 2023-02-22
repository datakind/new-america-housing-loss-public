"""
Created on Sat Sep 18 23:11:22 2021

@author: datakind
"""
import logging
import os
import sys
import typing as T
import io
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


def main(input_path: str) -> None:
    cities = requests.get("https://evictionlab.org/uploads/all_sites_monthly_2020_2021.csv")
    city_content = cities.content
    #put into dataframe
    city_df = pd.read_csv(io.StringIO(city_content.decode('utf-8')))
    return None
