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

from load_data import main as load_data

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
    evictions = pd.read_csv(io.StringIO(city_content.decode('utf-8')))

    #drop if city is NaN
    evictions = evictions.dropna(subset=['city'])
    #split city and state
    evictions[['city', 'state']] = evictions['city'].str.split(',', expand=True)

    #removing sealed GeoID records 
    evictions=evictions[evictions.GEOID != 'sealed']

    #reformatting the date 
    evictions['temp_month'] = evictions['month'].str[:3]
    evictions['temp_year'] = evictions['month'].str[-5:]
    evictions['Eviction_Filing_Date'] = evictions.temp_month.str.cat(evictions.temp_year, sep='01')

    #dropping unnecessary columns
    evictions = evictions.drop('temp_month', axis=1)
    evictions = evictions.drop('temp_year', axis=1)

    #adding required blank columns 
    evictions[['Street_Address_1', 'County', 'zip_code']] = ['1 main street', '', 99999]

    #checking total number of non-sealed evictions
    print(f"Total number of evictions: {evictions['filings_2020'].sum()}")

    #disaggregating data
    evictions=evictions.reindex(evictions.index.repeat(evictions.filings_2020))

    #checking length of dataframe it should equal the total number of evictions
    print(f"Length of the disaggregated dataframe: {len(evictions)}")

    #dropping total number of evictions 
    evictions = evictions.drop('filings_2020', axis=1)

    #Converting date column to string 
    evictions = evictions.astype({'Eviction_Filing_Date':'string'})
    evictions['ID'] = np.arange(len(evictions))

    #create a folder called ets_data if there is not one already
    if not os.path.exists('ets_data'):
        os.makedirs('ets_data')

    #loop through the cities
    for city in evictions['city'].unique():

        #create a folder for the city if there is not one already 
        if not os.path.exists('ets_data/' + city):
            os.makedirs('ets_data/' + city)

        #create evictions, foreclosures, and tax liens folders if there are not already
        if not os.path.exists('ets_data/' + city + '/evictions'):
            os.makedirs('ets_data/' + city + '/evictions')
        if not os.path.exists('ets_data/' + city + '/mortgage_foreclosures'):
            os.makedirs('ets_data/' + city + '/mortgage_foreclosures')
        if not os.path.exists('ets_data/' + city + '/tax_lien_foreclosures'):
            os.makedirs('ets_data/' + city + '/tax_lien_foreclosures')

        #save the evictions data to the evictions folder
        evictions.to_csv('ets_data/' + city + '/evictions/evictions.csv', index=False)

    #loop through the cities
    for city in evictions['city'].unique():
        #call load_data with the new evictions file
        load_data('ets_data/' + city + '/')

    return None
