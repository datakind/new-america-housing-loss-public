import os

import dotenv

REQUIRED_SUB_DIRECTORIES = [
    'evictions',
    'mortgage_foreclosures',
    'tax_lien_foreclosures',
]

REQUIRED_ADDRESS_COLUMNS = ['street_address_1', 'city', 'state', 'zip_code']
# Can use these to subset data if needed, but don't truncate too much data right now
MIN_YEAR = 2016
MAX_YEAR = 2999

# The year used to get ACS data
ACS_YEAR = 2020

GEOCODE_URL = 'https://geocoding.geo.census.gov/geocoder/geographies/addressbatch'
GEOCODE_PAYLOAD = {
    #'benchmark': 'Public_AR_Current',
    #'vintage': 'Current_Current',
    'benchmark':'Public_AR_Census2020',
    'vintage':'Census2020_Census2020',
    'response': 'json',
}

#https://geocoding.geo.census.gov/geocoder/vintages?benchmark=Public_AR_Current

GEOCODE_RESPONSE_HEADER = [
    'id',
    'geocoded_address',
    'is_match',
    'is_exact',
    'returned_address',
    'coordinates',
    'tiger_line',
    'side',
    'state_fips',
    'county_fips',
    'tract',
    'block',
]
GEOCODE_CHUNK_SIZE = 100
GEOCODE_CACHE_SIZE = 1000

HUD_XWALK_RESPONSE_BASE = "https://www.huduser.gov/hudapi/public/usps?type=1"
# Load the .env file and get the HUD PD&R data access token from it
dotenv.load_dotenv()
PDR_ACCESS_TOKEN = os.getenv("HUD_PDR_TOKEN", "")
# Set a random number seed; try to find a better method than setting this here
RANDOM_SEED = 123456

STAT_SIGNIFICANCE_CUTOFF = 0.05

OUTPUT_PATH_GEOCODER_CACHE = 'output_data/geocoder_caches/'
GEOCODER_CACHE_FILE_PREFIX = 'geocoder_cache_'
OUTPUT_PATH_GEOCODED_DATA = 'output_data/full_datasets/'
OUTPUT_PATH_PLOTS = 'output_data/analysis_plots/'
OUTPUT_PATH_PLOTS_DETAIL = 'detailed_results'
OUTPUT_ALL_HOUSING_LOSS_PLOTS = (
    'output_data/analysis_plots/correlations_all_housing_loss'
)
OUTPUT_EVICTION_PLOTS = 'output_data/analysis_plots/correlations_eviction_only'
OUTPUT_FORECLOSURE_PLOTS = 'output_data/analysis_plots/correlations_foreclosure_only'
OUTPUT_PATH_SUMMARIES = 'output_data/data_summaries/'
OUTPUT_PATH_MAPS = 'output_data/mapping_data/'

GEOCODED_EVICTIONS_FILENAME = 'evictions_data_geocoded.csv'
GEOCODED_FORECLOSURES_FILENAME = 'foreclosures_data_geocoded.csv'
GEOCODED_TAX_LIENS_FILENAME = 'tax_liens_data_geocoded.csv'
HOUSING_LOSS_TIMESERIES_FILENAME = 'housing_loss_timeseries.png'
ACS_DATA_DICT_FILENAME = 'acs_data_dictionary.csv'
HOUSING_LOSS_SUMMARY_FILENAME = 'housing_loss_summary.csv'
TRACT_BOUNDARY_FILENAME = 'census_tract_boundaries.geojson'
GIS_IMPORT_FILENAME = 'gis_data_import.gpkg'
EVIC_ADDRESS_ERR_FILENAME = 'evic_address_errors.csv'
MORT_ADDRESS_ERR_FILENAME = 'mort_address_errors.csv'
TAX_ADDRESS_ERR_FILENAME= 'tax_address_errors.csv'
