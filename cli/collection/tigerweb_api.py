import json
import typing as T

import geopandas
import pandas as pd
import requests


# 1. Formatting JSON response objects to be more easily parsed by eye
def jprint(obj):
    # Create a formatted string of the Python JSON object
    text = json.dumps(obj, sort_keys=True, indent=4)
    print(text)


# 2. Retrieving all census tracts for the given county
def create_tigerweb_query(state_code: str, county_code: str) -> str:
    # Create an API call with the input state and county code
    base_uri = "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/tigerWMS_Census2020/MapServer/6/query?where="
    #base_uri = "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/tigerWMS_ACS2019/MapServer/8/query?where="
    connector = f"STATE%3D%27{state_code}%27+AND+COUNTY%3D%27{county_code}%27"
    end_uri = "&text=&objectIds=&time=&geometry=&geometryType=esriGeometryPolygon&inSR=&spatialRel=esriSpatialRelIntersects&distance=&units=esriSRUnit_Foot&relationParam=&outFields=&returnGeometry=true&returnTrueCurves=false&maxAllowableOffset=&geometryPrecision=&outSR=&havingClause=&returnIdsOnly=false&returnCountOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&returnZ=false&returnM=false&gdbVersion=&historicMoment=&returnDistinctValues=false&resultOffset=&resultRecordCount=&returnExtentOnly=false&datumTransformation=&parameterValues=&rangeValues=&quantizationParameters=&featureEncoding=esriDefault&f=geojson"

    # Stich everything together
    api_call = str(base_uri + connector + end_uri)
    return api_call


# 3. Function to reformat the tract codes in the TIGERweb response objects
def reformat_tract_code(tract: str, state_code: str, county_code: str) -> str:
    """Helper function to return GEOIDs compatible with those in deep-dive data files.

    Parameters
    ----------
    tract : A string
        An unformatted tract code, e.g. '53.38'
    state_code : A string
        The FIPS code for the state that we want to append.
    county_code : A string
        The FIPS code for the county that we want to append.

    Returns
    -------
    str
    """

    # If the tract code contains a period, remove it, then prepend zeroes until length is 6
    if "." in tract:
        tract = tract.replace(".", "")
        num_zeroes = 6 - len(tract)
        tract = ("0" * num_zeroes) + tract
    # Else prepend zeroes until the length is 4, then add 2 zeroes to the end
    else:
        num_zeroes = 4 - len(tract)
        tract = ("0" * num_zeroes) + tract + "00"

    # Prepend state and county FIPS codes
    geoid = state_code + county_code + tract

    return geoid


def rename_baseline(geojson_data, state_code: str, county_code: str) -> str:
    """Rename the 'BASELINE' identifiers to be named 'geoid', making it consistent with previous code output."""
    for ind, i in enumerate(geojson_data['features']):
        # Renaming 'BASELINE' key to 'census_tract_GEOID'
        geojson_data['features'][ind].get('properties')['geoid'] = (
            geojson_data['features'][ind].get('properties').pop('BASENAME')
        )
        # Reformatting the Census Tract IDs to conform to processed data set IDs
        geojson_data['features'][ind].get('properties')['geoid'] = reformat_tract_code(
            geojson_data['features'][ind].get('properties')['geoid'],
            state_code,
            county_code,
        )

    # Return the re-labeled GeoJSON
    return geojson_data


def get_input_data_geometry(
    state_fips: str, county_fips: list, geojson_filename: str
) -> T.Union[geopandas.GeoDataFrame, None]:
    """Main function to return geometry data for the input data/partner site."""
    # Check for invalid input
    if state_fips is None or county_fips is None:
        return None
    geojson_gdf_output = geopandas.GeoDataFrame()
    for i in county_fips:
        # Assemble the request URI and retrieve the data
        request = create_tigerweb_query(state_fips, i)
        response = requests.get(str(request))
        response = rename_baseline(response.json(), state_fips, i)

        # Write the JSON response to a file and read into a geopandas dataframe
        with open(geojson_filename, 'w') as outfile:
            json.dump(response, outfile)
        geojson_gdf = geopandas.read_file(geojson_filename)
        geojson_gdf_output = geojson_gdf_output.append(geojson_gdf)

    return geojson_gdf_output
