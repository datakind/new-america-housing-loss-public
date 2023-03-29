import os
import numpy as np
import pandas as pd
import requests 
import json
import time
import io
import subprocess
from io import StringIO

from multiprocessing import Process, Lock
os.system('pip install python-dotenv')
from dotenv import load_dotenv
load_dotenv()

username = os.getenv('FIRST_SUPERUSER')
password = os.getenv('FIRST_SUPERUSER_PASSWORD')
LOCALAPISITE = os.getenv('LOCALAPISITE')
APISITE = os.getenv('APISITE')
EL_USER_ID = os.getenv('EL_USER_ID')
OUTPUT_PATH = os.getenv('OUTPUT_PATH')
EL_CITY_URL = os.getenv('EL_CITY_URL')
EL_STATE_URL = os.getenv('EL_STATE_URL')


def load_ets_data(link) -> pd.DataFrame:
    """Load data from a link and return a dataframe
    Arguments:
        link {str} -- link to the Eviction Lab data monthly file for either all states or cities
    Returns:
        pd.DataFrame -- dataframe of data
    """
    data = requests.get(link)
    data_content = data.content
    df = pd.read_csv(io.StringIO(data_content.decode('utf-8')))
    return df

def clean_ets_data(df: pd.DataFrame, geo: str) -> pd.DataFrame:
    """Clean data from ETS and return a dataframe to input into FEAT API
    Arguments:
        df {pd.DataFrame} -- dataframe of input Eviction Lab data of all states or cities
        geo {str} -- string specifiying if the data is at the 'state' or 'city' level
    Returns:
        pd.DataFrame -- data re-formatted to feed into FEAT API
    """
    evictions = df.dropna(subset=[geo])
    #drop if type is not Census Tract
    evictions = evictions[evictions['type'] == 'Census Tract']
    if geo == 'city':
        evictions[['city', 'state']] = evictions['city'].str.split(',', expand=True)
        #replace spaces with underscores
        evictions['city'] = evictions['city'].str.replace(' ', '_')
        #replace state with FL if city includes Miami
        evictions['state'] = np.where(evictions['city'].str.contains('Miami'), 'FL', evictions['state'])
    evictions = evictions[evictions['GEOID'] != 'sealed']
    #reformat date
    evictions['temp_month'] = evictions['month'].str[:3]
    evictions['temp_year'] = evictions['month'].str[-5:]
    evictions['Date'] = evictions.temp_month.str.cat(evictions.temp_year, sep='01')
    #drop unnecessary columns
    evictions = evictions.drop(['temp_month', 'temp_year', 'type'], axis=1)
    #for each variable, confirm not in the data and create if needed 
    for var in ['city', 'County', 'zip_code', 'type']:
        if var not in evictions.columns:
            if var == 'zip_code':
                evictions[var] = 99999
            elif var == 'type':
                evictions[var] = 'evictions'
            else:
                evictions[var] = var
    
    sum_filing = evictions['filings_2020'].sum() 
    #disaggregating data
    evictions=evictions.reindex(evictions.index.repeat(evictions.filings_2020))
    #length of dataframe should equal the total number of evictions
    assert sum_filing == len(evictions)
    #dropping total number of evictions 
    evictions = evictions.drop('filings_2020', axis=1)
    #Converting date column to string 
    evictions = evictions.astype({'Date':'string'})
    evictions['ID'] = np.arange(len(evictions))
    #Make fake unique addresses
    evictions['street_address_1'] = evictions.ID.astype(str) + ' main street'
    return evictions

def authentification(username: str, password: str, api_path: str)-> str:
    """Authenticate to the FEAT API and return a bearer token
    Arguments:
        username {str} -- username for FEAT API
        password {str} -- password for FEAT API
        api_path {str} -- path to FEAT API, could be the public or local host
    Returns:
        str -- bearer token
    """
    cmd =  "curl -v -X 'POST' '"+ api_path +"/login/oauth' \
        -H 'accept: application/json' \
        -H 'Content-Type: application/x-www-form-urlencoded' \
        -d 'grant_type=&username=" + username + "&password=" + password+ "&scope=&client_id=&client_secret='"
    output = subprocess.check_output(cmd, shell=True)
    #get just the access_token from bearer
    bearer = json.loads(output.decode('utf-8'))['access_token']
    return bearer

def featapi_upload(filename: str, access_key: str, api_path: str, user_id: str) -> str:
    """Upload a file to the FEAT API run endpoint and return a run_id
    Arguments:
        filename {str} -- name of the file to submit for a run through FEAT e.g. Albuquerque.csv
        access_key {str} -- bearer token returned from authentification function
        api_path {str} -- path to FEAT API, could be the public or local host 
        user_id {str} -- user id for FEAT API
    Returns:
        str -- run_id
    """
    F = 'input_file=@'+filename+';type=text/csv'
    k = f"{access_key}'"
    
    cmd =  "curl -v -X 'POST' '"+ api_path +"/feat/submit?ui_user_id="+ user_id +"' \
        -H 'accept: application/json' \
        -H 'Authorization: Bearer " + k  + " -H 'Content-Type: multipart/form-data' -F" +  F
    
    output = subprocess.check_output(cmd, shell = True)
    run_id = json.loads(output.decode('utf-8'))['id']

    status = 0
    while status < 100 :
        cmd_return = "curl -X 'GET' '" + api_path + "/feat/runs/" + run_id + "' -H 'accept: application/json' \
            -H 'Authorization: Bearer " + k 
        output_status = subprocess.check_output(cmd_return, shell = True)
        status = json.loads(output_status.decode('utf-8'))['percent_complete']
        print('status of ', filename, ' is at ', status)
        time.sleep(10)
    print(run_id)

def run_feat(df:pd.DataFrame, geo: str, access_key: str, api_path: str, user_id: str):
    """Run each city or state through the FEAT API and collect the run ids
    Arguments:
        df {pd.DataFrame} -- cleaned dataframe returned from clean_ets_data function to be run through FEAT
        geo {str} -- string specifiying if the data is at the 'state' or 'city' level
        access_key {str} -- bearer token returned from authentification function
        api_path {str} -- path to FEAT API, could be the public or local host
        user_id {str} -- user id for FEAT API
    """
    if not os.path.exists(OUTPUT_PATH):
        os.makedirs(OUTPUT_PATH)
    df.groupby(geo).apply(
        lambda group: pd.DataFrame.to_csv(group, os.path.join(OUTPUT_PATH, f'{group.name}.csv'))
    )
    run_ids = dict()
    for filename in os.listdir(OUTPUT_PATH):
        print('filename', filename)
        run_id = featapi_upload(os.path.join(OUTPUT_PATH, filename), access_key, api_path, user_id)
        #define a dictionary to store the geo_type and run_id
        run_ids[filename] = run_id
    #save the dictionary to a csv
    run_ids_df = pd.DataFrame.from_dict(run_ids, orient='index')
    run_ids_df.to_csv(OUTPUT_PATH + 'run_ids' + geo + '.csv', index=True, header=False)

def main(link, geo, api, user_id) -> None:
    """Main function to run the script
    Arguments:
        link {str} -- link to the ETS data
        geo {str} -- string specifiying if the data is at the 'state' or 'city' level
        api {str} -- path to FEAT API, could be the public or local host
        user_id {str} -- user id for FEAT API
    """
    df = load_ets_data(link)
    evictions = clean_ets_data(df, geo)
    access_key = authentification(username, password, APISITE)
    run_feat(evictions, geo, access_key, api, user_id)
    return None

if __name__ == '__main__':
    main(EL_CITY_URL, 'city', LOCALAPISITE, EL_USER_ID)
    main(EL_STATE_URL, 'state', LOCALAPISITE, EL_USER_ID)
