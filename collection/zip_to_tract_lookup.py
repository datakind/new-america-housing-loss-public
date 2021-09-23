import json
import os

import dotenv
import numpy as np
import requests

response_base = "https://www.huduser.gov/hudapi/public/usps?type=1"
# Load the .env file and get the HUD PD&R data access token from it
dotenv.load_dotenv()
PDR_ACCESS_TOKEN = os.getenv("HUD_PDR_TOKEN", "")
# Set a random number seed; try to find a better method than setting this here
RANDOM_SEED = 123456
np.random.seed(RANDOM_SEED)


# Formatting JSON response objects to be more easily parsed by eye
def jprint(obj: str) -> str:
    """Create a formatted string of the Python JSON object."""
    text = json.dumps(obj, sort_keys=True, indent=4)
    print(text)


def zip_to_tract_lookup(zipcode: str, data_year: int) -> str:
    """Determine (probabilistically) the census tract for a given zipcode.

    Inputs
    ------
    zipcode: 5-digit string (zero-padded, if necessary) representing the zipcode
    data_year: The year for which data is to be retrieved

    Outputs
    -------
    geoid: The census tract determined for this zipcode based on the lookup
    """
    # Prepare the query string
    query_string = response_base + f'&year={data_year}&query={zipcode}'
    # Get the API response
    response = requests.get(
        query_string, headers={"Authorization": "Bearer " + PDR_ACCESS_TOKEN}
    )
    # Check for a successful response
    if response.status_code != 200:
        print(f"Error in response, Status code {response.status_code}")
        return None

    response_data = response.json()
    results = response_data['data']['results']
    print(json.dumps(response_data, sort_keys=True, indent=4))
    # Generate a random number
    rand_num = np.random.random()
    print(f'Random number = {rand_num}')
    cumul_tract_sum = 0.0
    # Loop through the results and when the cumulative sum first exceeds the
    # generated random number, assign that geoid to that zipcode; and if we
    # are at the last result in the loop, just return that one
    for idx, result in enumerate(results):
        print(idx)
        print(result)
        # Return the last result if we are at that point
        if idx == len(results) - 1:
            return result['geoid']
        # Otherwise do the loop and check for the condition
        cumul_tract_sum += result['tot_ratio']
        if rand_num < cumul_tract_sum:
            return result['geoid']


input_zip = '43235'
output = zip_to_tract_lookup(input_zip, 2019)
print(f'GEOID assigned to zipcode {input_zip} is {output}')
