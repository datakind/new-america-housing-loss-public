# DataKind New America Housing Loss Analysis Tool #

A Command Line Interface (CLI) that allows users to ingest eviction, 
foreclosure, and tax lien data and outputs statistical summaries and 
geolocation data. 

## Python CLI Usage Instructions
1. Ensure your data format is configured based on the specification in this [Google Sheet](https://docs.google.com/spreadsheets/d/1WKxpcxZI_MMJJ5lqcwauhsuw3NBxwcG5/edit?usp=sharing&ouid=104702318722434350576&rtpof=true&sd=true)
2. Download Python 3.8.10 here: https://www.python.org/downloads/release/python-3810/ and follow the instructions to install Python
3. Navigate to the New America Housing Loss Tool here: https://github.com/datakind/new-america-housing-loss-public.git
4. Click `Code` and `Download Zip` to download the code
5. Unzip the zip file in the directory of your choice
6. Open a terminal or a command prompt and navigate to where the code was downloaded (note: this directory will end with `new-america-housing-loss-public-main`)
7. Change directory to the `cli` using the command `cd cli` 
8. If you are running Windows, you will need to run the following commands to install dependencies:
   1. `py -m pip install whl/GDAL-3.3.3-cp38-cp38-win_amd64.whl`
   2. `py -m pip install whl/Fiona-1.8.20-cp38-cp38-win_amd64.whl`
9. Run the following commands to install dependencies:
   1. For Mac/Linux, run `python -m pip install -r requirements.txt`
   2. For Windows, run `py -m pip install -r requirements.txt`
10. Run the tool against your data:
    1. For Mac/Linux, run `python load_data.py /path/to/data/`
    2. For Windows, run `py load_data.py C:\path\to\data\`

## Structure

* `cli/` - code to run the DataKind New America Housing Loss Analysis Tool
