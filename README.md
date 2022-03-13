# DataKind New America Housing Loss Analysis Tool #

A Command Line Interface (CLI) that allows users to ingest eviction, 
foreclosure, and tax lien data and outputs statistical summaries and 
geolocation data. 

## Python CLI Usage Instructions
1. Ensure your data format is configured based on the required specification 
   1. This [Google Sheet](https://docs.google.com/spreadsheets/d/1WKxpcxZI_MMJJ5lqcwauhsuw3NBxwcG5/edit?usp=sharing&ouid=104702318722434350576&rtpof=true&sd=true) presents the data requirements
   2. This [Example Directory](https://github.com/datakind/new-america-housing-loss-public/tree/main/cli/collection/tests/resources) shows a populated version of the format with random addresses
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
    1. For Mac/Linux, run `python DKHousingLoss.py /path/to/input_data/`
    2. For Windows, run `py DKHousingLoss.py C:\path\to\input_data\`
11. The output will be available one level up from your data directory in a folder called `output_data`
    1. The `analysis_plots` directory contains time series and correlation analysis of your content
    2. The `data_summaries` directory contains a summary of evictions/foreclosures by geocode (enriched with American Community Survey (ACS) data)
    3. The `full_datasets` directory contains all eviction/foreclosure geocoded records
    4. The `mapping_data` directory contains a geopackage (.gpkg) file that can be examined using QGIS
12. __Optionally__ ....
    1. in addition to command line interface for defining input directory, a config file can be used. The config file name, be default, is _config.yaml_ and is located in same diretory as main script
    2. Supported config settings are : (__defaults__ in bold)
       1. logging_level: [CRITICAL, ERROR, WARNING, __INFO__, DEBUG]
       2. input:
          1. dir: _input directory name_ (same as cli)
          2. entered as nested dictionary, as shown here
          3. if command line includes directory name, cli overrides config file settings
       3. correlations : [__true__, false] - sets boolean to complete (or not) the correlation analysis plots
       4. mp_geocode: [__true__, false] - sets boolean to enable multi-processing of geo-coding operation

    
## Structure

* `cli/` - code to run the DataKind New America Housing Loss Analysis Tool
