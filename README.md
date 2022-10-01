# DataKind New America Housing Loss Analysis Tool #

A Command Line Interface (CLI) that allows users to ingest eviction, 
foreclosure, and tax lien data and outputs statistical summaries and 
geolocation data.

# Setup 

Get the FEAT code ...

1. Navigate to the New America Housing Loss Tool here: https://github.com/datakind/new-america-housing-loss-public.git
2. Click `Code` and `Download Zip` to download the code
3. Unzip the zip file in the directory of your choice

Then setup the FEAT environment

1Install [miniconda](https://docs.conda.io/en/latest/miniconda.html) by selecting the installer that fits your OS version. Once it is installed you may have to restart your terminal (closing your terminal and opening again)
2Build your environment:
   - On Mac/Linux: 
     - Open a terminal and change directory to be where you unzipped the FEAT code `new-america-housing-loss-public`
     - `conda env create -f environment.yml` 
   - On Windows: 
     - In the search box bottom right, search for 'Anaconda prompt' and start it
     - Change directory to be where you unzipped the FEAT code `new-america-housing-loss-public`
       - `conda env create -f environment_win.yml`

# Running FEAT

First, create your input data ...

1. Download [this spreadsheet](https://docs.google.com/spreadsheets/d/1WKxpcxZI_MMJJ5lqcwauhsuw3NBxwcG5/edit?usp=sharing&ouid=106808949113099347741&rtpof=true&sd=true)
2. In the sheet populate tabs `Evictions`, `Mortgage Foreclosures` and `Tax Lien Foreclosures` with your data. Note, some tabs can be empty if you don't have data
3. Save each tab as a csv file into the FEAT folders as follows:
    - `evictions.csv` should be saved to `new-america-housing-loss-public/cli/work/evictions`
    - `mortgage_foreclosures.csv` should be saved to `new-america-housing-loss-public/cli/work/mortgage_foreclosures`
    - `tax_lien_foreclosures.csv` should be saved to `new-america-housing-loss-public/cli/work/tax_lien_foreclosures` 

Then, run FEAT ...

1. Open a terminal and cd into the directory `new-america-housing-loss-public/cli/`
2. Activate your environment: `conda activate housing_loss_env`
3. Run the FEAT with your data:
    1. For Mac/Linux, run `python load_data.py ./work`
    2. For Windows, run `py load_data.py ./work`
    
The output will be available one level up from your data directory in a folder called `output_data`

- The `analysis_plots` directory contains time series and correlation analysis of your content
- The `data_summaries` directory contains a summary of evictions/foreclosures by geocode (enriched with American Community Survey (ACS) data)
- The `full_datasets` directory contains all eviction/foreclosure geocoded records
- The `mapping_data` directory contains a geopackage (.gpkg) file that can be examined using QGIS

