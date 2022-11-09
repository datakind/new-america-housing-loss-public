# DataKind New America Housing Loss Analysis Tool #

A Command Line Interface (CLI) that allows users to ingest eviction, 
foreclosure, and tax lien data and outputs statistical summaries and 
geolocation data.

# Setup 

Get the FEAT code ...

1. Navigate to the New America Housing Loss Tool here: https://github.com/datakind/new-america-housing-loss-public.git
2. Click `Code` and `Download Zip` to download the code
3. Unzip the zip file in the directory of your choice

Then setup the FEAT environment ...

1. Install [miniconda](https://docs.conda.io/en/latest/miniconda.html) by selecting the installer that fits your OS version.
2. Build your environment:
   - Open a terminal
     - On Mac/Linux use the Terminal app
     - On Windows run the program 'Anaconda prompt'
   - Change directory to be where you unzipped the FEAT code, `new-america-housing-loss-public`
     - `cd <DIRECTORY WHERE FEAT IS>`
   - Run command: `conda env create -f environment.yml` (if you need to rebuild, use `conda env update -f environment.yml`) 

# Running FEAT

First, create your input data ...

1. Download [this spreadsheet](https://docs.google.com/spreadsheets/d/1WKxpcxZI_MMJJ5lqcwauhsuw3NBxwcG5/edit?usp=sharing&ouid=106808949113099347741&rtpof=true&sd=true)
2. In the sheet populate tabs `Evictions`, `Mortgage Foreclosures` and `Tax Lien Foreclosures` with your data. Note, some tabs can be empty if you don't have data
4. Save each tab as a csv file into the FEAT folders as follows:
    - `evictions.csv` should be saved to `new-america-housing-loss-public/cli/work/evictions`
    - `mortgage_foreclosures.csv` should be saved to `new-america-housing-loss-public/cli/work/mortgage_foreclosures`
    - `tax_lien_foreclosures.csv` should be saved to `new-america-housing-loss-public/cli/work/tax_lien_foreclosures` 

Note: You don't have to have all three files, but must have at least one.

Then, run FEAT ...

1. Open a terminal
   - On Mac/Linux use the Terminal app
   - On Windows run the program 'Anaconda prompt'
2. Run command: `conda activate housing_loss_env`
3. Change directory to be where you unzipped the FEAT code, `new-america-housing-loss-public`
   - `cd <DIRECTORY WHERE FEAT IS>`
4. `cd cli/work`
5. Run command: `python ../load_data.py .`
    
The output will be available in a folder called `output_data`

- The `analysis_plots` directory contains time series and correlation analysis of your content
- The `data_summaries` directory contains a summary of evictions/foreclosures by geocode (enriched with American Community Survey (ACS) data)
- The `full_datasets` directory contains all eviction/foreclosure geocoded records
- The `mapping_data` directory contains a geopackage (.gpkg) file that can be examined using QGIS

