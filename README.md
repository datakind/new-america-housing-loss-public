# DataKind New America Housing Loss Analysis Tool #

A Command Line Interface (CLI) that allows users to ingest eviction, 
foreclosure, and tax lien data and outputs statistical summaries and 
geolocation data. 

## Docker Usage Instructions
1. Ensure your data format is configured based on the specification in this [Google Sheet](https://docs.google.com/spreadsheets/d/1WKxpcxZI_MMJJ5lqcwauhsuw3NBxwcG5/edit?usp=sharing&ouid=104702318722434350576&rtpof=true&sd=true)
2. Navigate to the Docker Desktop website and install Docker here: https://docs.docker.com/get-docker/
3. Click the installation link to install Docker for your Operating System
4. Run the `Docker Desktop Installer` **as an administrator** with the default settings (installation will take several minutes)
5. Click `Close and Restart` to complete the installation
6. Open Docker and Accept the Terms of Service, Docker should now be successfully installed.
7. Open a terminal (Mac/Unix) or command prompt (Windows)
8. Run the command `docker -v` to ensure Docker is installed correctly
9. `docker run -v "/path/to/data":/app/data dkemily/new-america-housing-loss-public:cli-tool` (where `/path/to/data` is the absolute path to your directory)
   1. In Mac/Linux, you will use a command similar to `docker run -v "/Users/my_user/data/test":/app/data dkemily/new-america-housing-loss-public:cli-tool`
   2. In Windows, you will use a command similar to `docker run -v "C:\path\to\data":/app/data dkemily/new-america-housing-loss-public:cli-tool`

## Python CLI Usage Instructions
1. Ensure your data format is configured based on the specification in this [Google Sheet](https://docs.google.com/spreadsheets/d/1WKxpcxZI_MMJJ5lqcwauhsuw3NBxwcG5/edit?usp=sharing&ouid=104702318722434350576&rtpof=true&sd=true)
2. Download Python 3.8.10 here: https://www.python.org/downloads/release/python-3810/ and follow the instructions to install Python
3. Navigate to the New America Housing Loss Tool here: https://github.com/datakind/new-america-housing-loss-public.git
4. Click `Code` and `Download Zip` to download the code
5. Unzip the zip file in the directory of your choice
6. Open a terminal or a command prompt and navigate to where the code was downloaded (note: this directory will end with `new-america-housing-loss-public-main`)
7. Run the following commands to install dependencies:
   1. For Mac/Linux, run `python -m pip install -r requirements.txt`
   2. For Windows, run `py -m pip install -r requirements.txt`
8. Run the tool against your data:
   1. For Mac/Linux, run `python collection/load_data.py /path/to/data/`
   2. For Windows, run `py collection/load_data.py C:\path\to\data\`

## Structure

* `collection/` - code to handle data ingestion and enrichment
* `analysis/` - code to handle statistical analysis and visualization
