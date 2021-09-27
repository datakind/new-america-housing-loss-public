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
7. Run the following commands to install dependencies:
   1. For Mac/Linux, run `python -m pip install -r requirements.txt`
   2. For Windows, run `py -m pip install -r requirements.txt`
8. Run the tool against your data:
   1. For Mac/Linux, run `python collection/load_data.py /path/to/data/`
   2. For Windows, run `py collection/load_data.py C:\path\to\data\`

## Docker Usage Instructions
1. Ensure your data format is configured based on the specification in this [Google Sheet](https://docs.google.com/spreadsheets/d/1WKxpcxZI_MMJJ5lqcwauhsuw3NBxwcG5/edit?usp=sharing&ouid=104702318722434350576&rtpof=true&sd=true)
2. Navigate to the Docker Desktop website and install Docker here: https://docs.docker.com/get-docker/
3. Click the installation link to install Docker for your Operating System
4. Run the `Docker Desktop Installer` **as an administrator** with the default settings (installation will take several minutes)
5. Click `Close and Restart` to complete the installation
6. Open Docker and Accept the Terms of Service, Docker should now be successfully installed.
7. Open a terminal (Mac/Unix) or command prompt (Windows)
8. `docker run -v "/path/to/data":/app/data dkemily/new-america-housing-loss-public:cli-tool` (where `/path/to/data` is the absolute path to your directory)
   1. In Mac/Linux, you will use a command similar to `docker run -v "/Users/my_user/data/test":/app/data dkemily/new-america-housing-loss-public:cli-tool`
   2. In Windows, you will use a command similar to `docker run -v "C:\path\to\data":/app/data dkemily/new-america-housing-loss-public:cli-tool`

## Automated GitHub access

[Create a token](https://github.com/settings/tokens) on GitHub with repository privileges.
Make sure to check the `read:packages` permission -- this will be needed for shared Vue packages later on. You may need to check the `repo` scope.

Create [`~/.netrc`](https://www.ibm.com/support/knowledgecenter/en/ssw_aix_71/filesreference/netrc.html) if it doesn't exist:
```bash
touch ~/.netrc
chmod 600 ~/.netrc
```

Then add this to it:

    machine github.com login TOKEN_YOU_JUST_MADE

## Structure

* `collection/` - code to handle data ingestion and enrichment
* `analysis/` - code to handle statistical analysis and visualization

## MacOS environment configuration
```bash
# Install brew
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# Install python3
brew install python3
export PATH=/usr/local/share/python:$PATH

# Install virtualenv
pip3 install virtualenv virtualenvwrapper

# Note: if you are using anaconda, you may need to search for virtualenvwrapper.sh
# Start by querying for `which python3`, then run `find . | grep virtualenv`

# Add these commands: to your ~/.bash_profile or ~/.zshrc
export PATH=/usr/local/share/python:$PATH
export WORKON_HOME=$HOME/.virtualenvs
export VIRTUALENVWRAPPER_PYTHON=/usr/local/bin/python3
export VIRTUALENVWRAPPER_VIRTUALENV=/usr/local/bin/virtualenv
source /usr/local/bin/virtualenvwrapper.sh
```

## Development
```bash
git clone https://github.com/datakind/new-america-housing-loss-tool
cd new-america-housing-loss-tool/cli
mkvirtualenv -p $(which python3) -a $PWD new-america-housing-loss
pip install --upgrade pip
make requirements
make test
```

## Creating a branch
To create a branch in Github, first run the instructions above to install the package,
then follow the procedure below to create and push a branch to the repository.
```bash
git pull origin main  # Pull the latest changes from the repository
git checkout -b my-branch main  # Create a new branch
# Make changes to python files and create tests as needed
cd new-america-housing-loss-tool/cli  # Ensure this directory is active
make format  # Run automation to automate formatting changes
make test  # Run automated tests and look for errors
git commit -m "description of your changes" your/file.py  # Commit local changes
git push origin my-branch  # Push local changes to the repository
```
