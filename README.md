# cli #

A Command Line Interface (CLI) that allows users to ingest eviction, 
foreclosure, and tax lien data and outputs statistical summaries and 
geolocation data. 

## Python CLI Usage Instructions
1. Download Python: https://www.python.org/downloads/ and follow the instructions to install Python
2. Navigate to the New America Housing Loss Tool here: https://github.com/datakind/new-america-housing-loss-public.git
3. Click `Code` and `Download Zip` to download the code
4. Open a terminal or a command prompt and navigate to where the code was downloaded
5. Type `pip install -r requirements.txt` to install the tool's dependencies
6. Type `python -m cli /path/to/data` to run the tool against your data

## Docker Usage Instructions
1. Download Docker Desktop for Windows here: https://docs.docker.com/desktop/windows/install/
2. Run the `Docker Desktop Installer` **as an administrator** with the default settings (installation will take several minutes) (administrative privileges are defined [here](https://forums.docker.com/t/solved-docker-failed-to-start-docker-desktop-for-windows/106976))
3. Click `Close and Restart` to complete the installation
4. Open Docker and Accept the Terms of Service, Docker should now be successfully installed.
5. In the command prompt, type in `docker pull dkemily/datakind-new-america-dev:new_housing_test` to pull the New Housing Tool
6. In Docker Hub, click `Run`
7. Under the `Volumes` tab, navigate to your data directory and add it to the `Host Path`
8. Under the `Container Path` type in `/data`
9. Click `Run` to process your data

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
