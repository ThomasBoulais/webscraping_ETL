# ETL Project: Web Scraping and Data Transformation

## Overview
This goal of this project is to Extract, Transform, Load (ETL) data through webscraping with BS4, transform it with a python script, and load the processed data into a SQLite3 database.

## Requirements
Before running the project, ensure you have Python 3.x installed. The project dependencies are managed through `pyproject.toml`, which is the recommended configuration for modern Python projects.

To install the dependencies, use [Poetry](https://python-poetry.org/), a dependency manager for Python projects:

1. Install Poetry (if you don't have it already):
    ```bash
    curl -sSL https://install.python-poetry.org | python3 -
    ```

2. Install the project dependencies:
    ```bash
    poetry install
    ```

    This will automatically install the required dependencies as specified in the `pyproject.toml` file.

## Setup

1. Clone the repository:
    ```bash
    git clone git@github.com:ThomasBoulais/webscraping_ETL.git
    cd webscraping_ETL
    ```

2. Install the dependencies with Poetry:
    ```bash
    poetry install
    ```

3. Configure the SQLite3 database connection:
    - By default, the data will be stored in a file called `Banks.db`. You can change this by modifying the `db_config.py` file.

## How it Works

1. **Extract**: 
   - The `extract` function makes an HTTP request to the wikipedia bank page and retrieves the HTML content.
   - BeautifulSoup is used to parse the HTML and extract specific data -the largest banks table- and put it in a DataFrame.

2. **Transform**: 
   - The `transform` function takes the extracted data and cleans it up. This can involve:
     - Converting data types on USB_Billion
     - Adding the equivalent in EUR, GBP and INR
   - The data is then structured into a pandas DataFrame for easier manipulation.

3. **Load**: 
   - The `load_to_csv` and `load_to_db` functions takes the cleaned data from the DataFrame and loads it respectively into a CSV file and an SQLite3 database.
   - If the database or table does not exist, it will be created.

4. **SQLite Query**: 
   - The `run_query` function allows SQL queries on the Banks.db created on SQLite3.

5. **Log**: 
   - The `log_progress` function is called mutliple times throughout the steps to gather their status and timestamp.    

## Running the Project

To run the full ETL pipeline, simply execute the following command:

    ```bash
    poetry run python etl_pipeline.py
    ```