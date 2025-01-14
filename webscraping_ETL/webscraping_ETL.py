# Code for ETL operations on largest banks

### Importing the required libraries
import requests 
import sqlite3 
import pandas as pd 
from bs4 import BeautifulSoup 
from datetime import datetime 
import numpy as np

### Constants
URL = 'https://en.wikipedia.org/wiki/List_of_largest_banks'
EXCHANGE_RATE_CSV = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv'
TABLE_ATTRIBS_EXTRACT = ['Name', 'MC_USD_Billion']
TABLE_ATTRIBS_FINAL = ['Name', 'MC_USD_Billion', 'MC_GBP_Billion', 'MC_EUR_Billion', 'MC_INR_Billion']
OUTPUT_CSV_PATH = './Largest_banks_data.csv'
DATABASE_NAME = 'Banks.db'
TABLE_NAME = 'Largest_banks'
LOG_FILE = 'code_log.txt'

### Functions
def log_progress(message):
    # Code to write log message each time it's called
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format)
    with open(LOG_FILE,"a") as f: 
        f.write(timestamp + ' : ' + message + '\n')

def extract(URL, TABLE_ATTRIBS_EXTRACT):
    # Code to get the banks data from the URL (specifically the wikipedia world bank URL)
    page = requests.get(URL).text
    data = BeautifulSoup(page,'html.parser')
    df = pd.DataFrame(columns=TABLE_ATTRIBS_EXTRACT)
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            if col[0].contents[0] != 'Rank':
                # print('index: ', col[0].contents[0])
                # print('bank name: ', col[1].find_all('a')[1]['title'])
                # print('total assets: ', col[2].contents[0])
                
                data_dict = {
                    'Name' : col[1].find_all('a')[1]['title'],
                    'MC_USD_Billion' : col[2].contents[0]
                }
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df,df1], ignore_index=True)
    return df 

def transform(df, EXCHANGE_RATE_CSV):
    # Code to transform the currency into float and get their equivalent in euros, pounds and rupees
    df['MC_USD_Billion'] = (df['MC_USD_Billion']
                            .str.replace('\n','')
                            .str.replace(',','').astype(float))
    df_rate = pd.read_csv(EXCHANGE_RATE_CSV)
    # print(df_rate)
    rate_eur = df_rate[df_rate['Currency'] == 'EUR']['Rate'].iloc[0].astype(float)
    rate_gbp = df_rate[df_rate['Currency'] == 'GBP']['Rate'].iloc[0].astype(float)
    rate_inr = df_rate[df_rate['Currency'] == 'INR']['Rate'].iloc[0].astype(float)
    df['MC_EUR_Billion'] = np.round(rate_eur * df['MC_USD_Billion'], 2)
    df['MC_GBP_Billion'] = np.round(rate_gbp * df['MC_USD_Billion'], 2)
    df['MC_INR_Billion'] = np.round(rate_inr * df['MC_USD_Billion'], 2)
    return df
    
def load_to_csv(df, OUTPUT_CSV_PATH):
    # Code to load the dataframe into a csv file on the input path
    df.to_csv(OUTPUT_CSV_PATH)

def load_to_db(df, sql_connection, TABLE_NAME):
    # Code to load the dataframe into a sql database 
    df.to_sql(TABLE_NAME, sql_connection, if_exists = 'replace', index =False)

def run_query(query_statement, sql_connection):
    # Code to query the data from the sql database
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_statement)
    print(query_output)

log_progress('Preliminaries complete. Initiating ETL process.')

# Call extract() function
df = extract(URL, TABLE_ATTRIBS_EXTRACT)
log_progress('Data extraction complete. Initiating Transformation process.')

# Call transform() function
df = transform(df, EXCHANGE_RATE_CSV)
log_progress('Data transformation complete. Initiating loading process.')

# Call load_to_csv
load_to_csv(df, OUTPUT_CSV_PATH)
log_progress('Data saved to CSV file.')

# Initiate SQLite3 connection
sql_connection = sqlite3.connect(DATABASE_NAME)
log_progress('SQL Connection initiated.')

# Call load_to_db()
load_to_db(df, sql_connection, TABLE_NAME)
log_progress('Data loaded to Database as table. Running the query.')

# Call run_query()
run_query('SELECT * FROM Largest_banks', sql_connection)
print('---------------------------------------------------------')
run_query('SELECT AVG(MC_GBP_Billion) FROM Largest_banks', sql_connection)
print('---------------------------------------------------------')
run_query('SELECT Name from Largest_banks LIMIT 5', sql_connection)
log_progress('Process Complete.')

# Close SQLite3 connection
sql_connection.close()
log_progress('-')