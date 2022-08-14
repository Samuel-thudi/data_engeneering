# importing libraries
import pandas as pd
import requests
import json 
import sqlite3
import os  

# API key for polygon.io, an API which has stock data
api_key = 'TQk1jrdtvpxdnqmTbdPA6_y4Hzy5GxxG'

# the url for the api to connect to 
url = 'https://api.polygon.io/v3/reference/tickers'

# the parameters that define what data is retrieved
parameters = {
    'apiKey': api_key, # your API key
    'type': 'CS', # query common stocks
    'market': 'stocks',
    'limit': 100 
}

# creating a function to read the data and return a pandas dataframe

def get_data(url,parameters): 
    # using requests.get() function to get the data using the url and parameters as json fomrat
    tickers_json = requests.get(url, parameters).json()

    # copying the results into a list 
    result = tickers_json['results']

    # converting the list to a dataframe
    data = pd.DataFrame(result)

    # keeping only the columsn we require
    req_data = data[['ticker','name','primary_exchange','currency_name','cik']]
    
    return req_data

def write_to_db(data): 
    # creating a sql database
    conn = sqlite3.connect('data_eng.db') 

    # cursor acts as a middleware between python and the sql database
    c = conn.cursor()

    stock = 'stock'

    # creating a table with required columns
    c.execute(f'CREATE TABLE IF NOT EXISTS {stock} ([ticker] text primary key, name text, primary_exchange text,currency_name text, cik number)')

    # commiting/confirming the changes made using above step
    conn.commit()

    # writing our database to the table 'stock'
    data.to_sql('stock', conn, if_exists='replace',index = True)
    
    print('Done')
    
# function to save data to csv
def write_to_csv(data,loc=None): 
    
#     creating an if else ladder to save csv either to default location or required location
    if loc is None: 
        data.to_csv('out.csv')
        print('saved in current working directory')
    elif loc is not None: 
        name = 'out.csv'
        directory = os.path.join(loc,name)
        data.to_csv(directory)
        print('saved in specified location')
    else: 
        pass


req_data = get_data(url,parameters)

write_to_db(data=req_data)
    
write_to_csv(data=req_data)


# connecting to db and creating a cursor
conn = sqlite3.connect('data_eng.db') 
c = conn.cursor()

# checking if the data is written to db 
c.execute('''  
SELECT * FROM stock
          ''')

# displaying the first 5 rows of the database
count = 0
for i in c.fetchall():
    while count < 5: 
        print(i)
        count+=1