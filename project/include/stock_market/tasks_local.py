from airflow.exceptions import AirflowNotFoundException
from airflow.hooks.base import BaseHook
from datetime import datetime
import pandas as pd
import json
import csv
import os
import psycopg2

LOCAL_DIRECTORY = './include/data/stock_prices'
SYMBOL = 'NQ=F'

def _get_stock_prices(url, symbol):
    import requests
    url = f"{url}{symbol}?period1=946684800&period2=1725667200&interval=1d&events=history"
    api = BaseHook.get_connection('stock_api')
    response = requests.get(url, headers=api.extra_dejson['headers'])
    return json.dumps(response.json()['chart']['result'][0])

def _store_prices(stock):
    stock = json.loads(stock)
    file_path = os.path.join(LOCAL_DIRECTORY, f"prices.json")
    
    with open(file_path, 'w') as f:
        json.dump(stock, f, ensure_ascii=False)
    
    return file_path

def format_stock_data(stock_data):

    with open(stock_data, 'r') as f:
        result = json.load(f)

    timestamps = result['timestamp']
    indicators = result['indicators']['quote'][0]

    dates = [datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d') for ts in timestamps]
    df = pd.DataFrame({
        'date': dates,
        'timestamp': timestamps,
        'open': indicators['open'],
        'high': indicators['high'],
        'low': indicators['low'],
        'close': indicators['close'],
        'volume': indicators['volume']
    })

    formatted_prices_csv = os.path.join(LOCAL_DIRECTORY, 'formatted_prices.csv')
    df.to_csv(formatted_prices_csv, index=False)

    return formatted_prices_csv


def _get_formatted_csv(path):

    if os.path.exists(path):
        return path
    else:
        raise AirflowNotFoundException("The CSV file does not exist.")
    
def load_to_postgres(file_path): 
    conn = psycopg2.connect(
        dbname='postgres',
        user='postgres',
        password='postgres',
        host='postgres',
        port='5432'
    )
    table_name = "new_stock_market"
    
    with open(file_path, 'r') as fprice_csv:
        cursor = conn.cursor()
        cursor.copy_expert(f"COPY {table_name} FROM stdin WITH CSV HEADER DELIMITER ','", fprice_csv)
        
    conn.commit()
    print(f"Data loaded successfully {table_name} from {file_path}")
    cursor.close()
    conn.close()