from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.base import BaseHook
from datetime import datetime
from io import StringIO
import pandas as pd
import psycopg2
import json
import csv



def _get_s3_hook():
    return S3Hook(aws_conn_id='aws_s3')

hook = _get_s3_hook()

BUCKET_NAME = 'my-stock-market'
SYMBOL = 'NQ=F'

def _get_stock_prices(url, symbol):
    import requests
    url = f"{url}{symbol}?period1=946684800&period2=1725667200&interval=1d&events=history"
    api = BaseHook.get_connection('stock_api')
    response = requests.get(url, headers=api.extra_dejson['headers'])
    return json.dumps(response.json()['chart']['result'][0])


def _store_prices(stock):
    stock = json.loads(stock)
    data = json.dumps(stock, ensure_ascii=False).encode('utf8')
    
    key = f"mydata/prices.json"

    hook.load_bytes(
        bytes_data=data,
        key=key,
        bucket_name=BUCKET_NAME,
        replace=True
    )
    return f'{BUCKET_NAME}/{key}'


def format_stock_data(stock_data):

    bucket_name, key = stock_data.split("/", 1)

    json_file = hook.read_key(key=key, bucket_name=bucket_name)
    result = json.loads(json_file)

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

    csv_file = StringIO()
    df.to_csv(csv_file, index=False)
    csv_data = csv_file.getvalue()

    formatted_csv = f"mydata/formatted_prices.csv"

    hook.load_bytes(
        bytes_data=csv_data.encode('utf-8'),
        key=formatted_csv,
        bucket_name=bucket_name,
        replace=True
    )

    return f'{bucket_name}/{formatted_csv}'


def _get_formatted_csv(path):
    formatted_price_csv = "mydata/formatted_prices.csv"
    keys = hook.list_keys(bucket_name=BUCKET_NAME, prefix=formatted_price_csv)

    if keys:
        return keys[0]
    else:
        raise print(f'The CSV file {formatted_price_csv} does not exist in bucket {BUCKET_NAME}')


def load_s3_csv_to_postgres(s3_key, bucket_name, table_name):
    
    conn = psycopg2.connect(
        dbname='postgres',
        user='postgres',
        password='postgres',
        host='postgres',
        port='5432'
    )
    cursor = conn.cursor()
    csv_content = hook.read_key(key=s3_key, bucket_name=bucket_name)

    cursor.copy_expert(
        f"COPY {table_name} FROM STDIN WITH CSV HEADER DELIMITER ','",
        StringIO(csv_content)
    )

    conn.commit()
    cursor.close()
    conn.close()
    print(f"Data loaded successfully into {table_name}")