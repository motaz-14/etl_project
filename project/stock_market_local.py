# from airflow.decorators import dag, task
# from airflow.hooks.base import BaseHook
# from airflow.sensors.base import PokeReturnValue
# from airflow.operators.python import PythonOperator
# from datetime import datetime
# import psycopg2
# import csv
# from psycopg2 import sql

# from include.stock_market.tasks import _get_stock_prices, _store_prices,format_stock_data , _get_formatted_csv ,LOCAL_DIRECTORY

# SYMBOL = 'NVDA'

# # Function to load data into PostgreSQL using psycopg2
# def load_to_postgres(file_path: str):
#     # Database connection parameters
#     conn = psycopg2.connect(
#         dbname='postgres',
#         user='postgres',
#         password='postgres',
#         host='postgres',
#         port='5432'
#     )
#     table_name = "new_stock_market"  # The table you are loading data into
#     try:
#         with open(file_path, 'r') as f:
#             cursor = conn.cursor()
#             # Directly load CSV into the stock_market table
#             cursor.copy_expert(f"COPY {table_name} FROM stdin WITH CSV HEADER DELIMITER ','", f)
        
#         conn.commit()
#         print(f"Data loaded successfully {table_name} from {file_path}")
    
#     except Exception as e:
#         print(f"Error occurred while loading data {table_name}: {e}")
#         conn.rollback()
    
#     finally:
#         cursor.close()
#         conn.close()
    
# @dag(
#     start_date=datetime(2023, 1, 1),
#     schedule='@daily', #3m
#     catchup=False,
#     tags=['stock_market'],

# )
# def stock_market():
    
#     @task.sensor(poke_interval=30, timeout=300, mode='poke')
#     def is_api_available() -> PokeReturnValue:
#         import requests
#         api = BaseHook.get_connection('stock_api')
#         url = f"{api.host}{api.extra_dejson['endpoint']}"
#         print(url)
#         response = requests.get(url, headers=api.extra_dejson['headers'])
#         condition = response.json()['finance']['result'] is None
#         return PokeReturnValue(is_done=condition, xcom_value=url)
    
#     get_stock_prices = PythonOperator(
#     task_id='get_stock_prices',
#     python_callable=_get_stock_prices,
#     op_kwargs={'url': '{{ ti.xcom_pull(task_ids="is_api_available") }}', 'symbol': SYMBOL},
#     do_xcom_push=True
#     )

    
#     store_prices = PythonOperator(
#         task_id='store_prices',
#         python_callable=_store_prices,
#         op_kwargs={'stock': '{{ ti.xcom_pull(task_ids="get_stock_prices") }}'}
#     )
#     #PythonOperator
#     format_prices = PythonOperator(
#     task_id='format_prices',
#     python_callable=format_stock_data,
#     op_kwargs={'stock_data': '{{ ti.xcom_pull(task_ids="store_prices") }}'}
#     )
    
#     get_formatted_csv = PythonOperator(
#     task_id='get_formatted_csv',
#     python_callable=_get_formatted_csv,
#     op_kwargs={
#         'path': '{{ ti.xcom_pull(task_ids="format_prices") }}'
#     }
#     )

#     #delete
#     # Load data into PostgreSQL using the PythonOperator and psycopg2
#     load_to_dw = PythonOperator(
#         task_id='load_to_dw',
#         python_callable=load_to_postgres,
#         op_kwargs={'file_path': '{{ ti.xcom_pull(task_ids="get_formatted_csv") }}'}
#     )

#     #delete
#     is_api_available() >> get_stock_prices >> store_prices >> format_prices >> get_formatted_csv >> load_to_dw
    
# stock_market()

# from airflow.hooks.base import BaseHook
# import json
# import os
# from airflow.exceptions import AirflowNotFoundException
# import csv
# from datetime import datetime

# LOCAL_DIRECTORY = './include/data/stock_prices'
# SYMBOL = 'NQ=F'

# def _get_stock_prices(url, symbol):
#     import requests
#     url = f"{url}{symbol}?period1=946684800&period2=1725667200&interval=1d&events=history"
#     api = BaseHook.get_connection('stock_api')
#     response = requests.get(url, headers=api.extra_dejson['headers'])
#     return json.dumps(response.json()['chart']['result'][0])

# def _store_prices(stock):
#     stock = json.loads(stock)
#     file_path = os.path.join(LOCAL_DIRECTORY, f"prices.json")
    
#     # Ensure the directory exists
#     os.makedirs(os.path.dirname(file_path), exist_ok=True)

#     # Write the JSON data to a file
#     with open(file_path, 'w') as f:
#         json.dump(stock, f, ensure_ascii=False)
    
#     return file_path

# import os
# import json
# import pandas as pd

# def format_stock_data(stock_data: str) -> str:
#     import pandas as pd

#     with open(stock_data, 'r') as f:
#         result = json.load(f)  # This is already 'result[0]'

#     timestamps = result['timestamp']
#     indicators = result['indicators']['quote'][0]  # this includes open, high, low, close, volume
#     # Convert timestamps to dates
#     dates = [datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d') for ts in timestamps]
#     df = pd.DataFrame({
#         'date': dates,
#         'timestamp': timestamps,
#         'open': indicators['open'],
#         'high': indicators['high'],
#         'low': indicators['low'],
#         'close': indicators['close'],
#         'volume': indicators['volume']
#     })

#     out_path = os.path.join(LOCAL_DIRECTORY, 'formatted_prices.csv')
#     os.makedirs(os.path.dirname(out_path), exist_ok=True)
#     df.to_csv(out_path, index=False)

#     return out_path





# def _get_formatted_csv(path):
#     # Check if the formatted file exists locally
#     if os.path.exists(path):
#         return path
#     else:
#         raise AirflowNotFoundException("The CSV file does not exist.")
