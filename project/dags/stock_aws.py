from airflow.operators.python import PythonOperator
from airflow.sensors.base import PokeReturnValue
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from datetime import datetime

from include.stock_market.task_aws import _get_stock_prices, _store_prices,format_stock_data , _get_formatted_csv,load_s3_csv_to_postgres, BUCKET_NAME

SYMBOL = 'NQ=F'

@dag(
    start_date=datetime(2023, 1, 1),
    schedule='@daily', #3m
    catchup=False,
    tags=['stock_market'],

)
def stock_market_aws():
    
    @task.sensor(poke_interval=30, timeout=300, mode='poke')
    def is_api_available() -> PokeReturnValue:
        import requests
        api = BaseHook.get_connection('stock_api')
        url = f"{api.host}{api.extra_dejson['endpoint']}"
        print(url)
        response = requests.get(url, headers=api.extra_dejson['headers'])
        condition = response.json()['finance']['result'] is None
        return PokeReturnValue(is_done=condition, xcom_value=url)
    
    get_stock_prices = PythonOperator(
        task_id='get_stock_prices',
        python_callable=_get_stock_prices,
        op_kwargs={'url': '{{ ti.xcom_pull(task_ids="is_api_available") }}', 'symbol': SYMBOL}
    )
    
    store_prices = PythonOperator(
        task_id='store_prices',
        python_callable=_store_prices,
        op_kwargs={'stock': '{{ ti.xcom_pull(task_ids="get_stock_prices") }}'}
    )
    
    format_prices = PythonOperator(
    task_id='format_prices',
    python_callable=format_stock_data,
    op_kwargs={'stock_data': '{{ ti.xcom_pull(task_ids="store_prices") }}'}
    )
    
    get_formatted_csv = PythonOperator(
        task_id='get_formatted_csv',
        python_callable=_get_formatted_csv,
        op_kwargs={
            'path': '{{ ti.xcom_pull(task_ids="store_prices") }}'
        }
    )
    
    load_to_dw = PythonOperator(
    task_id='load_to_dw',
    python_callable=load_s3_csv_to_postgres,
    op_kwargs={
        's3_key': 'mydata/formatted_prices.csv',
        'bucket_name': BUCKET_NAME,
        'table_name': 'stock_market_aws'
    }
)

    is_api_available() >> get_stock_prices >> store_prices >> format_prices >> get_formatted_csv >> load_to_dw
    
stock_market_aws()