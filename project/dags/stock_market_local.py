from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.sensors.base import PokeReturnValue
from airflow.operators.python import PythonOperator
from datetime import datetime

from include.stock_market.tasks_local import _get_stock_prices, _store_prices,format_stock_data , _get_formatted_csv,load_to_postgres ,LOCAL_DIRECTORY

SYMBOL = 'NQ=F'

   
@dag(
    start_date=datetime(2023, 1, 1),
    schedule='@daily', #3m
    catchup=False,
    tags=['stock_market'],
)
def stock_market_local():
    
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
    op_kwargs={'url': '{{ ti.xcom_pull(task_ids="is_api_available") }}', 'symbol': SYMBOL},
    do_xcom_push=True
    )

    
    store_prices = PythonOperator(
        task_id='store_prices',
        python_callable=_store_prices,
        op_kwargs={'stock': '{{ ti.xcom_pull(task_ids="get_stock_prices") }}'}
    )
    #PythonOperator
    format_prices = PythonOperator(
    task_id='format_prices',
    python_callable=format_stock_data,
    op_kwargs={'stock_data': '{{ ti.xcom_pull(task_ids="store_prices") }}'}
    )
    
    get_formatted_csv = PythonOperator(
    task_id='get_formatted_csv',
    python_callable=_get_formatted_csv,
    op_kwargs={
        'path': '{{ ti.xcom_pull(task_ids="format_prices") }}'
    }
    )

    load_to_dw = PythonOperator(
        task_id='load_to_dw',
        python_callable=load_to_postgres,
        op_kwargs={'file_path': '{{ ti.xcom_pull(task_ids="get_formatted_csv") }}'}
    )

    is_api_available() >> get_stock_prices >> store_prices >> format_prices >> get_formatted_csv >> load_to_dw
    
stock_market_local()
