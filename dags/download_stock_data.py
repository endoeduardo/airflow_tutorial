"""Dag used for extract stock data"""

from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import pendulum

from src.finance_extraction_tools import (
    list_files, treat_stock_data, get_stock_history,
    create_table, load_data
)

TICKERS = [
    "AAPL",
    "MSFT",
    "GOOG",
    "TSLA"
]

list_files_task = PythonOperator(
    task_id='list_files_task',
    python_callable=lambda tickers: [list_files('data/stock/' + ticker + '/') for ticker in tickers],
    op_args=[TICKERS]
)

treatment_task = PythonOperator(
    task_id='treatment_task',
    python_callable=treat_stock_data,
    op_kwargs={'tickers': TICKERS},
    provide_context=True
)

remove_raw_folder_task = BashOperator(
    task_id='remove_raw_data_folder_task',
    bash_command='rm -rf /opt/airflow/data'
)

create_table_task = PythonOperator(
    task_id='create_storage_table',
    python_callable=create_table
)

load_data_into_postgres = PythonOperator(
    task_id='load_data_into_postgres',
    python_callable=load_data
)

deleting_tmpf = BashOperator(
    task_id='removing_tmpf_folder',
    bash_command='rm -rf /dags/tmpf'
)

@dag(
    schedule = "0 0 * * 2-6",
    start_date = pendulum.datetime(2024, 6, 1, tz="UTC"),
    catchup = True
)
def get_stocks_dag():
    """Main dag"""

    # pylint: disable=W0106:expression-not-assigned
    [
        # This part can be switched by a step that dumps into a bucket on a cloud service
        get_stock_history.override(task_id=ticker)(ticker) for ticker in TICKERS
    ] >> list_files_task >> treatment_task >> remove_raw_folder_task >> \
    create_table_task >> load_data_into_postgres
    # pylint: enable=W0106:expression-not-assigned

dag = get_stocks_dag() # pylint: disable=C0103
