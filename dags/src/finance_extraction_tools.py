"""Scripts containing the functions for extracting, transforming and loading the data"""
import os
from pathlib import Path

import pandas as pd
from airflow.macros import ds_add
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import yfinance as y

TIMESTAMP_FORMAT = '%Y%m%dT%H%M%S'

@task()
def get_stock_history(stock_ticker: str, ds=None, ds_nodash=None) -> None:
    """Fetches the stock history"""
    file_path = f'data/stock/{stock_ticker}/{ds_nodash}.csv'
    Path(file_path).parent.mkdir(parents=True, exist_ok=True)

    stock_data = y.Ticker(ticker=stock_ticker)
    hist = stock_data.history(
        period='1d',
        interval='1h',
        start=ds_add(ds, -1),
        end=ds,
        prepost=True
    ).reset_index()

    hist.to_csv(
        file_path,
        sep=',',
        index=False
    )


def list_files(directory: str) -> list[str, str]:
    """Lists all the files inside of a folder, returns the file list"""
    files = os.listdir(directory)
    return files


def treat_stock_data(**kwargs) -> None:
    """Treat the stock data scrapped from the API, it selects only the columns needed"""
    ti = kwargs['ti']
    tickers = kwargs['tickers']
    datasets = ti.xcom_pull(task_ids='list_files_task')
    
    dataframes = []
    for dataset, ticker in zip(datasets, tickers):
        for file in dataset:
            filepath = os.path.join('data/stock', ticker, file)
            data = pd.read_csv(filepath, index_col=False)
            data['ticker'] = ticker
            # data['date'] = file.replace('.csv', '')

            dataframes.append(data)

    # Concatanates all the dataframe
    joined_dataframe = pd.concat(dataframes)

    # Creates a temporary folder for storing as csv before uploading to a database
    path = 'dags/tmpf/stocks.csv'
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    joined_dataframe.to_csv(path, index=False)


def create_table() -> None:
    """Creates if not exists a table on Postgres in order to dump the stock data"""
    pg_hook = PostgresHook(
        postgres_conn_id='postgres-airflow-hook'
    )
    conn = pg_hook.get_conn()

    cursor = conn.cursor()

    query = """
    CREATE TABLE IF NOT EXISTS airflow_database.stock_data (
        open DECIMAL,
        high DECIMAL,
        low DECIMAL,
        close DECIMAL,
        volume INTEGER,
        dividends DECIMAL,
        stocks_splits INTEGER,
        ticker VARCHAR(20),
        datetime TIMESTAMPTZ
    );
    """

    cursor.execute(query=query)
    conn.commit()
    cursor.close()
    conn.close()


def load_data() -> None:
    """Uploads the data into a postgres db"""
    pg_hook = PostgresHook(
        postgres_conn_id='postgres-airflow-hook'
    )
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # Queries the last row timestamp value
    cursor.execute("SELECT max(datetime) FROM airflow_database.stock_data;")
    last_timestamp = cursor.fetchone()[0]

    print(last_timestamp)
    
    # Make sure that only new data will be uploaded to the db
    data = pd.read_csv('dags/tmpf/stocks.csv')
    if last_timestamp:
        data = data.query(f'Datetime > "{last_timestamp}"')

    for _, row in data.iterrows():
        values = (
            row['Open'],
            row['High'],
            row['Low'],
            row['Close'],
            row['Volume'],
            row['Dividends'],
            row['Stock Splits'],
            row['ticker'],
            row['Datetime'],
        )
        query = """
        INSERT INTO airflow_database.stock_data 
            ( 
                open,
                high,
                low,
                close,
                volume,
                dividends,
                stocks_splits,
                ticker,
                datetime
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(query=query, vars=values)
        conn.commit()
    cursor.close()
    conn.close()
