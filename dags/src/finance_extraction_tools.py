"""Scripts containing the functions for extracting, transforming and loading the data"""
import os
from pathlib import Path

import pandas as pd
from airflow.macros import ds_add
from airflow.decorators import task
import yfinance as y


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
    )

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
            data['date'] = file.replace('.csv', '')

            dataframes.append(data)

    # Concatanates all the dataframe
    joined_dataframe = pd.concat(dataframes)

    # Transforming the date column
    joined_dataframe['date'] = pd.to_datetime(joined_dataframe['date'], format='%Y%m%d')

    # Creates a temporary folder for storing as csv before uploading to a database
    path = 'dags/tmpf/stocks.csv'
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    joined_dataframe.to_csv(path, index=False)
