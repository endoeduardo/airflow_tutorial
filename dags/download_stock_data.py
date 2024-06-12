"""Dag used for extract stock data"""
from pathlib import Path

from airflow.decorators import task, dag
from airflow.macros import ds_add
import pendulum

import yfinance as y

TICKERS = [
    "AAPL",
    "MSFT",
    "GOOG",
    "TSLA"
]

@task()
def get_stock_history(stock_ticker: str, ds=None, ds_nodash=None) -> None:
    """Main Function"""
    file_path = f'dags/stock/{stock_ticker}/{ds_nodash}.csv'
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

@dag(
    schedule = "0 0 * * 2-6",
    start_date = pendulum.datetime(2024, 6, 1, tz="UTC"),
    catchup = True
)
def get_stocks_dag():
    """Main dag"""
    for ticker in TICKERS:
        get_stock_history.override(task_id=ticker)(ticker)


dag = get_stocks_dag()
