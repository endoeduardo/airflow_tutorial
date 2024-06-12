"""fetch stock data"""
from datetime import datetime
import yfinance as y
# import pandas as pd

def main(stock_ticker: str):
    """Main Function"""
    current_date = datetime.now().strftime("%Y-%m-%d_%H-%M")
    stock_data = y.Ticker(ticker=stock_ticker)
    hist = stock_data.history(
        period='1d',
        interval='1h',
        start='2024-06-11',
        end='2024-06-12',
        prepost=True
    )
    print(type(current_date))

    filepath = f'src/data/{stock_ticker}/{current_date}.csv'
    hist.to_csv(
        filepath,
        sep=',',
        index=False
    )


if __name__ == "__main__":
    main("AAPL")
