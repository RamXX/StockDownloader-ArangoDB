import pandas as pd 
from arango import ArangoClient
import yfinance as yf
from collections import defaultdict
from datetime import datetime
import pandas_market_calendars as mcal
import warnings
import os
import pytz
from dotenv import load_dotenv

warnings.simplefilter(action='ignore')

# Constants
BEGINNING_DATE = '2015-01-01' # Earliest date used for downloads
STOCKS_COLLECTION='all_stocks' # Name of the collection holding all stocks

# Global variables
# today = pytz.UTC.localize(pd.Timestamp.now()).strftime('%Y-%m-%d')
nyse = mcal.get_calendar('NYSE') # NYSE calendar

def last_trading_day(nyse):
    """
    Returns the last completed trading date for NYSE
    """
    today_utc_naive = datetime.utcnow()
    
    first_day_of_year = datetime(today_utc_naive.year, 1, 1)
    valid_days = nyse.valid_days(start_date=first_day_of_year, end_date=today_utc_naive)
    
    schedule_today = nyse.schedule(start_date=valid_days[-1], end_date=valid_days[-1])
    
    market_close_today_utc = schedule_today.iloc[0]['market_close']
    if pd.Timestamp(datetime.utcnow(), tz='UTC') > market_close_today_utc:
        return market_close_today_utc
    else:
        schedule_prev_day = nyse.schedule(start_date=valid_days[-2], end_date=valid_days[-2])
        market_close_prev_day_utc = schedule_prev_day.iloc[0]['market_close']
        return market_close_prev_day_utc


def next_trading_day(nyse, date_str):
    """ 
    Returns the next valid trading date for a given date.
    """
    future_dates = pd.date_range(start=date_str, periods=10, freq='B')
    trading_days = nyse.valid_days(start_date=future_dates[0], end_date=future_dates[-1])    
    input_date_tz_aware = pd.Timestamp(date_str).tz_localize('UTC')    
    next_day = trading_days[trading_days > input_date_tz_aware].min()    
    return next_day


def init_db():
    """
    Initializes the ArangoDB Database and returns a db and the collection ready to work with.
    """
    load_dotenv()

    adbhost = os.environ["ADBHOST"]
    adbuser = os.environ["ADBUSER"]
    adbpw = os.environ["ADBPW"]
    adbport = os.environ["ADBPORT"]
    dbname = os.environ["ADBNAME"]
    adbroot = os.environ["ADBROOT"]
    adbrootpw = os.environ["ADBROOTPW"]

    client = ArangoClient(hosts=f'http://{adbhost}:{adbport}')

    sys_db = client.db('_system', username=adbroot, password=adbrootpw)
    if not sys_db.has_database(dbname):
        sys_db.create_database(dbname)
        print(f'Created database {dbname}.')
    else:
        print(f'Using database: {dbname}')

    if not sys_db.has_user(adbuser):
        sys_db.create_user(username=adbuser, password=adbpw, active=True)
        print(f'Created user "{adbuser}"')
    else:
        print(f'User {adbuser} exists.')
    
    if sys_db.update_permission(username=adbuser, database=dbname, permission='rw'):
        print(f'All operations on database {dbname} will be done under user {adbuser}.')
    else:
        print(f'Something failed attempting to update rw permissions for user {adbuser} on database {dbname}. Allowing to continue. Keep an eye on permission errors.')

    db = client.db(dbname, username=adbuser, password=adbpw)
    if not db.has_collection(STOCKS_COLLECTION):
            col = db.create_collection(name=STOCKS_COLLECTION)
            print(f'Created collection {STOCKS_COLLECTION}. Download will start from {BEGINNING_DATE}.')
            col.add_hash_index(fields=['ticker', 'date'], unique=True)
            col.add_skiplist_index(fields=['ticker'], unique=False)
            col.add_skiplist_index(fields=['date'], unique=False)
    else:
         col = db.collection(STOCKS_COLLECTION)
    return db, col


def get_tickers_list(picks='./mypicks.csv', inclusion='./inclusion_list.txt', exclusion='./exclusion_list.txt'):

    def read_file(file_path):
        """
        Reads a file into memory. If errors occur, display a message and continue.
        Symbols that contain a dot (like BRK.A) have the dot converted to a dash so yfinance works fine.
        """
        try:
            with open(file_path, 'r') as file:
                tickers = list(set(file.read().replace('\n', ' ').split()))
                return [s.replace('.', '-') for s in tickers]
        except FileNotFoundError:
            print(f"The file {file_path} does not exist. Ignoring input.")
            return []
        except Exception as e:
            print(f"An error occurred while reading {file_path}: {e}")
            return []


    def get_exchanges_tickers():
        """
        Get all the tickers we'll use. Downloads and stores major indexes stocks, so we don't have
        to call them every time (we're being nice to Wikipedia).
        """
        #### Russell 1000
        R1000_filename="Russell_1000_list.gzip"

        try:
            r1000_ticker_df = pd.read_parquet(R1000_filename)
        except:
            r1000_ticker_df = pd.read_html("https://en.wikipedia.org/wiki/Russell_1000_Index")[2]
            r1000_ticker_df.to_parquet(R1000_filename, compression="gzip")

        #### Dow Jones
        DJI_filename="DJI_list.gzip"

        try:
            dji_ticker_df = pd.read_parquet(DJI_filename)
        except:
            dji_ticker_df = pd.read_html("https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average")[1]
            dji_ticker_df.to_parquet(DJI_filename, compression="gzip")

        #### NASDAQ 100
        N100_filename = "NASDAQ_100_list.gzip"
        try:
            n100_ticker_df = pd.read_parquet(N100_filename)
        except:
            n100_ticker_df = pd.read_html("https://en.wikipedia.org/wiki/Nasdaq-100")[4]
            n100_ticker_df.to_parquet(N100_filename, compression="gzip")

        try:
            extra = pd.read_csv(picks)['Ticker'] # mypics.csv typically comes from StockRover.
            if extra[-1:].values[0] == 'Summary':
                extra = extra[:-1]
        except:
            extra = pd.Series([])

        ticker_df = pd.concat([r1000_ticker_df['Ticker'], dji_ticker_df['Symbol'], n100_ticker_df['Ticker'], extra], ignore_index=True)
        ticker_df_list = list(set(ticker_df.to_list()))
        return [s.replace('.', '-') for s in ticker_df_list]

    # get_tickers_list function logic starts here
    all_tickers = read_file(inclusion) + get_exchanges_tickers()
    exclusion_tickers = read_file(exclusion)
    for i in exclusion_tickers:
        try:
            all_tickers.remove(i)
        except:
            pass
    return all_tickers

def get_last_update(ticker, db):
    """
    Get the latest date entry for a ticker in the DB
    """
    # AQL query to find the latest date for a given ticker
    aql_query = f"""
    FOR doc IN {STOCKS_COLLECTION}
        FILTER doc.ticker == '{ticker}'
        SORT doc.date DESC
        LIMIT 1
        RETURN doc.date
    """
    cursor = db.aql.execute(aql_query)
    latest_date = None
    for record in cursor:
        latest_date = record
        break  
    return latest_date


def calculate_downloads(db, tickers):
    """
    Returns a list of dictionaries with date and the tickers to download starting on that date.
    """
    # Internal functions
    def anything_to_download(d):
        return (next_trading_day(nyse, d) < last_trading_day(nyse))

    # Main function logic starts here.
    all_dates = []

    for i in tickers:
        lu = get_last_update(i, db)
        if lu != None:
            if anything_to_download(lu):
                all_dates.append([lu, i])
        else:
            all_dates.append([BEGINNING_DATE, i])

    ticker_dict = defaultdict(list)
    for item in all_dates:
        date, ticker = item
        ticker_dict[date].append(ticker)
    return [{"date": date, "tickers": tickers} for date, tickers in ticker_dict.items()]


def data_download(tickers=[], start=BEGINNING_DATE, interval='1d'):
    """
    Download the tickers in the daily timeframe starting with the proper date.
    Returns a list of records ready to insert or process.
    """
    if not tickers:
        return None
    
    try:
        df = yf.download(tickers=tickers, start=start, interval=interval)
    except:
        print('Something failed in the yfinance download.')
        return None
    
    records = []
    dates = df.index.unique()
    if isinstance(df.columns, pd.MultiIndex):
        for date in dates:
            for ticker in tickers:
                record = {
                    'ticker': ticker,
                    'date': date.strftime('%Y-%m-%d'),
                    'open': df.loc[date, ('Open', ticker)],
                    'high': df.loc[date, ('High', ticker)],
                    'low': df.loc[date, ('Low', ticker)],
                    'close': df.loc[date, ('Adj Close', ticker)]
                }
                records.append(record)
    else:
        ticker = tickers[0]
        for date in dates:
            record = {
                'ticker': ticker,
                'date': date.strftime('%Y-%m-%d'),
                'open': df.loc[date, 'Open'],
                'high': df.loc[date, 'High'],
                'low': df.loc[date, 'Low'],
                'close': df.loc[date, 'Adj Close']
            }
            records.append(record)
    return records


def main():
    db, col = init_db()
    tickers = get_tickers_list(picks='./mypicks.csv', inclusion='./inclusion_list.txt', exclusion='./exclusion_list.txt')
    # tickers = ['AAPL', 'AA', 'AMZN', '^GSPC'] # Manual override for testing only -- REMOVE for prod.
    download_lists = calculate_downloads(db, tickers) # Returns a list of sets ordered per date.
    if not download_lists:
        print('Nothing to download')
    else:
        for entry in download_lists:        
            records = data_download(tickers=entry['tickers'], start=entry['date'], interval='1d')
            col.import_bulk(records)


if __name__ == "__main__":
    main()

