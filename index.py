import concurrent.futures
import math
import os
import traceback
import glob
import csv
import time
import random
import requests

import yfinance as yf
import pandas as pd
import proxlist

from typing import List
from concurrent.futures import ThreadPoolExecutor
from typing import List, Any
from requests.exceptions import ChunkedEncodingError, RequestException
from urllib3.exceptions import ProtocolError

def fetch_stock_data(symbol: str, max_retries: int = 5, initial_delay: float = 1.0, use_proxy: bool = False) -> None:
    """
    Fetch stock data for a given symbol and save it to a CSV file.
    
    Args:
        symbol: Stock symbol to fetch
        max_retries: Maximum number of retry attempts
        initial_delay: Initial delay between retries
        use_proxy: Whether to use proxy for requests
    """
    if not isinstance(symbol, str):
        print(f"Invalid symbol type: {type(symbol)}. Skipping...")
        write_to_csv(str(symbol), "failed.csv")
        return
    
    delay = initial_delay
    for attempt in range(max_retries):
        try:
            proxy = None
            if use_proxy:
                try:
                    proxy = proxlist.random_proxy()
                except Exception as e:
                    print(f"Error getting random proxy: {e}")
            
            print(f"Fetching {symbol}" + (f" with proxy {proxy}" if proxy else "")
                  + f" (Attempt {attempt + 1}/{max_retries})")

            df = yf.download(symbol, period="max", interval="1d", group_by="ticker", proxy=proxy)
            
            # Handle multi-index columns if present
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(1)
            
            # Check if DataFrame is empty or missing required columns
            required_columns = ["Open", "High", "Low", "Close", "Volume"]
            if df.empty or not all(col in df.columns for col in required_columns):
                raise ValueError(f"Missing required columns. Available columns: {df.columns.tolist()}")
            
            # Add Ticker column
            df['Ticker'] = symbol
            
            # Round down the numeric columns using math.floor
            numeric_columns = ["Open", "High", "Low", "Close"]
            df[numeric_columns] = df[numeric_columns].apply(lambda x: x.apply(math.floor))
            
            # Save only the columns we need
            output_columns = ["Ticker", "Open", "High", "Low", "Close", "Volume"]
            df[output_columns].to_csv(f"csv/{symbol}.csv")
            
            print(f"Fetching {symbol} with proxy {proxy}: success!")
            return  # Success, exit the function
            
        except (ChunkedEncodingError, ProtocolError, RequestException) as net_error:
            print(f"Network error while fetching {symbol}: {net_error}")
        except ValueError as ve:
            print(f"Value error while fetching {symbol}: {ve}")
        except Exception as error:
            error_message = f"Unexpected error fetching {symbol}"
            if proxy:
                error_message += f" with proxy {proxy}"
            error_message += f": {error}"
            print(error_message)
        
        # If we get here, an error occurred. Wait before retrying.
        if attempt < max_retries - 1:  # No need to wait after the last attempt
            wait_time = delay * (2 ** attempt) + random.uniform(0, 1)
            print(f"Retrying in {wait_time:.2f} seconds...")
            time.sleep(wait_time)
    
    # If we've exhausted all retries, write to the failed CSV
    write_to_csv(symbol, "failed.csv")
    print(f"Failed to fetch {symbol} after {max_retries} attempts")

def write_to_csv(data: Any, file_name: str) -> None:
    """Write data to a CSV file."""
    if isinstance(data, str):
        row = [data]
    else:
        item = data.split(",")
        symbol = "IHSG" if item[0] == "JKSE" else item[0]
        row = [symbol] + item[1:6] + [item[7]]
    
    with open(file_name, 'a', newline='', encoding='utf-8') as f:
        csv.writer(f).writerow(row)

def fetch_async(stock_list: List[str], max_retries: int = 5, initial_delay: float = 1.0, use_proxy: bool = False) -> List[str]:
    """
    Fetch stock data asynchronously for a list of symbols and return list of failed stocks.
    
    Args:
        stock_list: List of stock symbols to fetch
        max_retries: Maximum number of retry attempts
        initial_delay: Initial delay between retries
        use_proxy: Whether to use proxy for requests
    """
    failed_stocks = []
    with ThreadPoolExecutor(max_workers=1) as executor:
        future_to_stock = {
            executor.submit(
                fetch_stock_data, 
                symbol, 
                max_retries, 
                initial_delay,
                use_proxy
            ): symbol for symbol in stock_list
        }
        for future in concurrent.futures.as_completed(future_to_stock):
            symbol = future_to_stock[future]
            try:
                result = future.result()
                if result is not None:
                    print(f"Async result error for {symbol}")
                    print(result)
                    failed_stocks.append(symbol)
            except Exception as error:
                print(f"Exception error occurred for {symbol}:")
                print(error)
                print(traceback.format_exc())
                failed_stocks.append(symbol)
    return failed_stocks

def retry_failed_fetches(max_retries: int = 3, initial_delay: float = 5.0, use_proxy: bool = False) -> None:
    """
    Retry fetching data for failed stocks with exponential backoff.
    
    Args:
        max_retries: Maximum number of retry attempts
        initial_delay: Initial delay between retries
        use_proxy: Whether to use proxy for requests
    """
    failed_csv_path = "failed.csv"
    if not is_empty_csv(failed_csv_path):
        with open(failed_csv_path, "r") as file:
            stock_list = [row[0] for row in csv.reader(file)]
        
        delay = initial_delay
        for attempt in range(max_retries):
            print(f"Retry attempt {attempt + 1}/{max_retries}")
            remaining_stocks = fetch_async(stock_list, max_retries, initial_delay, use_proxy)
            
            if not remaining_stocks:
                print("All failed stocks successfully fetched.")
                # Clear the failed.csv file
                open(failed_csv_path, 'w').close()
                return
            
            if attempt < max_retries - 1:  # No need to wait after the last attempt
                wait_time = delay * (2 ** attempt) + random.uniform(0, 1)
                print(f"Retrying remaining stocks in {wait_time:.2f} seconds...")
                time.sleep(wait_time)
            
            stock_list = remaining_stocks
        
        # If we've exhausted all retries, update the failed.csv with remaining stocks
        with open(failed_csv_path, 'w', newline='', encoding='utf-8') as f:
            csv.writer(f).writerows([[stock] for stock in remaining_stocks])
        print(f"Failed to fetch {len(remaining_stocks)} stocks after {max_retries} retry attempts.")
    else:
        print("Nothing to retry")

def merge_csv_files() -> None:
    """Merge all individual stock CSV files into a single result file."""
    files = glob.glob("csv/*.csv")
    df = pd.concat((pd.read_csv(f, header=0) for f in files))
    df.to_csv("results.csv", index=False)

def is_empty_csv(path: str) -> bool:
    """Check if a CSV file is empty (contains only header)."""
    with open(path) as csvfile:
        return sum(1 for _ in csv.reader(csvfile)) <= 1
    
def get_stock_list() -> List[str]:
    """
    Extract trading pairs from Coinbase Exchange API.
    Returns a list of trading pairs (e.g., 'BTC-USD', 'ETH-USD', ...) that are:
    - Currently online
    - Trading against USD
    """
    try:
        url = 'https://api.exchange.coinbase.com/products'
        headers = {'Content-Type': 'application/json'}
        
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an exception for bad status codes
        
        pairs = response.json()
        
        # Extract only online USD/USDT trading pairs
        trading_pairs = []
        for pair in pairs:
            if (isinstance(pair, dict) and 
                'id' in pair and 
                'status' in pair and 
                pair['status'] == 'online' and 
                'quote_currency' in pair and 
                pair['quote_currency'] in ['USD']):
                
                trading_pairs.append(pair['id'])
        
        if not trading_pairs:
            raise ValueError("No active trading pairs found")
            
        print(f"Found {len(trading_pairs)} valid USD/USDT trading pairs")
        return trading_pairs
        
    except Exception as e:
        print(f"Error fetching product list: {str(e)}")
        print("Raw API response:", response.text if 'response' in locals() else "No response")
        raise

if __name__ == '__main__':
    print("Start Crypto updater...")
    start_time = time.time()

    # Configure whether to use proxies
    USE_PROXY = False  # Set to True to enable proxy usage

    stock_list = get_stock_list()

    # Create csv folder
    os.makedirs("csv", exist_ok=True)

    # Create failed.csv
    open("failed.csv", "w").close()

    # Create results.csv
    open("results.csv", "w").close()

    # Fetch data
    fetch_async(stock_list, use_proxy=USE_PROXY)

    # Retry failed fetches
    retry_failed_fetches(max_retries=3, initial_delay=5.0, use_proxy=USE_PROXY)

    # Merge CSV files
    merge_csv_files()

    elapsed_time = time.time() - start_time
    print(f"Elapsed time: {elapsed_time:.2f} seconds")
