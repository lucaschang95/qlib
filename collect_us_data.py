#!/usr/bin/env python3

import pandas as pd
from yahooquery import Ticker
import os
import time
from pathlib import Path
from tqdm import tqdm
import numpy as np

def get_sp500_symbols():
    """Get S&P 500 symbols as a proxy for major US stocks"""
    # Major US stocks that are commonly available
    symbols = [
        'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'BRK-B', 'UNH', 'JNJ',
        'V', 'PG', 'JPM', 'HD', 'MA', 'DIS', 'PYPL', 'BAC', 'NFLX', 'ADBE',
        'CRM', 'CMCSA', 'XOM', 'VZ', 'KO', 'ABT', 'PFE', 'PEP', 'TMO', 'COST',
        'ABBV', 'WMT', 'MRK', 'AVGO', 'ACN', 'DHR', 'TXN', 'NEE', 'LIN', 'NKE',
        'CVX', 'QCOM', 'BMY', 'UPS', 'PM', 'LLY', 'ORCL', 'HON', 'IBM', 'INTC',
        # Add some more to get around 100 stocks
        'AMD', 'COP', 'GILD', 'MDLZ', 'SBUX', 'ISRG', 'AMGN', 'GE', 'CAT', 'AXP',
        'GS', 'MMM', 'CVS', 'BA', 'MCD', 'WBA', 'RTX', 'DE', 'BLK', 'SPGI',
        'NOW', 'ZTS', 'SYK', 'TJX', 'BKNG', 'ADP', 'VRTX', 'ADI', 'LRCX', 'KLAC',
        'EL', 'REGN', 'FISV', 'CSX', 'USB', 'TFC', 'SCHW', 'MU', 'AMAT', 'INTU',
        'CI', 'SO', 'DUK', 'BSX', 'PLD', 'CCI', 'NSC', 'AON', 'ICE', 'FCX',
        # Add indices
        '^GSPC', '^DJI', '^IXIC'
    ]
    return symbols

def download_stock_data(symbol, start_date, end_date, delay=0.1):
    """Download data for a single symbol"""
    try:
        time.sleep(delay)  # Rate limiting
        ticker = Ticker(symbol)
        data = ticker.history(interval='1d', start=start_date, end=end_date)
        
        if isinstance(data, pd.DataFrame) and not data.empty:
            # Reset index to get symbol and date as columns
            data_reset = data.reset_index()
            
            # Ensure we have the required columns
            required_cols = ['open', 'high', 'low', 'close', 'volume']
            if all(col in data_reset.columns for col in required_cols):
                # Add adjclose if not present (use close as fallback)
                if 'adjclose' not in data_reset.columns:
                    data_reset['adjclose'] = data_reset['close']
                
                # Calculate factor (for qlib compatibility)
                data_reset['factor'] = data_reset['adjclose'] / data_reset['close']
                data_reset['factor'] = data_reset['factor'].fillna(1.0)
                
                return data_reset
            else:
                print(f"Missing required columns for {symbol}")
                return None
        else:
            print(f"No data received for {symbol}")
            return None
            
    except Exception as e:
        print(f"Error downloading {symbol}: {e}")
        return None

def collect_us_data():
    """Collect US stock data for the specified time ranges"""
    
    # Create output directory
    output_dir = Path("us_stock_data")
    output_dir.mkdir(exist_ok=True)
    
    # Get symbols
    symbols = get_sp500_symbols()
    print(f"Collecting data for {len(symbols)} symbols...")
    
    # Date ranges
    start_date = '2017-01-01'
    end_date = '2025-04-30'
    
    print(f"Date range: {start_date} to {end_date}")
    
    successful_downloads = 0
    failed_downloads = 0
    
    # Download data for each symbol
    for symbol in tqdm(symbols, desc="Downloading"):
        data = download_stock_data(symbol, start_date, end_date, delay=0.2)
        
        if data is not None:
            # Clean symbol name for filename
            clean_symbol = symbol.replace('^', '').replace('-', '_')
            filename = f"{clean_symbol}.csv"
            filepath = output_dir / filename
            
            # Save to CSV
            data.to_csv(filepath, index=False)
            successful_downloads += 1
            
            print(f"✓ {symbol}: {len(data)} days saved to {filename}")
        else:
            failed_downloads += 1
            print(f"✗ {symbol}: Failed to download")
    
    print(f"\nDownload completed!")
    print(f"Successful: {successful_downloads}")
    print(f"Failed: {failed_downloads}")
    print(f"Data saved to: {output_dir}")
    
    return output_dir

if __name__ == "__main__":
    collect_us_data() 