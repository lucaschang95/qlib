#!/usr/bin/env python3

import pandas as pd
from yahooquery import Ticker
import os
from pathlib import Path

def test_yahoo_data():
    """Test downloading recent US stock data from Yahoo Finance"""
    
    # Test with a few major US stocks
    symbols = ['AAPL', 'MSFT', 'GOOGL', 'TSLA', 'AMZN']
    
    print("Testing Yahoo Finance data access...")
    
    for symbol in symbols:
        try:
            print(f"Downloading data for {symbol}...")
            ticker = Ticker(symbol)
            
            # Get data from 2024 to current
            data = ticker.history(interval='1d', start='2024-01-01', end='2025-04-30')
            
            if isinstance(data, pd.DataFrame) and not data.empty:
                print(f"✓ {symbol}: Got {len(data)} days of data")
                print(f"  Date range: {data.index.min()} to {data.index.max()}")
                
                # Save to CSV for inspection
                output_dir = Path("test_data")
                output_dir.mkdir(exist_ok=True)
                
                data_reset = data.reset_index()
                data_reset.to_csv(output_dir / f"{symbol}.csv", index=False)
                print(f"  Saved to test_data/{symbol}.csv")
            else:
                print(f"✗ {symbol}: No data received")
                
        except Exception as e:
            print(f"✗ {symbol}: Error - {e}")
    
    print("\nTest completed!")

if __name__ == "__main__":
    test_yahoo_data() 