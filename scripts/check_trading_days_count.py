"""
Trading Days Counter Script

This script checks the trading days count for specified stock instruments.

Usage:
    # Check trading days for multiple stocks in a specific year
    python scripts/check_trading_days_count.py check --instruments="AAPL,MSFT,GOOGL,AMZN,NVDA,TSLA,FB" --year=2020 --qlib_dir="~/.qlib/qlib_data/us_data"
    
    # Check trading days for all years (no year specified)
    python scripts/check_trading_days_count.py check --instruments="AAPL,MSFT,GOOGL" --qlib_dir="~/.qlib/qlib_data/us_data"
    
    # Use default qlib configuration (if qlib is already initialized)
    python scripts/check_trading_days_count.py check --instruments="AAPL,MSFT,GOOGL" --year=2020

Parameters:
    instruments: Comma-separated list of stock symbols (required)
                Example: "AAPL,MSFT,GOOGL,AMZN,NVDA,TSLA,FB"
    year: Specific year to check (optional)
          Example: 2020, 2021, 2022, etc.
    qlib_dir: Path to qlib data directory (optional)
             Example: "~/.qlib/qlib_data/us_data"

Output:
    - Trading days count for each instrument
    - Start and end dates of data
    - Statistical summary (average, min, max trading days)

Examples:
    # MAG7 stocks in 2020
    python scripts/check_trading_days_count.py check --instruments="AAPL,MSFT,GOOGL,AMZN,NVDA,TSLA,FB" --year=2020 --qlib_dir="~/.qlib/qlib_data/us_data"
    
    # Chinese stocks in 2021
    python scripts/check_trading_days_count.py check --instruments="000001.SZ,000002.SZ,600000.SH" --year=2021 --qlib_dir="/path/to/cn_data"
"""

from loguru import logger
import os
from typing import List, Optional

import fire
import pandas as pd
import qlib
from tqdm import tqdm

from qlib.data import D


class TradingDaysCounter:
    """检查指定股票列表中每个股票的交易日数量"""

    def __init__(
        self,
        qlib_dir=None,
        freq="day",
    ):
        self.data = {}
        self.freq = freq
        
        if qlib_dir:
            qlib.init(provider_uri=qlib_dir)
        else:
            # 如果没有指定qlib_dir，尝试使用默认配置
            try:
                qlib.init()
            except Exception as e:
                logger.error(f"Failed to initialize qlib: {e}")
                logger.error("Please specify qlib_dir parameter or ensure qlib is properly configured")
                raise

    def load_instruments_data(self, instruments: List[str]):
        """加载指定股票的数据"""
        logger.info(f"Loading data for {len(instruments)} instruments...")
        
        # 检查股票代码是否存在
        all_instruments = D.instruments(market="all")
        available_instruments = D.list_instruments(instruments=all_instruments, as_list=True, freq=self.freq)
        
        missing_instruments = []
        valid_instruments = []
        
        for instrument in instruments:
            if instrument in available_instruments:
                valid_instruments.append(instrument)
            else:
                missing_instruments.append(instrument)
        
        if missing_instruments:
            logger.warning(f"Instruments not found: {missing_instruments}")
        
        if not valid_instruments:
            logger.error("No valid instruments found!")
            return
        
        # 只加载必要的字段来减少内存使用
        required_fields = ["$close"]  # 只需要一个字段来统计交易日
        
        for instrument in tqdm(valid_instruments, desc="Loading instrument data"):
            try:
                df = D.features([instrument], required_fields, freq=self.freq)
                if not df.empty:
                    self.data[instrument] = df
                else:
                    logger.warning(f"No data found for instrument: {instrument}")
            except Exception as e:
                logger.error(f"Failed to load data for {instrument}: {e}")

    def count_trading_days(self, instruments: List[str], year: Optional[int] = None) -> pd.DataFrame:
        """统计每个股票的交易日数量"""
        
        # 加载数据
        self.load_instruments_data(instruments)
        
        if not self.data:
            logger.error("No data loaded!")
            return pd.DataFrame()
        
        result_dict = {
            "instrument": [],
            "trading_days_count": [],
            "start_date": [],
            "end_date": [],
        }
        
        # 如果指定了年份，添加年份信息到结果中
        if year:
            result_dict["year"] = []
        
        for instrument, df in self.data.items():
            if hasattr(df.index, 'get_level_values'):
                # 如果是MultiIndex，获取日期级别
                dates = df.index.get_level_values(1) if df.index.nlevels > 1 else df.index.get_level_values(0)
            else:
                dates = df.index
            
            # 转换为 pandas datetime
            dates = pd.to_datetime(dates)
            
            # 如果指定了年份，过滤该年份的数据
            if year:
                dates = dates[dates.year == year]
                if len(dates) == 0:
                    logger.warning(f"No data found for instrument {instrument} in year {year}")
                    continue
            
            # 删除重复日期并排序
            unique_dates = sorted(list(set(dates)))
            
            result_dict["instrument"].append(instrument)
            result_dict["trading_days_count"].append(len(unique_dates))
            result_dict["start_date"].append(unique_dates[0].strftime("%Y-%m-%d") if unique_dates else "N/A")
            result_dict["end_date"].append(unique_dates[-1].strftime("%Y-%m-%d") if unique_dates else "N/A")
            
            if year:
                result_dict["year"].append(year)
        
        result_df = pd.DataFrame(result_dict).set_index("instrument")
        return result_df

    def check(self, instruments, qlib_dir: Optional[str] = None, year: Optional[int] = None):
        """
        检查指定股票列表的交易日数量
        
        Args:
            instruments: 股票代码列表，用逗号分隔，例如: "000001.SZ,000002.SZ,600000.SH"
            qlib_dir: qlib数据目录路径，可选
            year: 指定年份，例如: 2023，如果不指定则统计所有年份
        """
        if qlib_dir:
            qlib.init(provider_uri=qlib_dir)
        
        # 解析股票列表 - 处理可能的 tuple 输入
        if isinstance(instruments, tuple):
            # 如果是 tuple，将所有元素作为股票列表
            instrument_list = [str(inst).strip() for inst in instruments]
        elif isinstance(instruments, str):
            instrument_list = [inst.strip() for inst in instruments.split(",")]
        else:
            # 如果不是字符串，尝试转换
            instrument_list = [str(inst).strip() for inst in instruments] if hasattr(instruments, '__iter__') else [str(instruments)]
        
        if year:
            logger.info(f"Checking trading days count for {len(instrument_list)} instruments in year {year}: {instrument_list}")
        else:
            logger.info(f"Checking trading days count for {len(instrument_list)} instruments: {instrument_list}")
        
        # 统计交易日数量
        result_df = self.count_trading_days(instrument_list, year)
        
        if result_df.empty:
            logger.error("No results to display!")
            return
        
        # 显示结果
        if year:
            print(f"\nTrading Days Count Summary for Year {year}:")
        else:
            print(f"\nTrading Days Count Summary:")
        print("=" * 60)
        print(result_df.to_string())
        
        # 显示统计信息
        print(f"\nStatistics:")
        print(f"Total instruments checked: {len(result_df)}")
        print(f"Average trading days: {result_df['trading_days_count'].mean():.2f}")
        print(f"Min trading days: {result_df['trading_days_count'].min()}")
        print(f"Max trading days: {result_df['trading_days_count'].max()}")
        
        if year:
            print(f"Year: {year}")


if __name__ == "__main__":
    fire.Fire(TradingDaysCounter) 