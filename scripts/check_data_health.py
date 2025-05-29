from loguru import logger
import os
from typing import Optional

import fire
import pandas as pd
import qlib
from tqdm import tqdm

from qlib.data import D


class DataHealthChecker:
    """Checks a dataset for data completeness and correctness. The data will be converted to a pd.DataFrame and checked for the following problems:
    - any of the columns ["open", "high", "low", "close", "volume"] are missing
    - any data is missing
    - any step change in the OHLCV columns is above a threshold (default: 0.5 for price, 3 for volume)
    - any factor is missing
    - any stock is missing data on trading days (trading day coverage)
    """

    def __init__(
        self,
        csv_path=None,
        qlib_dir=None,
        freq="day",
        large_step_threshold_price=0.5,
        large_step_threshold_volume=3,
        missing_data_num=0,
    ):
        assert csv_path or qlib_dir, "One of csv_path or qlib_dir should be provided."
        assert not (csv_path and qlib_dir), "Only one of csv_path or qlib_dir should be provided."

        self.data = {}
        self.problems = {}
        self.freq = freq
        self.large_step_threshold_price = large_step_threshold_price
        self.large_step_threshold_volume = large_step_threshold_volume
        self.missing_data_num = missing_data_num

        if csv_path:
            assert os.path.isdir(csv_path), f"{csv_path} should be a directory."
            files = [f for f in os.listdir(csv_path) if f.endswith(".csv")]
            for filename in tqdm(files, desc="Loading data"):
                df = pd.read_csv(os.path.join(csv_path, filename))
                self.data[filename] = df

        elif qlib_dir:
            qlib.init(provider_uri=qlib_dir)
            self.load_qlib_data()

    def load_qlib_data(self):
        instruments = D.instruments(market="all")
        instrument_list = D.list_instruments(instruments=instruments, as_list=True, freq=self.freq)
        required_fields = ["$open", "$close", "$low", "$high", "$volume", "$factor"]
        for instrument in instrument_list:
            df = D.features([instrument], required_fields, freq=self.freq)
            df.rename(
                columns={
                    "$open": "open",
                    "$close": "close",
                    "$low": "low",
                    "$high": "high",
                    "$volume": "volume",
                    "$factor": "factor",
                },
                inplace=True,
            )
            self.data[instrument] = df
        print(df)

    def check_missing_data(self, instruments=None) -> Optional[pd.DataFrame]:
        """Check if any data is missing in the DataFrame."""
        result_dict = {
            "instruments": [],
            "open": [],
            "high": [],
            "low": [],
            "close": [],
            "volume": [],
        }
        
        # 过滤要检查的数据
        data_to_check = self.data
        if instruments is not None:
            data_to_check = {k: v for k, v in self.data.items() if k in instruments}
        
        for filename, df in data_to_check.items():
            missing_data_columns = df.isnull().sum()[df.isnull().sum() > self.missing_data_num].index.tolist()
            if len(missing_data_columns) > 0:
                result_dict["instruments"].append(filename)
                result_dict["open"].append(df.isnull().sum()["open"])
                result_dict["high"].append(df.isnull().sum()["high"])
                result_dict["low"].append(df.isnull().sum()["low"])
                result_dict["close"].append(df.isnull().sum()["close"])
                result_dict["volume"].append(df.isnull().sum()["volume"])

        result_df = pd.DataFrame(result_dict).set_index("instruments")
        if not result_df.empty:
            return result_df
        else:
            logger.info(f"✅ There are no missing data.")
            return None

    def check_large_step_changes(self, instruments=None) -> Optional[pd.DataFrame]:
        """Check if there are any large step changes above the threshold in the OHLCV columns."""
        result_dict = {
            "instruments": [],
            "col_name": [],
            "date": [],
            "pct_change": [],
        }
        
        # 过滤要检查的数据
        data_to_check = self.data
        if instruments is not None:
            data_to_check = {k: v for k, v in self.data.items() if k in instruments}
        
        for filename, df in data_to_check.items():
            affected_columns = []
            for col in ["open", "high", "low", "close", "volume"]:
                if col in df.columns:
                    pct_change = df[col].pct_change(fill_method=None).abs()
                    threshold = self.large_step_threshold_volume if col == "volume" else self.large_step_threshold_price
                    if pct_change.max() > threshold:
                        large_steps = pct_change[pct_change > threshold]
                        result_dict["instruments"].append(filename)
                        result_dict["col_name"].append(col)
                        result_dict["date"].append(large_steps.index.to_list()[0][1].strftime("%Y-%m-%d"))
                        result_dict["pct_change"].append(pct_change.max())
                        affected_columns.append(col)

        result_df = pd.DataFrame(result_dict).set_index("instruments")
        if not result_df.empty:
            return result_df
        else:
            logger.info(f"✅ There are no large step changes in the OHLCV column above the threshold.")
            return None

    def check_required_columns(self, instruments=None) -> Optional[pd.DataFrame]:
        """Check if any of the required columns (OLHCV) are missing in the DataFrame."""
        required_columns = ["open", "high", "low", "close", "volume"]
        result_dict = {
            "instruments": [],
            "missing_col": [],
        }
        
        # 过滤要检查的数据
        data_to_check = self.data
        if instruments is not None:
            data_to_check = {k: v for k, v in self.data.items() if k in instruments}
        
        for filename, df in data_to_check.items():
            if not all(column in df.columns for column in required_columns):
                missing_required_columns = [column for column in required_columns if column not in df.columns]
                result_dict["instruments"].append(filename)
                result_dict["missing_col"] += missing_required_columns

        result_df = pd.DataFrame(result_dict).set_index("instruments")
        if not result_df.empty:
            return result_df
        else:
            logger.info(f"✅ The columns (OLHCV) are complete and not missing.")
            return None

    def check_missing_factor(self, instruments=None) -> Optional[pd.DataFrame]:
        """Check if the 'factor' column is missing in the DataFrame."""
        result_dict = {
            "instruments": [],
            "missing_factor_col": [],
            "missing_factor_data": [],
        }
        
        # 过滤要检查的数据
        data_to_check = self.data
        if instruments is not None:
            data_to_check = {k: v for k, v in self.data.items() if k in instruments}
        
        for filename, df in data_to_check.items():
            if "000300" in filename or "000903" in filename or "000905" in filename:
                continue
            if "factor" not in df.columns:
                result_dict["instruments"].append(filename)
                result_dict["missing_factor_col"].append(True)
            if df["factor"].isnull().all():
                if filename in result_dict["instruments"]:
                    result_dict["missing_factor_data"].append(True)
                else:
                    result_dict["instruments"].append(filename)
                    result_dict["missing_factor_col"].append(False)
                    result_dict["missing_factor_data"].append(True)

        result_df = pd.DataFrame(result_dict).set_index("instruments")
        if not result_df.empty:
            return result_df
        else:
            logger.info(f"✅ The `factor` column already exists and is not empty.")
            return None

    def check_missing_trading_days(self, instruments=None) -> Optional[pd.DataFrame]:
        """Check if any stock is missing data on trading days."""
        result_dict = {
            "instruments": [],
            "missing_trading_days": [],
            "total_missing_days": [],
            "coverage_ratio": [],
        }
        
        # 过滤要检查的数据
        data_to_check = self.data
        if instruments is not None:
            data_to_check = {k: v for k, v in self.data.items() if k in instruments}
        
        if not data_to_check:
            logger.info(f"✅ No data to check for trading days coverage.")
            return None
        
        # 从qlib标准交易日历文件获取交易日
        try:
            # 尝试从qlib数据目录读取交易日历
            import qlib
            provider_uri = qlib.config.C.provider_uri
            if isinstance(provider_uri, dict):
                data_path = provider_uri.get('__DEFAULT_FREQ')
            else:
                data_path = provider_uri
            
            calendar_file = os.path.join(data_path, 'calendars', f'{self.freq}.txt')
            if os.path.exists(calendar_file):
                with open(calendar_file, 'r') as f:
                    trading_days = [line.strip() for line in f.readlines()]
                trading_days = pd.to_datetime(trading_days)
                logger.info(f"Loaded {len(trading_days)} trading days from {calendar_file}")
            else:
                logger.warning(f"Trading calendar file not found: {calendar_file}, falling back to union method")
                # 回退到原来的并集方法
                all_dates = set()
                for filename, df in data_to_check.items():
                    if hasattr(df.index, 'get_level_values'):
                        dates = df.index.get_level_values(1) if df.index.nlevels > 1 else df.index.get_level_values(0)
                    else:
                        dates = df.index
                    all_dates.update(dates)
                
                if not all_dates:
                    logger.info(f"✅ No date information found in the data.")
                    return None
                
                trading_days = sorted(list(all_dates))
                
        except Exception as e:
            logger.warning(f"Failed to load trading calendar: {e}, falling back to union method")
            # 回退到原来的并集方法
            all_dates = set()
            for filename, df in data_to_check.items():
                if hasattr(df.index, 'get_level_values'):
                    dates = df.index.get_level_values(1) if df.index.nlevels > 1 else df.index.get_level_values(0)
                else:
                    dates = df.index
                all_dates.update(dates)
            
            if not all_dates:
                logger.info(f"✅ No date information found in the data.")
                return None
            
            trading_days = sorted(list(all_dates))
        
        # 检查每只股票的交易日覆盖情况
        for filename, df in data_to_check.items():
            if hasattr(df.index, 'get_level_values'):
                # 如果是MultiIndex，获取日期级别
                stock_dates = df.index.get_level_values(1) if df.index.nlevels > 1 else df.index.get_level_values(0)
            else:
                stock_dates = df.index
            
            stock_dates = set(pd.to_datetime(stock_dates))
            trading_days_set = set(pd.to_datetime(trading_days))
            missing_dates = trading_days_set - stock_dates
            
            if missing_dates:
                missing_dates_list = sorted(list(missing_dates))
                result_dict["instruments"].append(filename)
                result_dict["missing_trading_days"].append([d.strftime("%Y-%m-%d") for d in missing_dates_list[:5]])  # 只显示前5个缺失日期
                result_dict["total_missing_days"].append(len(missing_dates))
                coverage_ratio = (len(trading_days) - len(missing_dates)) / len(trading_days) * 100
                result_dict["coverage_ratio"].append(f"{coverage_ratio:.2f}%")
        
        result_df = pd.DataFrame(result_dict).set_index("instruments")
        if not result_df.empty:
            return result_df
        else:
            logger.info(f"✅ All stocks have complete trading days coverage.")
            return None

    def check_data(self, instruments=None):
        # 如果指定了股票，过滤数据用于统计
        data_to_check = self.data
        if instruments is not None:
            data_to_check = {k: v for k, v in self.data.items() if k in instruments}
            missing_instruments = [inst for inst in instruments if inst not in self.data]
            if missing_instruments:
                logger.warning(f"Instruments not found in data: {missing_instruments}")
        
        check_missing_data_result = self.check_missing_data(instruments)
        check_large_step_changes_result = self.check_large_step_changes(instruments)
        check_required_columns_result = self.check_required_columns(instruments)
        check_missing_factor_result = self.check_missing_factor(instruments)
        check_missing_trading_days_result = self.check_missing_trading_days(instruments)
        if (
            check_missing_data_result is not None
            or check_large_step_changes_result is not None
            or check_required_columns_result is not None
            or check_missing_factor_result is not None
            or check_missing_trading_days_result is not None
        ):
            print(f"\nSummary of data health check ({len(data_to_check)} files checked):")
            print("-------------------------------------------------")
            if isinstance(check_missing_data_result, pd.DataFrame):
                logger.warning(f"There is missing data.")
                print(check_missing_data_result)
            if isinstance(check_large_step_changes_result, pd.DataFrame):
                logger.warning(f"The OHLCV column has large step changes.")
                print(check_large_step_changes_result)
            if isinstance(check_required_columns_result, pd.DataFrame):
                logger.warning(f"Columns (OLHCV) are missing.")
                print(check_required_columns_result)
            if isinstance(check_missing_factor_result, pd.DataFrame):
                logger.warning(f"The factor column does not exist or is empty")
                print(check_missing_factor_result)
            if isinstance(check_missing_trading_days_result, pd.DataFrame):
                logger.warning(f"Stocks are missing data on trading days.")
                print(check_missing_trading_days_result)


if __name__ == "__main__":
    fire.Fire(DataHealthChecker)
