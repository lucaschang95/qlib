from loguru import logger
import os
from typing import Optional, List
import json

import fire
import pandas as pd
import qlib
from tqdm import tqdm

from qlib.data import D


class FactorChecker:
    """Checks stocks in qlib dataset for non-1 values in the factor field."""

    def __init__(
        self,
        qlib_dir=None,
        freq="day",
    ):
        assert qlib_dir, "qlib_dir should be provided."
        
        self.data = {}
        self.freq = freq
        
        qlib.init(provider_uri=qlib_dir)
        logger.info(f"Initialized qlib with provider_uri: {qlib_dir}")

    def load_qlib_data(self, instruments=None):
        """Load factor data from qlib for specified instruments or all instruments."""
        if instruments is None:
            instruments = D.instruments(market="all")
            instrument_list = D.list_instruments(instruments=instruments, as_list=True, freq=self.freq)
        else:
            # If instruments is provided as string, parse it as JSON
            if isinstance(instruments, str):
                try:
                    instrument_list = json.loads(instruments)
                except json.JSONDecodeError:
                    # If not valid JSON, treat as single instrument
                    instrument_list = [instruments]
            elif isinstance(instruments, list):
                instrument_list = instruments
            else:
                instrument_list = [instruments]
        
        logger.info(f"Loading factor data for {len(instrument_list)} instruments...")
        
        for instrument in tqdm(instrument_list, desc="Loading factor data"):
            try:
                df = D.features([instrument], ["$factor"], freq=self.freq)
                if not df.empty:
                    df.rename(columns={"$factor": "factor"}, inplace=True)
                    self.data[instrument] = df
                else:
                    logger.warning(f"No data found for instrument: {instrument}")
            except Exception as e:
                logger.error(f"Failed to load data for {instrument}: {e}")

    def check_factor_non_one(self, instruments=None) -> Optional[pd.DataFrame]:
        """Check for non-1 values in the factor field."""
        result_dict = {
            "instruments": [],
            "non_one_count": [],
            "total_records": [],
            "non_one_ratio": [],
            "min_factor": [],
            "max_factor": [],
            "sample_non_one_dates": [],
            "sample_non_one_values": [],
        }
        
        # Load data first
        self.load_qlib_data(instruments)
        
        if not self.data:
            logger.warning("No data loaded. Please check if instruments exist or qlib_dir is correct.")
            return None
        
        for instrument, df in self.data.items():
            if 'factor' not in df.columns:
                logger.warning(f"Factor column not found for {instrument}")
                continue
            
            # Remove NaN values for analysis
            factor_series = df['factor'].dropna()
            
            if factor_series.empty:
                logger.warning(f"No valid factor data for {instrument}")
                continue
            
            # Find non-1 values
            non_one_mask = factor_series != 1.0
            non_one_values = factor_series[non_one_mask]
            
            if len(non_one_values) > 0:
                # Get sample dates and values (up to 5)
                sample_indices = non_one_values.index[:5]
                sample_dates = []
                sample_values = []
                
                for idx in sample_indices:
                    if hasattr(idx, '__len__') and len(idx) > 1:
                        # MultiIndex case (instrument, date)
                        date_str = idx[1].strftime("%Y-%m-%d") if hasattr(idx[1], 'strftime') else str(idx[1])
                    else:
                        # Single index case
                        date_str = idx.strftime("%Y-%m-%d") if hasattr(idx, 'strftime') else str(idx)
                    
                    sample_dates.append(date_str)
                    sample_values.append(f"{non_one_values.loc[idx]:.6f}")
                
                result_dict["instruments"].append(instrument)
                result_dict["non_one_count"].append(len(non_one_values))
                result_dict["total_records"].append(len(factor_series))
                result_dict["non_one_ratio"].append(f"{len(non_one_values)/len(factor_series)*100:.2f}%")
                result_dict["min_factor"].append(f"{factor_series.min():.6f}")
                result_dict["max_factor"].append(f"{factor_series.max():.6f}")
                result_dict["sample_non_one_dates"].append("; ".join(sample_dates))
                result_dict["sample_non_one_values"].append("; ".join(sample_values))
        
        if result_dict["instruments"]:
            result_df = pd.DataFrame(result_dict).set_index("instruments")
            return result_df
        else:
            logger.info(f"✅ All factor values are 1.0 for the checked instruments.")
            return None

    def check_data(self, instruments=None):
        """Main method to check factor data and display results."""
        logger.info("Starting factor non-1 value check...")
        
        check_result = self.check_factor_non_one(instruments)
        
        if check_result is not None:
            print(f"\nFactor Non-1 Value Check Results:")
            print("=" * 80)
            print(f"Found {len(check_result)} instruments with non-1 factor values:")
            print(check_result.to_string())
            
            # Summary statistics
            total_instruments = len(check_result)
            total_non_one_records = check_result['non_one_count'].sum()
            total_records = check_result['total_records'].sum()
            overall_ratio = total_non_one_records / total_records * 100 if total_records > 0 else 0
            
            print(f"\nSummary:")
            print(f"- Total instruments with non-1 factors: {total_instruments}")
            print(f"- Total non-1 factor records: {total_non_one_records}")
            print(f"- Total factor records: {total_records}")
            print(f"- Overall non-1 ratio: {overall_ratio:.2f}%")
        else:
            print(f"\n✅ All factor values are 1.0 for all checked instruments.")


if __name__ == "__main__":
    fire.Fire(FactorChecker) 