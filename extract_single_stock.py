import os
import pandas as pd
import qlib
from qlib.data import D

def extract_stock_data(stock_symbol, qlib_dir, output_dir, freq="day"):
    """从qlib中提取单个股票数据并保存为CSV"""
    # 初始化qlib
    qlib.init(provider_uri=qlib_dir)
    
    # 创建输出目录
    os.makedirs(output_dir, exist_ok=True)
    
    # 获取数据
    required_fields = ["$open", "$close", "$low", "$high", "$volume", "$factor"]
    df = D.features([stock_symbol], required_fields, freq=freq)
    
    # 重命名列
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
    
    # 保存为CSV
    output_file = os.path.join(output_dir, f"{stock_symbol}.csv")
    df.to_csv(output_file)
    print(f"Data saved to: {output_file}")
    return output_file

if __name__ == "__main__":
    import fire
    fire.Fire(extract_stock_data) 