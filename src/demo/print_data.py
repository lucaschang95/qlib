#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Qlib 数据查看和可视化脚本

功能描述:
    这个脚本演示如何使用 Qlib 量化投资数据和研究平台加载和可视化美股数据。
    主要功能包括:
    1. 初始化 Qlib 环境
    2. 加载指定股票的历史数据
    3. 显示数据的基本信息
    4. 绘制价格和成交量的时间序列图表

使用场景:
    - 快速查看股票的历史价格和成交量数据
    - 验证 Qlib 数据是否正确安装和配置
    - 为量化策略开发提供数据探索工具

系统要求:
    - Python 3.7+
    - qlib 库
    - pandas
    - matplotlib

数据要求:
    - 需要预先下载和安装美股数据到 ~/.qlib/qlib_data/us_data 目录
    - 或根据实际数据路径修改 provider_uri 配置

配置说明:
    - instruments: 要查看的股票代码列表，默认为 ['AAPL']
    - start_time/end_time: 数据查询的时间范围
    - fields: 要获取的数据字段，默认为收盘价和成交量
    - provider_uri: Qlib 数据存储路径，需要根据实际情况调整

使用方法:
    python src/demo/print_data.py

输出:
    - 控制台显示数据加载状态和基本信息
    - 弹出图表窗口显示价格和成交量的时间序列图

作者: qlib demo
创建时间: 2024
"""

import qlib
from qlib.config import REG_US # <--- 修改为 REG_US
from qlib.data import D
import pandas as pd
import matplotlib.pyplot as plt

# 1. 初始化 Qlib
# 假设你的 .bin 文件存储在 ~/.qlib/qlib_data/us_data (美股数据常见路径)
# !!! 请根据你的实际 Qlib 美股数据存储路径修改 provider_uri !!!
try:
    qlib.init(provider_uri='~/.qlib/qlib_data/us_data', region=REG_US)
    print("Qlib 初始化成功 (区域: US)")
except Exception as e:
    print(f"Qlib 初始化失败: {e}")
    print("请确保已正确安装 Qlib 并下载了美股数据。")
    print("你可能需要调整 'provider_uri' 指向你的美股 .bin 数据存储路径。")
    exit()

# 2. 加载数据
instruments = ['AAPL'] # <--- 修改为 AAPL
start_time = '2020-01-01'
end_time = '2023-12-31'
fields = ['$close', '$volume']  # 假设字段名与之前相同

print(f"\n尝试加载股票 {instruments} 从 {start_time} 到 {end_time} 的字段: {fields}")

try:
    data = D.features(instruments=instruments, start_time=start_time, end_time=end_time, fields=fields)
except Exception as e:
    print(f"加载数据时出错: {e}")
    print("请检查：")
    print(f"  1. 股票代码 '{instruments[0]}' 是否在你的数据中。")
    print(f"  2. 日期范围 '{start_time}' - '{end_time}' 是否有效且有数据。")
    print(f"  3. 字段名 {fields} 是否正确。")
    print(f"  4. Qlib provider_uri 设置是否指向了包含 AAPL 数据的正确目录。")
    data = pd.DataFrame() # 创建一个空的 DataFrame 以避免后续绘图代码出错

# 查看加载的数据
if not data.empty:
    print("\n成功加载数据:")
    print(data.head())
else:
    print("\n未能加载到任何数据。请检查上述错误提示。")

# 3. 数据可视化
if not data.empty:
    # 确保数据格式正确
    print(f"\n数据形状: {data.shape}")
    print(f"数据列: {data.columns.tolist()}")
    print(f"数据索引类型: {type(data.index)}")
    
    # 简单的数据可视化
    plt.figure(figsize=(12, 6))
    
    # 获取日期索引（处理 MultiIndex）
    if isinstance(data.index, pd.MultiIndex):
        dates = data.index.get_level_values('datetime')
    else:
        dates = data.index
    
    # 绘制收盘价
    plt.subplot(2, 1, 1)
    if '$close' in data.columns:
        plt.plot(dates, data['$close'])
        plt.title(f'{instruments[0]} Close Price')
        plt.ylabel('Close Price ($)')
    
    # 绘制成交量
    plt.subplot(2, 1, 2)
    if '$volume' in data.columns:
        plt.plot(dates, data['$volume'])
        plt.title(f'{instruments[0]} Volume')
        plt.ylabel('Volume')
        plt.xlabel('Date')
    
    plt.tight_layout()
    plt.show()
else:
    print("\n没有数据可供可视化。")