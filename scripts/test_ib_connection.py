#!/usr/bin/env python3
"""
测试 Interactive Brokers Gateway 连接并下载 AAPL 历史数据
"""

from ib_insync import IB, Stock, util
import pandas as pd
from datetime import datetime, timedelta
import logging

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_ib_connection():
    """测试IB连接并下载AAPL数据"""
    ib = IB()
    
    try:
        # 连接到IB Gateway (默认端口4001) 或 TWS (默认端口7497)
        logger.info("正在连接到 IB Gateway...")
        ib.connect('127.0.0.1', 4002, clientId=1)
        logger.info("✅ 成功连接到 IB Gateway")
        
        # 设置市场数据类型 (1=Live, 2=Frozen, 3=Delayed, 4=Delayed-Frozen)
        ib.reqMarketDataType(4)  # 使用延迟数据，不需要市场数据订阅
        
        # 创建股票合约
        contract = Stock('AAPL', 'SMART', 'USD')
        logger.info(f"创建合约: {contract}")
        
        # 获取合约详情
        ib.qualifyContracts(contract)
        logger.info(f"合约详情: {contract}")
        
        # 获取历史数据 - 最近5天的1分钟数据
        logger.info("正在请求历史数据...")
        bars = ib.reqHistoricalData(
            contract,
            endDateTime='',  # 当前时间
            durationStr='5 D',  # 5天数据
            barSizeSetting='1 min',  # 1分钟K线
            whatToShow='TRADES',  # 交易数据
            useRTH=True,  # 只使用常规交易时间
            formatDate=1  # 返回字符串格式日期
        )
        
        if bars:
            logger.info(f"✅ 成功获取到 {len(bars)} 条历史数据")
            
            # 转换为DataFrame
            df = util.df(bars)
            logger.info(f"数据时间范围: {df['date'].min()} 到 {df['date'].max()}")
            logger.info(f"数据列: {list(df.columns)}")
            
            # 显示前5行和后5行数据
            print("\n=== 前5行数据 ===")
            print(df.head())
            print("\n=== 后5行数据 ===")
            print(df.tail())
            
            # 保存到CSV文件
            filename = f"AAPL_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            df.to_csv(filename, index=False)
            logger.info(f"✅ 数据已保存到: {filename}")
            
            return True
        else:
            logger.error("❌ 没有获取到历史数据")
            return False
            
    except Exception as e:
        logger.error(f"❌ 连接或数据获取失败: {str(e)}")
        return False
        
    finally:
        # 断开连接
        if ib.isConnected():
            ib.disconnect()
            logger.info("已断开IB连接")

if __name__ == "__main__":
    print("🚀 开始测试 IB Gateway 连接...")
    print("请确保:")
    print("1. IB Gateway 或 TWS 正在运行")
    print("2. API 连接已启用")
    print("3. 端口设置正确 (Gateway: 4002 或 4001, TWS: 7497)")
    print("-" * 50)
    
    # 测试历史数据
    success = test_ib_connection()
    
    if success:
        print("\n" + "="*50)
        print("🎉 历史数据测试成功!")
        print("✅ AAPL 历史数据已成功下载并保存到CSV文件")
    else:
        print("\n❌ 连接测试失败，请检查:")
        print("1. IB Gateway/TWS 是否正在运行")
        print("2. API 设置是否正确")
        print("3. 防火墙设置") 