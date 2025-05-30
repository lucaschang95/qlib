#!/usr/bin/env python3
"""
获取股票日K线数据 - 过去30个交易日
"""

from ib_insync import IB, Stock, util
import pandas as pd
from datetime import datetime, timedelta
import logging

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_daily_data(symbol='AAPL', days=30, exchange='SMART', currency='USD'):
    """
    获取股票日K线数据
    
    Args:
        symbol: 股票代码，如'AAPL', 'MSFT', 'GOOGL'等
        days: 需要获取的交易日数量，默认30天
        exchange: 交易所，默认'SMART'
        currency: 货币，默认'USD'
    
    Returns:
        DataFrame: 包含日K线数据的DataFrame
    """
    ib = IB()
    
    try:
        # 连接到IB Gateway
        logger.info("正在连接到 IB Gateway...")
        ib.connect('127.0.0.1', 4002, clientId=1)
        logger.info("✅ 成功连接到 IB Gateway")
        
        # 设置市场数据类型
        ib.reqMarketDataType(4)  # 使用延迟数据
        
        # 创建股票合约
        contract = Stock(symbol, exchange, currency)
        logger.info(f"创建合约: {contract}")
        
        # 获取合约详情
        ib.qualifyContracts(contract)
        logger.info(f"合约详情: {contract}")
        
        # 计算duration - 为了确保获得足够的交易日，我们请求更长的时间段
        # 30个交易日大约需要6-7周的日历时间（考虑周末和节假日）
        duration_days = max(days * 1.5, 50)  # 至少50天确保覆盖30个交易日
        
        # 获取日K线数据
        logger.info(f"正在请求 {symbol} 的日K线数据（过去约{int(duration_days)}天）...")
        bars = ib.reqHistoricalData(
            contract,
            endDateTime='',  # 当前时间
            durationStr=f'{int(duration_days)} D',  # 请求的天数
            barSizeSetting='1 day',  # 日K线
            whatToShow='TRADES',  # 交易数据
            useRTH=True,  # 只使用常规交易时间
            formatDate=1  # 返回字符串格式日期
        )
        
        if bars:
            # 转换为DataFrame
            df = util.df(bars)
            
            # 由于我们请求了更多天数，现在截取最近的指定交易日数量
            df = df.tail(days).reset_index(drop=True)
            
            logger.info(f"✅ 成功获取到 {len(df)} 个交易日的数据")
            logger.info(f"数据时间范围: {df['date'].min()} 到 {df['date'].max()}")
            logger.info(f"数据列: {list(df.columns)}")
            
            # 添加一些基本的技术指标计算
            df['price_change'] = df['close'] - df['open']
            df['price_change_pct'] = (df['close'] - df['open']) / df['open'] * 100
            df['daily_range'] = df['high'] - df['low']
            df['daily_range_pct'] = (df['high'] - df['low']) / df['open'] * 100
            
            return df
        else:
            logger.error("❌ 没有获取到历史数据")
            return None
            
    except Exception as e:
        logger.error(f"❌ 连接或数据获取失败: {str(e)}")
        return None
        
    finally:
        # 断开连接
        if ib.isConnected():
            ib.disconnect()
            logger.info("已断开IB连接")

def save_daily_data(df, symbol, filename=None):
    """保存日K线数据到CSV文件"""
    if df is None:
        return None
        
    if filename is None:
        filename = f"{symbol}_daily_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    
    df.to_csv(filename, index=False)
    logger.info(f"✅ 数据已保存到: {filename}")
    return filename

def display_data_summary(df, symbol):
    """显示数据摘要"""
    if df is None:
        return
        
    print(f"\n{'='*60}")
    print(f"📈 {symbol} 日K线数据摘要")
    print(f"{'='*60}")
    print(f"数据条数: {len(df)} 个交易日")
    print(f"时间范围: {df['date'].min()} 到 {df['date'].max()}")
    print(f"最新收盘价: ${df['close'].iloc[-1]:.2f}")
    print(f"期间最高价: ${df['high'].max():.2f}")
    print(f"期间最低价: ${df['low'].min():.2f}")
    print(f"平均成交量: {df['volume'].mean():,.0f}")
    print(f"期间涨跌: ${df['close'].iloc[-1] - df['close'].iloc[0]:.2f} ({((df['close'].iloc[-1] / df['close'].iloc[0]) - 1) * 100:.2f}%)")
    
    print(f"\n📊 最近5个交易日:")
    recent_data = df[['date', 'open', 'high', 'low', 'close', 'volume', 'price_change_pct']].tail()
    recent_data['price_change_pct'] = recent_data['price_change_pct'].round(2)
    print(recent_data.to_string(index=False))

def main():
    """主函数"""
    print("📈 股票日K线数据获取工具")
    print("="*50)
    
    # 可以修改这些参数
    symbol = 'AAPL'  # 股票代码
    days = 30        # 交易日数量
    
    print(f"正在获取 {symbol} 过去 {days} 个交易日的数据...")
    
    # 获取数据
    df = get_daily_data(symbol=symbol, days=days)
    
    if df is not None:
        # 显示数据摘要
        display_data_summary(df, symbol)
        
        # 保存数据
        filename = save_daily_data(df, symbol)
        
        print(f"\n✅ 任务完成！数据已保存到: {filename}")
        
        return df
    else:
        print("\n❌ 数据获取失败")
        return None

if __name__ == "__main__":
    # 运行主程序
    df = main()
    
    # 如果需要获取其他股票的数据，可以取消注释下面的代码
    # print("\n" + "="*50)
    # print("获取其他股票数据...")
    # 
    # other_stocks = ['MSFT', 'GOOGL', 'TSLA']
    # for stock in other_stocks:
    #     print(f"\n正在获取 {stock} 数据...")
    #     stock_df = get_daily_data(symbol=stock, days=30)
    #     if stock_df is not None:
    #         save_daily_data(stock_df, stock)
    #         display_data_summary(stock_df, stock) 