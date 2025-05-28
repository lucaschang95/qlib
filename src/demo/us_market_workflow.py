#  Copyright (c) Microsoft Corporation.
#  Licensed under the MIT License.
"""
Qlib provides two kinds of interfaces.
(1) Users could define the Quant research workflow by a simple configuration.
(2) Qlib is designed in a modularized way and supports creating research workflow by code just like building blocks.

The interface of (1) is `qrun XXX.yaml`.  The interface of (2) is script like this, which nearly does the same thing as `qrun XXX.yaml`
代码最终目的：验证这套"LightGBM模型 + TopkDropout策略"的组合在美国股市能否持续盈利。
"""
import qlib
from qlib.constant import REG_US
from qlib.utils import init_instance_by_config, flatten_dict
from qlib.workflow import R
from qlib.workflow.record_temp import SignalRecord, PortAnaRecord, SigAnaRecord
from qlib.tests.data import GetData


# 美国市场配置
SP500_MARKET = "sp500"
SP500_BENCH = "^GSPC"  # S&P500指数作为基准

# LightGBM模型配置
US_GBDT_MODEL = {
    "class": "LGBModel",
    "module_path": "qlib.contrib.model.gbdt",
    "kwargs": {
        "loss": "mse",
        "colsample_bytree": 0.8879,
        "learning_rate": 0.0421,
        "subsample": 0.8789,
        "lambda_l1": 205.6999,
        "lambda_l2": 580.9768,
        "max_depth": 8,
        "num_leaves": 210,
        "num_threads": 20,
    },
}

# 美国市场数据集配置
US_DATASET_CONFIG = {
    "class": "DatasetH",
    "module_path": "qlib.data.dataset",
    "kwargs": {
        "handler": {
            "class": "Alpha158",
            "module_path": "qlib.contrib.data.handler",
            "kwargs": {
                "start_time": "2008-01-01",
                "end_time": "2020-08-01",
                "fit_start_time": "2008-01-01",
                "fit_end_time": "2014-12-31",
                "instruments": SP500_MARKET,
            },
        },
        "segments": {
            "train": ("2008-01-01", "2014-12-31"),
            "valid": ("2015-01-01", "2016-12-31"),
            "test": ("2017-01-01", "2020-08-01"),
        },
    },
}

# 美国市场GBDT任务配置
US_GBDT_TASK = {
    "model": US_GBDT_MODEL,
    "dataset": US_DATASET_CONFIG,
}


if __name__ == "__main__":
    # use default data
    # 数据准备阶段 - 使用美股数据
    provider_uri = "~/.qlib/qlib_data/us_data"  # target_dir
    GetData().qlib_data(target_dir=provider_uri, region=REG_US, exists_skip=True)
    qlib.init(provider_uri=provider_uri, region=REG_US)

    # 模型和数据集配置
    model = init_instance_by_config(US_GBDT_TASK["model"])
    dataset = init_instance_by_config(US_GBDT_TASK["dataset"])

    # 投资组合分析配置 - 适配美股交易规则
    port_analysis_config = {
        "executor": {
            "class": "SimulatorExecutor",
            "module_path": "qlib.backtest.executor",
            "kwargs": {
                "time_per_step": "day",
                "generate_portfolio_metrics": True,
            },
        },
        "strategy": {
            "class": "TopkDropoutStrategy",
            "module_path": "qlib.contrib.strategy.signal_strategy",
            "kwargs": {
                "signal": (model, dataset),
                "topk": 50,
                "n_drop": 5,
            },
        },
        "backtest": {
            "start_time": "2017-01-01",
            "end_time": "2020-08-01",
            "account": 100000000,  # 1亿美元初始资金
            "benchmark": SP500_BENCH,
            "exchange_kwargs": {
                "freq": "day",
                "limit_threshold": None,  # 美股没有涨跌停限制
                "deal_price": "close",
                "open_cost": 0.001,   # 美股交易成本较低
                "close_cost": 0.001,
                "min_cost": 1,        # 最小交易成本1美元
            },
        },
    }

    # NOTE: This line is optional
    # It demonstrates that the dataset can be used standalone.
    example_df = dataset.prepare("train")
    print(example_df.head())

    # start exp
    with R.start(experiment_name="us_market_workflow"):
        R.log_params(**flatten_dict(US_GBDT_TASK))
        model.fit(dataset)
        R.save_objects(**{"params.pkl": model})

        # prediction
        recorder = R.get_recorder()
        sr = SignalRecord(model, dataset, recorder)
        sr.generate()

        # Signal Analysis
        sar = SigAnaRecord(recorder)
        sar.generate()

        # backtest. If users want to use backtest based on their own prediction,
        # please refer to https://qlib.readthedocs.io/en/latest/component/recorder.html#record-template.
        par = PortAnaRecord(recorder, port_analysis_config, "day")
        par.generate() 