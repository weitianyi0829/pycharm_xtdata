import pandas as pd
import numpy as np
from xtquant import xtdata
from datetime import datetime, timedelta
import time
import warnings

warnings.filterwarnings('ignore')


def calc_atr_chandelier_exit(stock_list: list, window: int = 22, multiplier: float = 3.0) -> dict:
    """
    基于 xtdata 获取前复权行情，向量化计算 ATR 吊灯止损的绝对触发价。
    公式: HighestHigh(22) - 3 * ATR(22)

    :param stock_list: 需要计算止损价的标的列表
    :param window: ATR 与最高价的回溯周期 (默认 22)
    :param multiplier: ATR 乘数 (默认 3.0)
    :return: 包含各标的绝对止损价的字典，如 {'510300.SH': 3.45}
    """
    if not stock_list:
        return {}

    start_date = (datetime.now() - timedelta(days=100)).strftime('%Y%m%d')

    print("正在拉取行情数据计算 ATR 吊灯止损线...")
    for stock in stock_list:
        xtdata.download_history_data(stock, period='1d', start_time=start_date)

    time.sleep(1)  # 等待数据落盘

    market_data = xtdata.get_local_data(
        stock_list=stock_list,
        period='1d',
        start_time=start_date,
        dividend_type='front'
    )

    stop_loss_dict = {}

    for stock in stock_list:
        df = market_data.get(stock)

        # 数据长度校验：至少需要 window + 1 根 K 线计算前收盘价和滚动均值
        if df is None or df.empty or len(df) < window + 1:
            print(f"⚠️ {stock} 数据不足，无法计算 ATR 止损，默认设为 0。")
            stop_loss_dict[stock] = 0.0
            continue

        if 'time' in df.columns:
            df.set_index('time', inplace=True)

        high = df['high']
        low = df['low']
        prev_close = df['close'].shift(1)

        # 1. 向量化计算 True Range (TR)
        # TR = Max(High - Low, |High - PrevClose|, |Low - PrevClose|)
        tr1 = high - low
        tr2 = (high - prev_close).abs()
        tr3 = (low - prev_close).abs()

        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

        # 2. 计算 ATR (TR 的 N 日简单移动平均)
        atr = tr.rolling(window=window).mean()

        # 3. 计算过去 N 日的最高价
        highest_high = high.rolling(window=window).max()

        # 4. 计算吊灯止损价
        chandelier_exit = highest_high - multiplier * atr

        # 提取最新一个交易日的止损价
        latest_stop_price = chandelier_exit.iloc[-1]

        # 处理异常值，并保留 3 位小数精度
        if pd.isna(latest_stop_price) or latest_stop_price < 0:
            stop_loss_dict[stock] = 0.0
        else:
            stop_loss_dict[stock] = round(float(latest_stop_price), 3)

    return stop_loss_dict


if __name__ == '__main__':
    # 测试：假设宽基动量轮动选出了 创业板指 ETF 和 沪深300 ETF
    target_beta_etfs = ['159915.SZ', '510300.SH']

    stop_prices = calc_atr_chandelier_exit(target_beta_etfs)

    print("\n【ATR 吊灯止损绝对防线】")
    for code, price in stop_prices.items():
        print(f"{code} -> 严格止损价: {price}")

    # ==========================================
    # 与 CSV 生成模块的无缝融合演示
    # ==========================================
    # 假设此时你正在构建 target_list 准备转成 DataFrame 导出
    target_list = []
    for code in target_beta_etfs:
        # 读取算好的绝对价格
        sl_price = stop_prices.get(code, 0)

        target_list.append({
            'sec_code': code,
            'target_weight': 0.20,  # 全仓切入 20% 卫星仓位
            'module_tag': 'sat_mode_b',
            'stop_loss_type': 'atr',
            'stop_loss_price': sl_price  # <--- 直接填入绝对数字
        })

    df_target = pd.DataFrame(target_list)
    print("\n【准备写入 CSV 的目标持仓片段】")
    print(df_target.to_string())