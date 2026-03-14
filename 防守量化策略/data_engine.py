import pandas as pd
import numpy as np
from xtquant import xtdata
from datetime import datetime
import time


def fetch_data_and_calc_rsrs(stock_list: list, n: int = 18, m: int = 600) -> pd.Series:
    """
    基于 xtdata 获取前复权行情，利用 Pandas 向量化计算 RSRS 因子标准分
    修复了 QMT 毫秒时间戳 numpy.int64/str 格式化失败的问题。
    """
    min_length = n + m
    start_date = '20200101'

    print(f"1. 正在向 QMT 发送历史数据全量下载指令 (起点: {start_date})...")
    for stock in stock_list:
        xtdata.download_history_data(stock, period='1d', start_time=start_date)

    print("2. 正在等待本地硬盘数据落盘...")
    timeout = 30
    start_time_stamp = time.time()

    def get_disk_data():
        return xtdata.get_local_data(
            stock_list=stock_list,
            period='1d',
            start_time=start_date,
            dividend_type='front'
        )

    # 轮询校验机制
    while True:
        local_data = get_disk_data()
        probe_df = local_data.get(stock_list[0])

        if probe_df is not None and not probe_df.empty and len(probe_df) >= 100:
            print(f"数据落盘同步完成！基准标的当前有效数据长度: {len(probe_df)} 根K线。")
            break

        if time.time() - start_time_stamp > timeout:
            current_len = len(probe_df) if probe_df is not None else 0
            raise TimeoutError(f"QMT 硬盘数据同步超时({timeout}s)！当前硬盘读取长度仅为 {current_len}。")

        time.sleep(1)

    # 3. 将 get_local_data 的字典结构重塑为因子计算所需的截面矩阵
    high_dict = {}
    low_dict = {}

    for stock in stock_list:
        df = local_data.get(stock)
        if df is None or df.empty:
            continue

        if 'time' in df.columns:
            df.set_index('time', inplace=True)

        high_dict[stock] = df['high']
        low_dict[stock] = df['low']

    df_high = pd.DataFrame(high_dict)
    df_low = pd.DataFrame(low_dict)

    # 4. 数据清洗：前向填充处理港股休市导致的 NaN
    df_high = df_high.ffill()
    df_low = df_low.ffill()

    # 截取最后 800 行以优化后续运算速度
    df_high = df_high.tail(800)
    df_low = df_low.tail(800)

    print("3. 数据重塑完毕，开始向量化计算 RSRS 因子...")

    # 5. 彻底向量化计算 RSRS
    E_xy = (df_high * df_low).rolling(window=n).mean()
    E_x = df_low.rolling(window=n).mean()
    E_y = df_high.rolling(window=n).mean()

    cov_pop = E_xy - E_x * E_y
    rolling_cov = cov_pop * (n / (n - 1))

    rolling_var = df_low.rolling(window=n).var()

    beta = rolling_cov / rolling_var

    z_min_periods = min(250, m)
    beta_mean = beta.rolling(window=m, min_periods=z_min_periods).mean()
    beta_std = beta.rolling(window=m, min_periods=z_min_periods).std()

    z_score_matrix = (beta - beta_mean) / beta_std

    # 6. 提取最后一行有效信号
    latest_z_scores = z_score_matrix.iloc[-1].dropna()

    # 核心修复：强制转换为整型以兼容 numpy.int64/str，再转为日期格式
    raw_time = latest_z_scores.name
    try:
        # QMT 的日线时间戳为北京时间毫秒级 (13位)
        formatted_date = pd.to_datetime(int(raw_time), unit='ms').strftime('%Y-%m-%d')
        latest_z_scores.name = formatted_date
    except Exception as e:
        print(f"时间戳格式化警告: {e}")
        pass  # 若发生异常保留原状，不阻断主程序

    return latest_z_scores


if __name__ == '__main__':
    target_etfs = ['512890.SH', '513630.SH', '510880.SH']

    latest_rsrs = fetch_data_and_calc_rsrs(target_etfs)

    print(f"\n【最新 RSRS Z-score 指标】")
    print(f"信号日期: {latest_rsrs.name}")
    print("-" * 30)
    print(latest_rsrs.to_string())