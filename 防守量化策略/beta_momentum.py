import pandas as pd
import numpy as np
from xtquant import xtdata
from datetime import datetime, timedelta
import time
import warnings

warnings.filterwarnings('ignore')


def fetch_data_and_calc_momentum(etf_list: list) -> pd.DataFrame:
    """
    基于 xtdata 获取前复权行情，向量化计算宽基 ETF 的 20/60 日动量及 MA20 趋势过滤状态。
    """
    # 动量计算最长需要 60 根 K 线，预留 150 天以应对节假日
    start_date = (datetime.now() - timedelta(days=150)).strftime('%Y%m%d')

    print(f"正在向 QMT 发送宽基 ETF 历史数据下载指令 (起点: {start_date})...")
    for etf in etf_list:
        xtdata.download_history_data(etf, period='1d', start_time=start_date)

    print("正在等待宽基数据落盘...")
    timeout = 30
    start_time_stamp = time.time()

    # 轮询校验机制，确保 60 日动量所需的底层 K 线长度达标 (至少 65 根)
    while True:
        local_data = xtdata.get_local_data(
            stock_list=etf_list,
            period='1d',
            start_time=start_date,
            dividend_type='front'
        )
        probe_df = local_data.get(etf_list[0])

        if probe_df is not None and not probe_df.empty and len(probe_df) > 65:
            break

        if time.time() - start_time_stamp > timeout:
            raise TimeoutError(f"QMT 宽基数据同步超时({timeout}s)！请检查客户端网络与数据下载状态。")
        time.sleep(1)

    # 重塑为截面收盘价矩阵
    close_dict = {}
    for etf in etf_list:
        df = local_data.get(etf)
        if df is None or df.empty:
            continue
        if 'time' in df.columns:
            df.set_index('time', inplace=True)
        close_dict[etf] = df['close']

    df_close = pd.DataFrame(close_dict)

    # 彻底前向填充，防止某宽基指数当天停牌导致运算断层
    df_close = df_close.ffill()

    # 核心向量化计算: 没有任何 for 循环
    # 1. 计算 MA20 趋势过滤线
    ma20 = df_close.rolling(window=20).mean()
    is_uptrend = df_close > ma20

    # 2. 计算 20 日与 60 日 ROC (Rate of Change)
    roc20 = df_close.pct_change(periods=20)
    roc60 = df_close.pct_change(periods=60)

    # 3. 合成动量得分: Momentum = 0.5 * ROC20 + 0.5 * ROC60
    momentum_score = 0.5 * roc20 + 0.5 * roc60

    # 提取最新一个交易日的截面数据
    latest_close = df_close.iloc[-1]
    latest_ma20 = ma20.iloc[-1]
    latest_uptrend = is_uptrend.iloc[-1]
    latest_momentum = momentum_score.iloc[-1]

    # 组装结果 DataFrame
    result_df = pd.DataFrame({
        'Close': latest_close,
        'MA20': latest_ma20,
        'Is_Uptrend': latest_uptrend,
        'Momentum': latest_momentum
    })

    return result_df


def select_beta_satellite_etf(etf_list: list, threshold: float = 0.03) -> list:
    """
    Beta 宽基动量轮动主逻辑：过滤弱势标的 -> 考核动量阈值 -> 取 Top 1

    :param etf_list: 宽基监控池
    :param threshold: 动量启动阈值，默认 3% (0.03)
    :return: 达标的单一宽基 ETF 列表（若无达标者则返回空列表，退回触发模式 A）
    """
    print("=== 开始执行卫星仓位 (模式 B) 宽基动量轮动 ===")

    df_factors = fetch_data_and_calc_momentum(etf_list)

    # 1. 过滤：强制要求收盘价必须站上 20 日均线
    df_uptrend = df_factors[df_factors['Is_Uptrend'] == True].copy()

    if df_uptrend.empty:
        print(">> 所有宽基均跌破 MA20，当前市场无主升浪趋势。")
        return []

    # 2. 考核：得分降序排列，并卡死 3% 动量阈值
    df_sorted = df_uptrend.sort_values(by='Momentum', ascending=False)
    top_etf = df_sorted.index[0]
    top_momentum = df_sorted.iloc[0]['Momentum']

    print("\n【宽基动量监测横截面数据】")
    print(df_sorted[['Close', 'MA20', 'Momentum']].to_string(formatters={'Momentum': '{:.2%}'.format}))

    if top_momentum > threshold:
        print(f"\n✅ 捕捉到主升浪宽基！【{top_etf}】动量得分 {top_momentum:.2%} > 阈值 {threshold:.2%}。")
        return [top_etf]
    else:
        print(f"\n>> 虽有标的站上 MA20，但最强标的【{top_etf}】动量({top_momentum:.2%}) 未达 3% 启动阈值。")
        return []


if __name__ == '__main__':
    # 监控池：沪深300、中证500、创业板指、科创50
    broad_market_etfs = ['510300.SH', '510500.SH', '159915.SZ', '588090.SH']

    selected_beta_etf = select_beta_satellite_etf(broad_market_etfs)

    if selected_beta_etf:
        print(f"决策引擎: 卫星仓位 20% 将全仓切入 -> {selected_beta_etf[0]}")
    else:
        print("决策引擎: 宽基轮动条件不成立，卫星仓位退回执行 【模式 A: 个股Alpha选股】。")