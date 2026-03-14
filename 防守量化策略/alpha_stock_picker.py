import pandas as pd
import numpy as np
from xtquant import xtdata
from datetime import datetime, timedelta
import time
import warnings
import jqdatasdk as jq

warnings.filterwarnings('ignore')

# ==========================================
# 请在此处填入你的聚宽 (JoinQuant) 账号密码
# ==========================================
JQ_USERNAME = '18192430926'
JQ_PASSWORD = 'Weinairang0829'

try:
    jq.auth(JQ_USERNAME, JQ_PASSWORD)
except Exception as e:
    print(f"⚠️ 聚宽登录失败，请检查账号密码！错误: {e}")


def qmt_to_jq(code: str) -> str:
    """将 QMT 代码 (600000.SH) 转换为 聚宽代码 (600000.XSHG)"""
    if code.endswith('.SH'): return code.replace('.SH', '.XSHG')
    if code.endswith('.SZ'): return code.replace('.SZ', '.XSHE')
    if code.endswith('.BJ'): return code.replace('.BJ', '.XBSE')
    return code


def jq_to_qmt(code: str) -> str:
    """将 聚宽代码 (600000.XSHG) 转换为 QMT 代码 (600000.SH)"""
    if code.endswith('.XSHG'): return code.replace('.XSHG', '.SH')
    if code.endswith('.XSHE'): return code.replace('.XSHE', '.SZ')
    if code.endswith('.XBSE'): return code.replace('.XBSE', '.BJ')
    return code


def get_shenwan_industry_map(stock_list: list, parquet_path: str = "universe_2025-12-05.parquet") -> dict:
    """利用本地 Parquet 快照文件精准构建 股票代码 -> 申万一级行业 的映射字典"""
    industry_map = {}
    try:
        # 1. 加载本地 Parquet 映射表
        df_universe = pd.read_parquet(parquet_path)

        # 2. 仅过滤出申万一级行业 (sw_l1)
        df_sw_l1 = df_universe[df_universe['pool_type'] == 'sw_l1']

        # 3. 去重并构建极速映射字典
        df_sw_l1 = df_sw_l1.drop_duplicates(subset=['stock_code'])
        mapping_dict = dict(zip(df_sw_l1['stock_code'], df_sw_l1['pool_name']))

        # 4. 匹配目标标的
        for stock in stock_list:
            industry_map[stock] = mapping_dict.get(stock, 'Unknown')

    except Exception as e:
        print(f"⚠️ 读取 Parquet 行业映射文件失败，采用 Unknown 兜底。错误: {e}")
        for stock in stock_list:
            industry_map[stock] = 'Unknown'

    return industry_map


def filter_basic_info(stock_list: list) -> list:
    """基础排雷：利用 QMT 剔除 ST、次新"""
    valid_stocks = []
    one_year_ago = int((datetime.now() - timedelta(days=365)).strftime('%Y%m%d'))
    for stock in stock_list:
        detail = xtdata.get_instrument_detail(stock)
        if not detail: continue
        name = detail.get('InstrumentName', '')
        if 'ST' in name or '退' in name: continue
        open_date = detail.get('OpenDate', 20990101)
        try:
            if int(open_date) > one_year_ago: continue
        except:
            continue
        valid_stocks.append(stock)
    return valid_stocks


def fetch_financial_and_market_data(stock_list: list) -> pd.DataFrame:
    """
    【核心重构】: 聚宽获取财务特征 + QMT 获取波动率
    """
    print("1. 正在利用 QMT 本地算力计算 60 日波动率...")
    for stock in stock_list:
        xtdata.download_history_data(stock, period='1d',
                                     start_time=(datetime.now() - timedelta(days=120)).strftime('%Y%m%d'))
    time.sleep(1)

    market_data = xtdata.get_local_data(stock_list=stock_list, period='1d', count=80, dividend_type='front')
    vol_dict = {}
    for stock in stock_list:
        df = market_data.get(stock)
        if df is not None and not df.empty and len(df) >= 60:
            returns = df['close'].pct_change().dropna().tail(60)
            vol_dict[stock] = returns.std() * np.sqrt(250)
        else:
            vol_dict[stock] = np.nan

    print("2. 正在通过 JoinQuant 云端获取最新财务指标...")
    jq_codes = [qmt_to_jq(s) for s in stock_list]

    # 自动计算最近的一个工作日
    today = datetime.now()
    if today.weekday() == 5:
        last_trade_day = today - timedelta(days=1)
    elif today.weekday() == 6:
        last_trade_day = today - timedelta(days=2)
    else:
        last_trade_day = today

    # 核心修复：建立动态日期回溯列表 (今天 -> 3个月前 -> 半年前 -> 1年前)
    # 彻底规避聚宽试用账号的数据延迟问题
    test_dates = [
        last_trade_day.strftime('%Y-%m-%d'),
        (last_trade_day - timedelta(days=90)).strftime('%Y-%m-%d'),
        (last_trade_day - timedelta(days=180)).strftime('%Y-%m-%d'),
        (last_trade_day - timedelta(days=365)).strftime('%Y-%m-%d')
    ]

    q = jq.query(
        jq.valuation.code,
        jq.indicator.roe,
        jq.cash_flow.net_operate_cash_flow,
        jq.income.net_profit
    ).filter(jq.valuation.code.in_(jq_codes))

    df_jq = pd.DataFrame()

    # 启动动态探测
    for d in test_dates:
        print(f">> 尝试从聚宽获取财务快照，对齐日期: {d} ...")
        try:
            df_jq = jq.get_fundamentals(q, date=d)
            if df_jq is not None and not df_jq.empty:
                print(f"✅ 成功穿透 API 限制，获取到 {d} 的有效财务数据！")
                break  # 拿到数据，立刻跳出循环
        except Exception as e:
            print(f"   接口请求异常: {e}")

    if df_jq is None or df_jq.empty:
        raise ValueError(
            "🚨 致命错误：聚宽连续回溯 1 年均返回空数据！请彻底检查账号是否已到期，或前往聚宽官网签到领取积分。")

    # 数据重组与清洗
    fin_list = []
    for _, row in df_jq.iterrows():
        qmt_code = jq_to_qmt(row['code'])
        fin_list.append({
            'sec_code': qmt_code,
            'ROE': row['roe'] / 100.0 if pd.notna(row['roe']) else np.nan,
            'OCF': row['net_operate_cash_flow'],
            'NetProfit': row['net_profit'],
            'Vol': vol_dict.get(qmt_code, np.nan)
        })

    df_fin = pd.DataFrame(fin_list)
    return df_fin.dropna()


def select_alpha_satellite_stocks(pool_list: list, top_n: int = 5) -> list:
    print(f"=== 开始执行卫星仓位 (模式 A) 选股 ===")

    valid_stocks = filter_basic_info(pool_list)
    print(f"剔除 ST/次新后剩余: {len(valid_stocks)} 只")

    df_factors = fetch_financial_and_market_data(valid_stocks)
    if df_factors.empty:
        print("警告：财务因子截面为空，退出选股。")
        return []

    # ==========================================
    # 财务与现金流硬性排雷 (高息股的照妖镜)
    # 1. ROE >= 8% (保证基本盈利能力)
    # 2. 经营现金流 > 0 (公司真正在赚钱，而不是纸面富贵)
    # 3. 经营现金流 / 净利润 >= 0.8 (利润含金量极高，有钱分红)
    # ==========================================
    cond_roe = df_factors['ROE'] >= 0.08
    cond_ocf1 = df_factors['OCF'] > 0
    cond_ocf2 = (df_factors['OCF'] / df_factors['NetProfit']) >= 0.8

    df_filtered = df_factors[cond_roe & cond_ocf1 & cond_ocf2].copy()
    print(f"聚宽财务质量排雷后剩余: {len(df_filtered)} 只纯正现金奶牛")

    if df_filtered.empty:
        return []

    # ==========================================
    # 因子标准化与打分：高质量 (ROE) + 低波动 (Vol)
    # 因为池子本身就是红利100，所以直接选盈利最好且最抗跌的
    # ==========================================
    df_filtered['Z_ROE'] = (df_filtered['ROE'] - df_filtered['ROE'].mean()) / df_filtered['ROE'].std()
    df_filtered['Z_Vol'] = (df_filtered['Vol'] - df_filtered['Vol'].mean()) / df_filtered['Vol'].std()

    df_filtered['Z_ROE'] = df_filtered['Z_ROE'].fillna(0)
    df_filtered['Z_Vol'] = df_filtered['Z_Vol'].fillna(0)

    # Score = 0.6 * 高ROE - 0.4 * 低波动
    df_filtered['Score'] = 0.6 * df_filtered['Z_ROE'] - 0.4 * df_filtered['Z_Vol']
    df_sorted = df_filtered.sort_values(by='Score', ascending=False)

    industry_map = get_shenwan_industry_map(df_sorted['sec_code'].tolist())
    df_sorted['Industry'] = df_sorted['sec_code'].map(industry_map)

    final_selected = []
    industry_counter = {}

    for _, row in df_sorted.iterrows():
        stock = row['sec_code']
        ind = row['Industry']

        current_count = industry_counter.get(ind, 0)
        if current_count < 2:
            final_selected.append(stock)
            industry_counter[ind] = current_count + 1

        if len(final_selected) >= top_n:
            break

    print("\n【最终入选 Alpha 卫星名单 (真金白银基本面)】")
    for i, stock in enumerate(final_selected):
        score = df_sorted[df_sorted['sec_code'] == stock]['Score'].values[0]
        ind = df_sorted[df_sorted['sec_code'] == stock]['Industry'].values[0]
        print(f"Top {i + 1}: {stock} | 行业: {ind} | 得分: {score:.4f}")

    return final_selected


if __name__ == '__main__':
    # 测试代码
    mock_dividend_pool = [
        '601088.SH', '600028.SH', '601288.SH', '000983.SZ', '601988.SH',
        '600036.SH', '601166.SH', '000900.SZ', '600900.SH', '002142.SZ'
    ]
    selected_alpha_stocks = select_alpha_satellite_stocks(mock_dividend_pool)