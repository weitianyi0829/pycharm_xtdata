import os
import pandas as pd

# 导入你所有自建的模块
from data_engine import fetch_data_and_calc_rsrs
from alpha_stock_picker import select_alpha_satellite_stocks
from beta_momentum import select_beta_satellite_etf
from risk_manager import calc_atr_chandelier_exit
from portfolio_builder import generate_target_portfolio_csv

# ================= 标的池配置 =================
GLOBAL_ETF_POOL = [
    '512890.SH', '513630.SH', '510880.SH', '512830.SH',  # 核心红利
    '511880.SH', '518880.SH', '511520.SH', '511360.SH'  # 全天候防守矩阵
]

BROAD_MARKET_ETFS = ['510300.SH', '510500.SH', '159915.SZ', '588090.SH']  # Beta 宽基池

# 完整的 中证红利100 成分股池
DIVIDEND_100_POOL = [
    '000408.SZ', '000672.SZ', '000933.SZ', '002043.SZ', '002154.SZ',
    '002540.SZ', '002572.SZ', '002756.SZ', '301109.SZ', '600036.SH',
    '600096.SH', '600502.SH', '600737.SH', '600938.SH', '601001.SH',
    '601187.SH', '601598.SH', '601717.SH', '601916.SH', '603967.SH',
    '000429.SZ', '000895.SZ', '002267.SZ', '002563.SZ', '002867.SZ',
    '600256.SH', '600729.SH', '601101.SH', '601168.SH', '601318.SH',
    '601658.SH', '601825.SH', '601838.SH', '601919.SH', '601963.SH',
    '603706.SH', '920599.BJ', '000983.SZ', '002416.SZ', '600012.SH',
    '600039.SH', '600123.SH', '600273.SH', '600461.SH', '600546.SH',
    '600997.SH', '601019.SH', '601699.SH', '601857.SH', '601997.SH',
    '603565.SH', '601928.SH', '000651.SZ', '600373.SH', '601077.SH',
    '601229.SH', '600295.SH', '600755.SH', '600057.SH', '600919.SH',
    '600064.SH', '600015.SH', '600901.SH', '600757.SH', '601000.SH',
    '601225.SH', '000090.SZ', '601666.SH', '601216.SH', '002737.SZ',
    '600348.SH', '600985.SH', '601169.SH', '600188.SH', '002233.SZ',
    '600282.SH', '600971.SH', '601098.SH', '002601.SZ', '600350.SH',
    '601998.SH', '600585.SH', '600398.SH', '601668.SH', '601328.SH',
    '000157.SZ', '600016.SH', '601009.SH', '601288.SH', '601818.SH',
    '600028.SH', '600741.SH', '601166.SH', '601088.SH', '601398.SH',
    '601939.SH', '601988.SH', '601006.SH', '600153.SH', '600177.SH'
]


# =============================================

def daily_job():
    print("=" * 50)
    print("🚀 开始执行 V1.2 量化策略盘后主算力任务")
    print("=" * 50)

    # ----------------------------------------
    # Step 1: 核心底仓 80% 择时计算
    # ----------------------------------------
    print("\n[阶段一] 计算核心底仓 RSRS 因子...")
    latest_rsrs = fetch_data_and_calc_rsrs(GLOBAL_ETF_POOL)

    # ----------------------------------------
    # Step 2: 卫星仓位 20% 动态轮动分配
    # ----------------------------------------
    print("\n[阶段二] 决断 20% 卫星仓位归属...")
    satellite_targets = []

    # 先探测 模式 B (主升浪动量)
    beta_etfs = select_beta_satellite_etf(BROAD_MARKET_ETFS, threshold=0.03)

    if beta_etfs:
        # 捕获主升浪，执行模式 B
        target_beta_code = beta_etfs[0]
        # 计算 ATR 绝对止损价
        atr_stops = calc_atr_chandelier_exit([target_beta_code])
        sl_price = atr_stops.get(target_beta_code, 0.0)

        satellite_targets.append({
            'sec_code': target_beta_code,
            'target_weight': 0.20,
            'module_tag': 'sat_mode_b',
            'stop_loss_type': 'atr',
            'stop_loss_price': sl_price
        })
    else:
        # 无主升浪，退回执行 模式 A (Alpha 选股)
        alpha_stocks = select_alpha_satellite_stocks(DIVIDEND_100_POOL, top_n=5)

        if alpha_stocks:
            weight_per_stock = round(0.20 / len(alpha_stocks), 4)
            for stock in alpha_stocks:
                # 模式 A 采用硬止损，基准价 Ptrade 盘中按买入均价算，本地传 0 即可
                satellite_targets.append({
                    'sec_code': stock,
                    'target_weight': weight_per_stock,
                    'module_tag': 'sat_mode_a',
                    'stop_loss_type': 'hard',
                    'stop_loss_price': 0.0
                })

    # ----------------------------------------
    # Step 3: 合并清单并生成 CSV
    # ----------------------------------------
    print("\n[阶段三] 融合信号，生成终极指令文件...")
    # 指定一个固定的输出目录，方便每天去该目录取文件上传 Ptrade
    output_dir = os.path.abspath("./Ptrade_Output")
    os.makedirs(output_dir, exist_ok=True)

    generate_target_portfolio_csv(
        rsrs_series=latest_rsrs,
        satellite_targets=satellite_targets,
        output_dir=output_dir
    )

    print("\n🎉 今日盘后主算力任务全部执行完毕！")
    print("请将 ./Ptrade_Output/target_positions.csv 上传至 Ptrade 客户端执行实盘对齐。")


if __name__ == '__main__':
    daily_job()