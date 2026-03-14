import pandas as pd
import os
from datetime import datetime, timedelta


def generate_target_portfolio_csv(rsrs_series: pd.Series, satellite_targets: list, output_dir: str = "./") -> str:
    """
    终极 CSV 生成引擎：合并底仓全天候矩阵与动态卫星仓位

    :param rsrs_series: 核心底仓 RSRS 信号
    :param satellite_targets: 调度器传来的卫星仓位字典列表
    """
    benchmark_code = '512890.SH'
    if benchmark_code not in rsrs_series:
        raise ValueError(f"缺少基准 RSRS 信号: {benchmark_code}")

    z_score = rsrs_series[benchmark_code]
    print(f"\n[决策引擎] {benchmark_code} 当前 RSRS Z-score: {z_score:.4f}")

    target_list = []

    # ==========================================
    # 模块 1：80% 核心底仓逻辑
    # ==========================================
    if z_score > -0.5:
        print(">> 底仓状态: 多头维持 (60%红利 + 20%港股红利)")
        target_list.append(
            {'sec_code': '512890.SH', 'target_weight': 0.60, 'module_tag': 'core_active', 'stop_loss_type': 'none',
             'stop_loss_price': 0})
        target_list.append(
            {'sec_code': '513630.SH', 'target_weight': 0.20, 'module_tag': 'core_active', 'stop_loss_type': 'none',
             'stop_loss_price': 0})
    else:
        print(">> 底仓状态: 破位防守 (50%银华日利 + 30%黄金 + 20%政金债)")
        target_list.append(
            {'sec_code': '511880.SH', 'target_weight': 0.40, 'module_tag': 'core_defensive', 'stop_loss_type': 'none',
             'stop_loss_price': 0})
        target_list.append(
            {'sec_code': '518880.SH', 'target_weight': 0.24, 'module_tag': 'core_defensive', 'stop_loss_type': 'none',
             'stop_loss_price': 0})
        target_list.append(
            {'sec_code': '511520.SH', 'target_weight': 0.16, 'module_tag': 'core_defensive', 'stop_loss_type': 'none',
             'stop_loss_price': 0})

    # ==========================================
    # 模块 2：装载 20% 动态卫星仓位
    # ==========================================
    if not satellite_targets:
        # 防呆：如果卫星模块全部失效，将 20% 资金默认放入银华日利防守
        print("⚠️ 卫星仓位为空！20% 资金强制退守银华日利。")
        target_list.append(
            {'sec_code': '511880.SH', 'target_weight': 0.20, 'module_tag': 'sat_fallback', 'stop_loss_type': 'none',
             'stop_loss_price': 0})
    else:
        # 叠加传入的卫星持仓
        target_list.extend(satellite_targets)

    # ==========================================
    # 生成与导出
    # ==========================================
    df_target = pd.DataFrame(target_list)

    # 聚合相同代码的权重 (例如底仓和卫星 fallback 都买入了 511880.SH)
    # 取 stop_loss_price 最大值作为保守防线
    df_target = df_target.groupby(['sec_code', 'module_tag', 'stop_loss_type']).agg({
        'target_weight': 'sum',
        'stop_loss_price': 'max'
    }).reset_index()

    target_trade_date = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
    df_target.insert(0, 'trade_date', target_trade_date)

    # 精度校验
    total_weight = df_target['target_weight'].sum()
    if not (0.98 <= total_weight <= 1.02):
        raise ValueError(f"目标总权重异常: {total_weight}，停止输出！")

    file_path = os.path.join(output_dir, "target_positions.csv")
    df_target.to_csv(file_path, index=False, encoding='utf-8')

    print(f"\n✅ 目标持仓 CSV 文件最终生成成功: {os.path.abspath(file_path)}")
    print(df_target.to_string())
    return file_path