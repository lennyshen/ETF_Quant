"""
ETF量化统计 - 每日自动数据更新脚本

由 GitHub Actions 定时执行（每交易日 16:00 北京时间）：
  1. 获取最新ETF量化数据
  2. 通过 GitHub API 追加到 ETF_Quant_Data.csv

也可本地手动运行（需设置环境变量 GT=<github_token>）
"""

import sys
import os
import traceback
from datetime import datetime

# 确保模块路径正确
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from data_fetcher import fetch_all_etf_data, save_to_csv, CSV_PATH
from github_storage import append_data_to_github, get_github_token


def cli_progress(pct, msg=""):
    """命令行进度显示"""
    bar_len = 40
    filled = int(bar_len * pct)
    bar = "█" * filled + "░" * (bar_len - filled)
    print(f"\r  [{bar}] {pct*100:5.1f}%  {msg:<60}", end="", flush=True)
    if pct >= 1.0:
        print()


def main():
    print("=" * 70)
    print("  ETF量化统计 - 每日自动数据更新")
    print(f"  运行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    print()

    # 检查 GitHub Token
    token = get_github_token()
    if not token:
        print("[ERROR] 未找到 GitHub Token！")
        print("  请设置环境变量 GT=<your_github_token>")
        print("  或在 .streamlit/secrets.toml 中配置 GT = \"...\"")
        sys.exit(1)
    print("[INFO] GitHub Token 已配置")

    try:
        # Step 1: 获取数据
        print("\n[1/3] 正在获取ETF数据...")
        df = fetch_all_etf_data(progress_callback=cli_progress)

        if df is None or df.empty:
            print("\n[ERROR] 未获取到任何数据！")
            sys.exit(1)

        date_str = df["日期"].iloc[0]
        print(f"\n[INFO] 成功获取 {len(df)} 只ETF数据，日期: {date_str}")

        # 统计
        above = len(df[df["价格与60日均线关系"] == "≥ 60日均线"])
        below = len(df[df["价格与60日均线关系"] == "< 60日均线"])
        macd_pos = len(df[df["周MACD柱"].notna() & (df["周MACD柱"] > 0)])
        macd_neg = len(df[df["周MACD柱"].notna() & (df["周MACD柱"] < 0)])
        cross_up = len(df[df.get("均线穿越", pd.Series()) == "上穿60日均线"]) if "均线穿越" in df.columns else 0
        cross_down = len(df[df.get("均线穿越", pd.Series()) == "下穿60日均线"]) if "均线穿越" in df.columns else 0

        print(f"[STAT] ≥ 60日均线: {above} 只 | < 60日均线: {below} 只")
        print(f"[STAT] MACD红柱: {macd_pos} 只 | MACD绿柱: {macd_neg} 只")
        print(f"[STAT] 上穿均线: {cross_up} 只 | 下穿均线: {cross_down} 只")

        # Step 2: 保存到本地（备份）
        print(f"\n[2/3] 正在保存本地备份...")
        save_to_csv(df, CSV_PATH)

        # Step 3: 推送到 GitHub
        print(f"\n[3/3] 正在推送数据到 GitHub...")
        ok = append_data_to_github(token, df)
        if ok:
            print("[DONE] 数据已成功推送到 GitHub！")
        else:
            print("[ERROR] GitHub 推送失败！本地备份已保存。")
            sys.exit(1)

    except Exception as e:
        print(f"\n[ERROR] 数据更新失败: {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
