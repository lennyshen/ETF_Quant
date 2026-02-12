"""
ETF量化统计 - 数据获取与计算模块

数据源:
  - ETF名称: akshare fund_name_em()（单次调用，全量基金名称）
  - 日K线/周K线: akshare fund_etf_hist_sina()（每只ETF一次调用，从sina获取）
  - 管理费率/托管费率: 直接抓取东方财富基金概况页面
  
技术指标:
  - 60日均线: 日K线收盘价的60日SMA
  - 周MACD(12,26,9): 从日K线重采样为周K线后计算
"""

import akshare as ak
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import re
import json
import time
import requests
import warnings

warnings.filterwarnings("ignore")

from etf_config import ETF_CODES

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
FEES_CACHE_PATH = os.path.join(BASE_DIR, "etf_fees_cache.json")
CSV_PATH = os.path.join(BASE_DIR, "ETF_Quant.csv")

# HTTP请求头
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}


# ---------------------------------------------------------------------------
# 1. 交易所前缀映射
# ---------------------------------------------------------------------------

def _get_sina_symbol(code: str) -> str:
    """
    将6位ETF代码转换为sina接口所需的带交易所前缀格式
    159xxx -> sz159xxx (深交所)
    其余 -> shXXXXXX (上交所)
    """
    if code.startswith("15"):
        return f"sz{code}"
    else:
        return f"sh{code}"


# ---------------------------------------------------------------------------
# 2. ETF名称（批量，单次API调用）
# ---------------------------------------------------------------------------

def get_etf_name_map() -> dict:
    """
    使用 ak.fund_name_em() 获取全量基金名称映射
    Returns: {code: name} dict
    """
    try:
        df = ak.fund_name_em()
        df["基金代码"] = df["基金代码"].astype(str).str.zfill(6)
        name_map = dict(zip(df["基金代码"], df["基金简称"]))
        return name_map
    except Exception as e:
        print(f"[ERROR] 获取基金名称失败: {e}")
        return {}


# ---------------------------------------------------------------------------
# 3. K线数据（单只ETF，使用sina源）
# ---------------------------------------------------------------------------

def get_etf_daily_kline_sina(code: str) -> pd.DataFrame | None:
    """
    获取单只ETF全部日K线数据（sina源）
    返回的DataFrame列: date, open, high, low, close, volume
    """
    try:
        symbol = _get_sina_symbol(code)
        df = ak.fund_etf_hist_sina(symbol=symbol)
        if df is not None and not df.empty:
            df["close"] = pd.to_numeric(df["close"], errors="coerce")
            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values("date").reset_index(drop=True)
        return df
    except Exception as e:
        return None


def resample_daily_to_weekly(daily_df: pd.DataFrame) -> pd.DataFrame | None:
    """
    将日K线重采样为周K线
    """
    if daily_df is None or daily_df.empty:
        return None
    try:
        df = daily_df.copy()
        df = df.set_index("date")
        weekly = df.resample("W").agg(
            {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}
        ).dropna()
        weekly = weekly.reset_index()
        return weekly
    except Exception:
        return None


# ---------------------------------------------------------------------------
# 4. 基金费率（抓取东方财富基金概况页面）
# ---------------------------------------------------------------------------

def get_fund_fees_scrape(code: str) -> tuple[str, str]:
    """
    从东方财富基金概况页面抓取管理费率和托管费率
    Returns: (管理费率, 托管费率)  e.g. ("0.50%", "0.10%")
    """
    try:
        url = f"http://fundf10.eastmoney.com/jbgk_{code}.html"
        resp = requests.get(url, headers=_HEADERS, timeout=15)
        resp.encoding = "utf-8"
        if resp.status_code != 200:
            return "N/A", "N/A"

        text = resp.text
        mgmt_match = re.search(r"管理费率.*?([\d.]+%)", text)
        custody_match = re.search(r"托管费率.*?([\d.]+%)", text)
        mgmt = mgmt_match.group(1) if mgmt_match else "N/A"
        custody = custody_match.group(1) if custody_match else "N/A"
        return mgmt, custody
    except Exception:
        return "N/A", "N/A"


def load_fees_cache() -> dict:
    """加载本地费率缓存"""
    if os.path.exists(FEES_CACHE_PATH):
        try:
            with open(FEES_CACHE_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_fees_cache(cache: dict):
    """保存费率缓存"""
    try:
        with open(FEES_CACHE_PATH, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[WARN] 保存费率缓存失败: {e}")


# ---------------------------------------------------------------------------
# 5. 技术指标计算
# ---------------------------------------------------------------------------

def calculate_ma60_relation(daily_df: pd.DataFrame):
    """
    计算最新收盘价与60日均线的关系，以及是否发生穿越
    Returns: (latest_close, ma60_value, relation_str, cross_signal)
      cross_signal: "上穿60日均线" / "下穿60日均线" / ""
    """
    if daily_df is None or len(daily_df) < 61:
        if daily_df is not None and len(daily_df) >= 60:
            close = daily_df["close"].astype(float)
            ma60 = close.rolling(window=60).mean()
            latest_close = round(float(close.iloc[-1]), 4)
            latest_ma60 = round(float(ma60.iloc[-1]), 4)
            if np.isnan(latest_ma60):
                return latest_close, None, "N/A", ""
            relation = "≥ 60日均线" if latest_close >= latest_ma60 else "< 60日均线"
            return latest_close, latest_ma60, relation, ""
        return None, None, "N/A", ""

    close = daily_df["close"].astype(float)
    ma60 = close.rolling(window=60).mean()

    latest_close = round(float(close.iloc[-1]), 4)
    latest_ma60 = round(float(ma60.iloc[-1]), 4)

    if np.isnan(latest_ma60):
        return latest_close, None, "N/A", ""

    relation = "≥ 60日均线" if latest_close >= latest_ma60 else "< 60日均线"

    # 穿越检测：比较最近两个交易日收盘价与各自MA60的关系
    prev_close = float(close.iloc[-2])
    prev_ma60 = float(ma60.iloc[-2])

    cross_signal = ""
    if not np.isnan(prev_ma60):
        prev_above = prev_close >= prev_ma60
        curr_above = latest_close >= latest_ma60
        if not prev_above and curr_above:
            cross_signal = "上穿60日均线"
        elif prev_above and not curr_above:
            cross_signal = "下穿60日均线"

    return latest_close, latest_ma60, relation, cross_signal


def calculate_weekly_macd(weekly_df: pd.DataFrame, fast=12, slow=26, signal=9):
    """
    计算周K线MACD (DIF, DEA, MACD柱)，以及MACD柱是否发生红绿转换
    MACD柱 = 2 * (DIF - DEA)
    Returns: (dif, dea, macd_hist, color_change)
      color_change: "红转绿" / "绿转红" / ""
    """
    if weekly_df is None or len(weekly_df) < slow + signal:
        return None, None, None, ""

    close = weekly_df["close"].astype(float)

    ema_fast = close.ewm(span=fast, adjust=False).mean()
    ema_slow = close.ewm(span=slow, adjust=False).mean()

    dif = ema_fast - ema_slow
    dea = dif.ewm(span=signal, adjust=False).mean()
    macd_hist = 2 * (dif - dea)

    curr_macd = float(macd_hist.iloc[-1])
    prev_macd = float(macd_hist.iloc[-2]) if len(macd_hist) >= 2 else 0.0

    # 红绿转换检测
    color_change = ""
    if prev_macd > 0 and curr_macd <= 0:
        color_change = "红转绿"
    elif prev_macd <= 0 and curr_macd > 0:
        color_change = "绿转红"

    return (
        round(float(dif.iloc[-1]), 4),
        round(float(dea.iloc[-1]), 4),
        round(curr_macd, 4),
        color_change,
    )


# ---------------------------------------------------------------------------
# 6. 主获取函数
# ---------------------------------------------------------------------------

def fetch_all_etf_data(progress_callback=None) -> pd.DataFrame | None:
    """
    获取所有ETF的量化数据

    Args:
        progress_callback: 可选回调 (pct: float, msg: str) -> None

    Returns:
        DataFrame:
            日期, 代码, 名称, 年管理费率, 年托管费率,
            最新收盘价, 60日均线, 价格与60日均线关系,
            周MACD_DIF, 周MACD_DEA, 周MACD柱
    """

    def _progress(pct, msg=""):
        if progress_callback:
            progress_callback(min(pct, 0.99), msg)

    total = len(ETF_CODES)

    # ------ Step 1: 获取基金名称（单次API调用） ------
    _progress(0.01, "正在获取ETF名称数据...")
    name_map = get_etf_name_map()
    if not name_map:
        print("[WARN] 无法获取基金名称，将使用代码代替")
    print(f"[INFO] 获取到 {len(name_map)} 只基金名称")

    # ------ Step 2: 批量获取日K线数据（sina源，并发） ------
    _progress(0.05, f"正在获取日K线数据 (共{total}只)...")
    daily_data = {}
    completed_d = [0]

    def _fetch_daily(code):
        result = get_etf_daily_kline_sina(code)
        completed_d[0] += 1
        if completed_d[0] % 50 == 0 or completed_d[0] == total:
            pct = 0.05 + 0.50 * (completed_d[0] / total)
            _progress(pct, f"正在获取日K线数据 ({completed_d[0]}/{total})...")
        return result

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(_fetch_daily, code): code for code in ETF_CODES}
        for future in as_completed(futures):
            code = futures[future]
            try:
                daily_data[code] = future.result()
            except Exception:
                daily_data[code] = None

    success_count = sum(1 for v in daily_data.values() if v is not None and not v.empty)
    print(f"[INFO] 日K线获取成功: {success_count}/{total}")

    # ------ Step 3: 从日K线重采样为周K线 ------
    _progress(0.58, "正在计算周K线数据...")
    weekly_data = {}
    for code in ETF_CODES:
        weekly_data[code] = resample_daily_to_weekly(daily_data.get(code))

    # ------ Step 4: 获取费率数据（带缓存，并发抓取） ------
    _progress(0.62, "正在获取费率数据...")
    fees_cache = load_fees_cache()
    missing_fee_codes = [c for c in ETF_CODES if c not in fees_cache]

    if missing_fee_codes:
        print(f"[INFO] 需要获取 {len(missing_fee_codes)} 只ETF的费率数据...")
        completed_f = [0]

        def _fetch_fee(code):
            mgmt, custody = get_fund_fees_scrape(code)
            completed_f[0] += 1
            if completed_f[0] % 50 == 0 or completed_f[0] == len(missing_fee_codes):
                pct = 0.62 + 0.30 * (completed_f[0] / len(missing_fee_codes))
                _progress(
                    pct,
                    f"正在获取费率数据 ({completed_f[0]}/{len(missing_fee_codes)})...",
                )
            return code, mgmt, custody

        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = [executor.submit(_fetch_fee, code) for code in missing_fee_codes]
            for future in as_completed(futures):
                try:
                    code, mgmt, custody = future.result()
                    fees_cache[code] = {"mgmt": mgmt, "custody": custody}
                except Exception:
                    pass

        save_fees_cache(fees_cache)
        print(f"[INFO] 费率数据已缓存")
    else:
        print(f"[INFO] 费率数据全部命中缓存 ({len(fees_cache)} 条)")

    # ------ Step 5: 构建结果表 ------
    _progress(0.94, "正在计算技术指标并构建结果...")
    results = []

    for code in ETF_CODES:
        name = name_map.get(code, code)
        fees = fees_cache.get(code, {})
        mgmt_fee = fees.get("mgmt", "N/A")
        custody_fee = fees.get("custody", "N/A")

        # 日K线数据
        d_df = daily_data.get(code)

        # 60日均线关系 + 穿越信号
        latest_close, ma60_val, ma60_rel, ma_cross = calculate_ma60_relation(d_df)

        # 周MACD + 红绿转换信号
        w_df = weekly_data.get(code)
        dif, dea, macd, macd_turn = calculate_weekly_macd(w_df)

        # 日期：使用日K线最后一个交易日
        if d_df is not None and not d_df.empty:
            date_str = str(d_df["date"].iloc[-1].strftime("%Y-%m-%d"))
        else:
            date_str = datetime.now().strftime("%Y-%m-%d")

        results.append(
            {
                "日期": date_str,
                "代码": code,
                "名称": name,
                "年管理费率": mgmt_fee,
                "年托管费率": custody_fee,
                "最新收盘价": latest_close,
                "60日均线": ma60_val,
                "价格与60日均线关系": ma60_rel,
                "均线穿越": ma_cross,
                "周MACD_DIF": dif,
                "周MACD_DEA": dea,
                "周MACD柱": macd,
                "MACD柱转向": macd_turn,
            }
        )

    df = pd.DataFrame(results)

    if progress_callback:
        progress_callback(1.0, "数据获取完成！")

    return df


# ---------------------------------------------------------------------------
# 7. CSV读写
# ---------------------------------------------------------------------------

def save_to_csv(new_data, csv_path=None):
    """将新数据追加/更新到CSV文件"""
    if csv_path is None:
        csv_path = CSV_PATH

    if new_data is None or new_data.empty:
        print("[WARN] 没有数据可保存")
        return

    if os.path.exists(csv_path):
        existing = pd.read_csv(csv_path, dtype={"代码": str})
        # 移除当天旧数据
        latest_date = new_data["日期"].iloc[0]
        existing = existing[existing["日期"] != latest_date]
        combined = pd.concat([existing, new_data], ignore_index=True)
    else:
        combined = new_data

    combined.to_csv(csv_path, index=False, encoding="utf-8-sig")
    print(f"[INFO] 数据已保存到 {csv_path}，共 {len(combined)} 条记录")


def load_from_csv(csv_path=None):
    """从CSV加载历史数据"""
    if csv_path is None:
        csv_path = CSV_PATH
    if os.path.exists(csv_path):
        return pd.read_csv(csv_path, dtype={"代码": str})
    return pd.DataFrame()


# ---------------------------------------------------------------------------
# CLI测试入口
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("=" * 60)
    print("ETF量化统计 - 数据获取测试")
    print("=" * 60)

    def cli_progress(pct, msg=""):
        bar_len = 30
        filled = int(bar_len * pct)
        bar = "█" * filled + "░" * (bar_len - filled)
        print(f"\r[{bar}] {pct * 100:.0f}% {msg}", end="", flush=True)
        if pct >= 1.0:
            print()

    df = fetch_all_etf_data(progress_callback=cli_progress)
    if df is not None:
        print(f"\n获取到 {len(df)} 只ETF数据")
        # 显示统计
        valid = df[df["最新收盘价"].notna()]
        print(f"有效K线数据: {len(valid)} 只")
        print(f"有费率数据: {len(df[df['年管理费率'] != 'N/A'])} 只")
        above = len(df[df["价格与60日均线关系"] == "≥ 60日均线"])
        below = len(df[df["价格与60日均线关系"] == "< 60日均线"])
        print(f"≥ 60日均线: {above} 只 | < 60日均线: {below} 只")
        macd_valid = df["周MACD柱"].notna()
        print(f"MACD有效: {macd_valid.sum()} 只")
        print()
        print(df.head(20).to_string())
        save_to_csv(df)
    else:
        print("获取数据失败！")
