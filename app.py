"""
ETF量化统计 - Streamlit App
功能：
1. 显示最新交易日ETF综合统计表（代码、名称、费率、60日均线关系、周MACD）
2. 支持按日期查看历史数据（从GitHub ETF_Quant_Data.csv读取）
3. 支持在线实时获取/刷新最新数据并自动同步至GitHub
"""

import streamlit as st
import pandas as pd
import numpy as np
import os
from datetime import datetime

from github_storage import (
    read_csv_from_github,
    append_data_to_github,
    get_github_token,
    GITHUB_OWNER,
    GITHUB_REPO,
    GITHUB_FILE_PATH,
)

# ---------------------------------------------------------------------------
# 页面配置
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="ETF量化统计",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOCAL_CSV_PATH = os.path.join(BASE_DIR, "ETF_Quant.csv")


# ---------------------------------------------------------------------------
# 数据加载（优先 GitHub，本地作缓存）
# ---------------------------------------------------------------------------
@st.cache_data(ttl=300)
def load_data():
    """
    加载历史数据：
    1. 优先从 GitHub 读取（持久化存储）
    2. 失败则从本地 CSV 读取（缓存）
    """
    token = get_github_token()
    if token:
        try:
            df = read_csv_from_github(token)
            if not df.empty:
                # 同步到本地缓存
                df.to_csv(LOCAL_CSV_PATH, index=False, encoding="utf-8-sig")
                return df
        except Exception as e:
            print(f"[WARN] GitHub 读取失败，使用本地缓存: {e}")

    # 本地 fallback
    if os.path.exists(LOCAL_CSV_PATH):
        return pd.read_csv(LOCAL_CSV_PATH, dtype={"代码": str})
    return pd.DataFrame()


# ---------------------------------------------------------------------------
# 获取并保存数据
# ---------------------------------------------------------------------------
def fetch_and_save_data():
    """获取最新ETF数据 → 保存到本地CSV → 同步到GitHub"""
    from data_fetcher import fetch_all_etf_data, save_to_csv

    progress_bar = st.progress(0, text="正在初始化...")
    status_text = st.empty()

    def progress_callback(pct, msg=""):
        progress_bar.progress(min(pct, 1.0), text=msg)

    try:
        new_data = fetch_all_etf_data(progress_callback=progress_callback)

        if new_data is not None and not new_data.empty:
            # 保存到本地
            save_to_csv(new_data, LOCAL_CSV_PATH)

            # 同步到 GitHub
            progress_bar.progress(0.98, text="正在同步数据到 GitHub...")
            token = get_github_token()
            if token:
                ok = append_data_to_github(token, new_data)
                if ok:
                    github_msg = "已同步至 GitHub"
                else:
                    github_msg = "GitHub 同步失败，数据已保存在本地"
            else:
                github_msg = "未配置 GitHub Token，数据仅保存在本地"

            progress_bar.progress(1.0, text="完成！")
            date_str = new_data["日期"].iloc[0]
            status_text.success(
                f"✅ 数据已更新！共 {len(new_data)} 只ETF，"
                f"日期: {date_str}。{github_msg}"
            )
            load_data.clear()
            st.rerun()
        else:
            status_text.error("❌ 获取数据失败，请稍后重试")
    except Exception as e:
        progress_bar.empty()
        status_text.error(f"❌ 错误: {str(e)}")


# ---------------------------------------------------------------------------
# 数据展示
# ---------------------------------------------------------------------------
def display_date_data(all_data, selected_date):
    """显示指定日期的ETF量化数据"""
    df = all_data[all_data["日期"] == selected_date].copy()

    if df.empty:
        st.warning(f"没有 {selected_date} 的数据")
        return

    st.subheader(f"📈 {selected_date} ETF量化数据")

    # ---------- 概览统计 ----------
    total = len(df)
    above_count = len(df[df["价格与60日均线关系"] == "≥ 60日均线"])
    below_count = len(df[df["价格与60日均线关系"] == "< 60日均线"])

    macd_valid = df["周MACD柱"].notna()
    macd_positive = int((df.loc[macd_valid, "周MACD柱"] > 0).sum())
    macd_negative = int((df.loc[macd_valid, "周MACD柱"] < 0).sum())

    # 信号统计
    ma_cross_col = "均线穿越" if "均线穿越" in df.columns else None
    macd_turn_col = "MACD柱转向" if "MACD柱转向" in df.columns else None
    cross_up = len(df[df[ma_cross_col] == "上穿60日均线"]) if ma_cross_col else 0
    cross_down = len(df[df[ma_cross_col] == "下穿60日均线"]) if ma_cross_col else 0
    turn_green = len(df[df[macd_turn_col] == "红转绿"]) if macd_turn_col else 0
    turn_red = len(df[df[macd_turn_col] == "绿转红"]) if macd_turn_col else 0

    row1_col1, row1_col2, row1_col3, row1_col4, row1_col5 = st.columns(5)
    with row1_col1:
        st.metric("ETF总数", f"{total} 只")
    with row1_col2:
        pct_above = f"{above_count / total * 100:.1f}%" if total > 0 else "0%"
        st.metric("≥ 60日均线", f"{above_count} 只", delta=pct_above)
    with row1_col3:
        pct_below = f"{below_count / total * 100:.1f}%" if total > 0 else "0%"
        st.metric("< 60日均线", f"{below_count} 只", delta=f"-{pct_below}", delta_color="inverse")
    with row1_col4:
        st.metric("周MACD红柱(>0)", f"{macd_positive} 只")
    with row1_col5:
        st.metric("周MACD绿柱(<0)", f"{macd_negative} 只")

    row2_col1, row2_col2, row2_col3, row2_col4, _ = st.columns(5)
    with row2_col1:
        st.metric("上穿60日均线", f"{cross_up} 只", delta="买入信号" if cross_up else None)
    with row2_col2:
        st.metric("下穿60日均线", f"{cross_down} 只", delta="卖出信号" if cross_down else None, delta_color="inverse")
    with row2_col3:
        st.metric("MACD绿转红", f"{turn_red} 只", delta="看多信号" if turn_red else None)
    with row2_col4:
        st.metric("MACD红转绿", f"{turn_green} 只", delta="看空信号" if turn_green else None, delta_color="inverse")

    st.markdown("---")

    # ---------- 筛选 ----------
    frow1_col1, frow1_col2, frow1_col3 = st.columns(3)
    with frow1_col1:
        ma_filter = st.selectbox(
            "60日均线筛选",
            ["全部", "≥ 60日均线", "< 60日均线"],
            key="ma_filter",
        )
    with frow1_col2:
        macd_filter = st.selectbox(
            "周MACD柱筛选",
            ["全部", "红柱 (>0)", "绿柱 (<0)"],
            key="macd_filter",
        )
    with frow1_col3:
        search = st.text_input("🔍 搜索（代码或名称）", "", key="search")

    frow2_col1, frow2_col2, _ = st.columns(3)
    with frow2_col1:
        cross_opts = ["全部", "上穿60日均线", "下穿60日均线"]
        cross_filter = st.selectbox("均线穿越信号", cross_opts, key="cross_filter")
    with frow2_col2:
        turn_opts = ["全部", "绿转红", "红转绿"]
        turn_filter = st.selectbox("MACD柱转向信号", turn_opts, key="turn_filter")

    # 应用筛选
    if ma_filter != "全部":
        df = df[df["价格与60日均线关系"] == ma_filter]
    if macd_filter == "红柱 (>0)":
        df = df[df["周MACD柱"].notna() & (df["周MACD柱"] > 0)]
    elif macd_filter == "绿柱 (<0)":
        df = df[df["周MACD柱"].notna() & (df["周MACD柱"] < 0)]
    if cross_filter != "全部" and "均线穿越" in df.columns:
        df = df[df["均线穿越"] == cross_filter]
    if turn_filter != "全部" and "MACD柱转向" in df.columns:
        df = df[df["MACD柱转向"] == turn_filter]
    if search:
        mask = df["代码"].str.contains(search, na=False) | df["名称"].str.contains(
            search, na=False
        )
        df = df[mask]

    # ---------- 数据表 ----------
    display_cols = [
        "代码",
        "名称",
        "年管理费率",
        "年托管费率",
        "最新收盘价",
        "60日均线",
        "价格与60日均线关系",
        "均线穿越",
        "周MACD_DIF",
        "周MACD_DEA",
        "周MACD柱",
        "MACD柱转向",
    ]
    display_cols = [c for c in display_cols if c in df.columns]

    st.dataframe(
        df[display_cols].reset_index(drop=True),
        use_container_width=True,
        height=min(700, 40 + 35 * len(df)),
        column_config={
            "代码": st.column_config.TextColumn("代码", width="small"),
            "名称": st.column_config.TextColumn("名称", width="medium"),
            "年管理费率": st.column_config.TextColumn("年管理费率", width="small"),
            "年托管费率": st.column_config.TextColumn("年托管费率", width="small"),
            "最新收盘价": st.column_config.NumberColumn("最新收盘价", format="%.4f"),
            "60日均线": st.column_config.NumberColumn("60日均线", format="%.4f"),
            "价格与60日均线关系": st.column_config.TextColumn("价格与均线关系", width="medium"),
            "均线穿越": st.column_config.TextColumn("均线穿越", width="medium"),
            "周MACD_DIF": st.column_config.NumberColumn("DIF", format="%.4f"),
            "周MACD_DEA": st.column_config.NumberColumn("DEA", format="%.4f"),
            "周MACD柱": st.column_config.NumberColumn("MACD柱", format="%.4f"),
            "MACD柱转向": st.column_config.TextColumn("MACD柱转向", width="medium"),
        },
    )

    st.caption(f"共 {len(df)} 条记录")

    # ---------- 下载按钮 ----------
    csv_download = df[display_cols].to_csv(index=False, encoding="utf-8-sig")
    st.download_button(
        label="📥 下载当前筛选数据 (CSV)",
        data=csv_download,
        file_name=f"ETF_Quant_{selected_date}.csv",
        mime="text/csv",
    )


# ---------------------------------------------------------------------------
# 主界面
# ---------------------------------------------------------------------------
def main():
    st.title("📊 ETF量化统计系统")
    st.markdown(
        "<p style='color:gray'>基于日K线60日均线信号 & 周K线MACD(12,26,9)的ETF量化监控</p>",
        unsafe_allow_html=True,
    )
    st.markdown("---")

    # 加载历史数据
    all_data = load_data()

    # ====== 侧边栏 ======
    with st.sidebar:
        st.header("📅 数据控制")

        # 日期选择
        if not all_data.empty and "日期" in all_data.columns:
            dates = sorted(all_data["日期"].unique(), reverse=True)
            selected_date = st.selectbox(
                "选择交易日期",
                dates,
                index=0,
                help="选择要查看的交易日期，默认显示最新数据",
            )
        else:
            selected_date = None
            st.info("暂无历史数据")

        st.markdown("---")

        # 获取数据按钮
        st.markdown("### 🔄 数据更新")
        st.caption("首次使用或需要更新数据时点击下方按钮")
        if st.button("获取 / 更新最新数据", use_container_width=True, type="primary"):
            fetch_and_save_data()

        st.markdown("---")

        # GitHub 状态
        token = get_github_token()
        if token:
            st.success(f"GitHub: {GITHUB_OWNER}/{GITHUB_REPO}", icon="✅")
            st.caption(f"数据文件: {GITHUB_FILE_PATH}")
        else:
            st.warning("未配置 GitHub Token (st.secrets[\"GT\"])", icon="⚠️")

        st.markdown("---")

        # 说明
        st.markdown(
            """
        ### 📖 指标说明
        | 指标 | 含义 |
        |------|------|
        | **60日均线关系** | 最新日K收盘价 vs 60日SMA |
        | **均线穿越** | 最新日收盘价上穿/下穿60日均线 |
        | **DIF** | EMA(12) - EMA(26) |
        | **DEA** | DIF的9周EMA |
        | **MACD柱** | 2 × (DIF - DEA) |
        | **MACD柱转向** | 最新周MACD柱红转绿/绿转红 |
        """
        )

        st.markdown("---")
        st.caption(f"ETF标的数量: {len(__import__('etf_config').ETF_CODES)} 只")
        st.caption("数据来源: 东方财富 via AKShare")
        st.caption("自动更新: 每交易日 16:00 (GitHub Actions)")

    # ====== 主内容区 ======
    if selected_date and not all_data.empty:
        display_date_data(all_data, selected_date)
    else:
        st.markdown(
            """
            <div style='text-align: center; padding: 60px 20px;'>
                <h2>👋 欢迎使用ETF量化统计系统</h2>
                <p style='font-size: 1.2em; color: gray;'>
                    请点击左侧 <b>「获取 / 更新最新数据」</b> 按钮开始
                </p>
                <p style='color: gray;'>
                    首次获取数据可能需要较长时间（约5-15分钟），请耐心等待
                </p>
            </div>
            """,
            unsafe_allow_html=True,
        )


if __name__ == "__main__":
    main()
