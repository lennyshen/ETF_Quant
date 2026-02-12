"""
ETF量化统计 - GitHub 存储模块

通过 GitHub REST API 读写 CSV 数据文件，实现持久化存储。
数据文件位于 GitHub 仓库: {GITHUB_OWNER}/{GITHUB_REPO}/{GITHUB_FILE_PATH}
"""

import base64
import io
import pandas as pd
import requests

# ---------------------------------------------------------------------------
# GitHub 配置（默认值，可被外部覆盖）
# ---------------------------------------------------------------------------
GITHUB_OWNER = "lennyshen"
GITHUB_REPO = "ETF_Quant"
GITHUB_FILE_PATH = "ETF_Quant_Data.csv"
GITHUB_BRANCH = "main"

_API_BASE = "https://api.github.com"
_RAW_BASE = "https://raw.githubusercontent.com"


def _headers(token: str) -> dict:
    return {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json",
    }


# ---------------------------------------------------------------------------
# 1. 读取 GitHub 上的 CSV
# ---------------------------------------------------------------------------

def read_csv_from_github(token: str) -> pd.DataFrame:
    """
    从 GitHub 仓库读取 CSV 文件，返回 DataFrame。
    优先使用 raw.githubusercontent.com（无大小限制），失败则用 Contents API。
    如果文件不存在，返回空 DataFrame。
    """
    # 方法1：直接下载 raw 内容（快，无大小限制）
    raw_url = f"{_RAW_BASE}/{GITHUB_OWNER}/{GITHUB_REPO}/{GITHUB_BRANCH}/{GITHUB_FILE_PATH}"
    headers = {"Authorization": f"token {token}"}
    try:
        resp = requests.get(raw_url, headers=headers, timeout=30)
        if resp.status_code == 200:
            df = pd.read_csv(io.StringIO(resp.text), dtype={"代码": str})
            print(f"[GitHub] 成功读取 {len(df)} 行数据")
            return df
        elif resp.status_code == 404:
            print("[GitHub] 文件不存在，将创建新文件")
            return pd.DataFrame()
        else:
            print(f"[GitHub] raw 读取失败 (HTTP {resp.status_code})，尝试 API...")
    except Exception as e:
        print(f"[GitHub] raw 读取异常: {e}，尝试 API...")

    # 方法2：Contents API（备用，有 1MB 限制）
    api_url = f"{_API_BASE}/repos/{GITHUB_OWNER}/{GITHUB_REPO}/contents/{GITHUB_FILE_PATH}"
    params = {"ref": GITHUB_BRANCH}
    try:
        resp = requests.get(api_url, headers=_headers(token), params=params, timeout=30)
        if resp.status_code == 200:
            content_b64 = resp.json()["content"]
            content = base64.b64decode(content_b64).decode("utf-8-sig")
            df = pd.read_csv(io.StringIO(content), dtype={"代码": str})
            print(f"[GitHub] API 读取成功，{len(df)} 行数据")
            return df
        elif resp.status_code == 404:
            print("[GitHub] 文件不存在，将创建新文件")
            return pd.DataFrame()
        else:
            print(f"[GitHub] API 读取失败: {resp.status_code} {resp.text[:200]}")
            return pd.DataFrame()
    except Exception as e:
        print(f"[GitHub] API 读取异常: {e}")
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# 2. 获取文件 SHA（更新文件时需要）
# ---------------------------------------------------------------------------

def _get_file_sha(token: str) -> str | None:
    """获取 GitHub 上文件的当前 SHA，用于更新操作。文件不存在则返回 None。"""
    api_url = f"{_API_BASE}/repos/{GITHUB_OWNER}/{GITHUB_REPO}/contents/{GITHUB_FILE_PATH}"
    params = {"ref": GITHUB_BRANCH}
    try:
        resp = requests.get(api_url, headers=_headers(token), params=params, timeout=15)
        if resp.status_code == 200:
            return resp.json()["sha"]
        return None
    except Exception:
        return None


# ---------------------------------------------------------------------------
# 3. 写入 / 追加 CSV 到 GitHub
# ---------------------------------------------------------------------------

def write_csv_to_github(token: str, df: pd.DataFrame, message: str = "") -> bool:
    """
    将完整的 DataFrame 写入 GitHub 文件（覆盖）。
    Returns: True 成功 / False 失败
    """
    if df is None or df.empty:
        print("[GitHub] 无数据可写入")
        return False

    if not message:
        message = f"📊 Update ETF data ({len(df)} rows)"

    csv_content = df.to_csv(index=False, encoding="utf-8")
    content_b64 = base64.b64encode(csv_content.encode("utf-8")).decode("ascii")

    sha = _get_file_sha(token)

    api_url = f"{_API_BASE}/repos/{GITHUB_OWNER}/{GITHUB_REPO}/contents/{GITHUB_FILE_PATH}"
    payload = {
        "message": message,
        "content": content_b64,
        "branch": GITHUB_BRANCH,
    }
    if sha:
        payload["sha"] = sha  # 更新已有文件

    try:
        resp = requests.put(api_url, headers=_headers(token), json=payload, timeout=60)
        if resp.status_code in (200, 201):
            action = "更新" if sha else "创建"
            print(f"[GitHub] 文件{action}成功: {GITHUB_FILE_PATH}")
            return True
        else:
            print(f"[GitHub] 写入失败: {resp.status_code} {resp.text[:300]}")
            return False
    except Exception as e:
        print(f"[GitHub] 写入异常: {e}")
        return False


def append_data_to_github(token: str, new_data: pd.DataFrame) -> bool:
    """
    将新数据追加到 GitHub 上的 CSV 文件。
    - 读取现有数据
    - 去重（按日期去重，新数据覆盖旧数据）
    - 合并后写回
    Returns: True 成功 / False 失败
    """
    if new_data is None or new_data.empty:
        print("[GitHub] 无新数据可追加")
        return False

    # 读取现有数据
    existing = read_csv_from_github(token)

    if not existing.empty and "日期" in existing.columns:
        # 去掉现有数据中与新数据同日期的行（新数据覆盖旧数据）
        new_dates = set(new_data["日期"].unique())
        existing = existing[~existing["日期"].isin(new_dates)]
        combined = pd.concat([existing, new_data], ignore_index=True)
    else:
        combined = new_data

    # 按日期排序
    if "日期" in combined.columns:
        combined = combined.sort_values("日期").reset_index(drop=True)

    date_str = new_data["日期"].iloc[0] if "日期" in new_data.columns else "unknown"
    message = f"📊 Update ETF data for {date_str} ({len(new_data)} ETFs)"

    return write_csv_to_github(token, combined, message)


# ---------------------------------------------------------------------------
# 4. 工具函数
# ---------------------------------------------------------------------------

def get_github_token() -> str | None:
    """
    从多个来源获取 GitHub Token:
    1. Streamlit secrets (st.secrets["GT"])
    2. 环境变量 GT
    """
    # 尝试 Streamlit secrets
    try:
        import streamlit as st
        token = st.secrets.get("GT")
        if token:
            return token
    except Exception:
        pass

    # 尝试环境变量
    import os
    token = os.environ.get("GT")
    if token:
        return token

    return None
