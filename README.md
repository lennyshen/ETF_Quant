# ETF量化统计系统

基于 Streamlit 的 ETF 量化监控工具，自动跟踪 800+ 只 ETF 的技术指标。

## 功能

- **综合统计表**：显示所有 ETF 的代码、名称、年管理费率、年托管费率、日K线与60日均线关系、周K线MACD值
- **历史数据查询**：通过日期选择框查看任意交易日的完整数据
- **自动更新**：GitHub Actions 每个交易日收盘后自动获取并保存数据
- **筛选搜索**：支持按均线关系、MACD方向筛选，支持代码/名称搜索
- **数据下载**：支持将筛选后的数据导出为 CSV

## 技术指标

| 指标 | 说明 |
|------|------|
| 60日均线关系 | 最新日K收盘价与60日简单移动平均线(SMA)的大小关系 |
| DIF | EMA(12) - EMA(26)，基于周K线收盘价 |
| DEA | DIF 的 9 周 EMA |
| MACD柱 | 2 × (DIF - DEA)，正值为红柱，负值为绿柱 |

## 快速开始

### 本地运行

```bash
# 1. 克隆仓库
git clone https://github.com/<your-username>/etf_quant_streamlit.git
cd etf_quant_streamlit

# 2. 安装依赖
pip install -r requirements.txt

# 3. 运行 Streamlit
streamlit run app.py
```

### 部署到 Streamlit Cloud

1. 将项目推送到 GitHub 仓库
2. 访问 [share.streamlit.io](https://share.streamlit.io)
3. 选择仓库，主文件设为 `app.py`
4. 点击 Deploy

### 数据更新

**手动更新**：在 App 左侧点击「获取 / 更新最新数据」按钮

**自动更新**（需推送到 GitHub）：
- GitHub Actions 每个交易日（周一至周五）北京时间 15:35 自动运行
- 数据保存在 `ETF_Quant.csv`
- 也可在 GitHub Actions 页面手动触发 (workflow_dispatch)

## 项目结构

```
etf_quant_streamlit/
├── app.py                          # Streamlit 主应用
├── data_fetcher.py                 # 数据获取与指标计算
├── etf_config.py                   # ETF代码列表配置
├── update_data.py                  # GitHub Actions 数据更新脚本
├── requirements.txt                # Python依赖
├── ETF_Quant.csv                   # 历史数据（自动生成）
├── etf_fees_cache.json             # 费率缓存（自动生成）
├── .streamlit/
│   └── config.toml                 # Streamlit主题配置
├── .github/
│   └── workflows/
│       └── daily_update.yml        # GitHub Actions自动更新
└── README.md
```

## 数据来源

- 行情数据：东方财富 (via [AKShare](https://github.com/akfamily/akshare))
- 基金费率：东方财富基金概况

## 注意事项

- 首次获取数据耗时较长（约5-15分钟），因为需要获取 800+ 只ETF的K线和费率数据
- 费率数据会本地缓存（`etf_fees_cache.json`），后续更新速度更快
- 数据获取依赖 AKShare 库的东方财富接口，如遇接口变更请更新 AKShare 版本
