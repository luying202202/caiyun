# A股板块资金流向

获取A股市场板块信息、大盘行情、明盘暗盘资金流向的工具。

## 快速开始

### 1. 安装依赖

```bash
pip install flask requests pandas
```

### 2. 启动服务

```bash
python server.py
```

启动后会看到提示：

```
A股板块信息服务器启动: http://localhost:5000
```

### 3. 打开页面

浏览器访问 **http://localhost:5000**

点击页面上的「刷新数据」按钮，即可获取最新数据。

### 4. 停止服务

在终端按 `Ctrl + C` 停止服务。

---

## 命令行方式

如果不需要前端页面，可以直接运行命令行脚本：

```bash
python get_a_stock_sectors.py
```

运行后会在当前目录生成 4 个 CSV 文件（每次运行覆盖）：

| 文件 | 说明 |
|------|------|
| `industry_sectors.csv` | 行业板块（含成交额、涨跌幅、领涨股） |
| `concept_sectors.csv` | 概念板块 |
| `region_sectors.csv` | 地域板块 |
| `market_fund_flow.csv` | 全市场资金流向汇总 |

---

## 页面功能

| 区域 | 内容 |
|------|------|
| 大盘指数 | 上证指数、深证成指、创业板指、科创50 的实时行情 |
| 各市场资金流向 | 沪市主板、深市主板、创业板、科创板、北交所 的明盘/暗盘资金 |
| 全市场资金流向 | 超大单、大单、明盘合计、暗盘(散户) 的流入/流出/净流入 |
| 板块列表 | 行业/概念/地域 三个Tab切换查看 |

---

## 数据来源

| 数据 | 来源 |
|------|------|
| 板块列表 | 搜狐财经 (q.stock.sohu.com) |
| 行业板块行情 | 新浪财经 (vip.stock.finance.sina.com.cn) |
| 个股资金流向 | 东方财富 datacenter |
| 大盘指数行情 | 腾讯财经 (qt.gtimg.cn) |

### 关于明盘/暗盘

- **明盘** = 超大单 + 大单（主力资金）
- **暗盘** = 中单 + 小单（散户资金）

---

## 文件说明

```
caiyun/
├── server.py              # Flask API服务（前端页面入口）
├── static/
│   └── index.html         # 前端页面
├── get_a_stock_sectors.py # 命令行版脚本
└── README.md              # 本文档
```
