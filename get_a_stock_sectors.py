"""
获取A股市场所有板块信息 + 明盘暗盘资金流向
数据源:
  - 搜狐财经: 板块列表(行业/概念/地域)
  - 新浪财经: 行业板块行情(成交额/涨跌幅/领涨股)
  - 东方财富datacenter: 个股资金流向(明盘/暗盘)

明盘 = 超大单 + 大单 (主力资金)
暗盘 = 中单 + 小单 (散户资金)
"""

import os
import sys
import re
import json
import time
import pandas as pd
from datetime import datetime

for key in ["http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY", "all_proxy", "ALL_PROXY"]:
    os.environ.pop(key, None)
os.environ["NO_PROXY"] = "*"

import requests

SESSION = requests.Session()
SESSION.trust_env = False
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
})

PROVINCES = [
    "安徽", "北京", "福建", "甘肃", "广东", "广西", "贵州", "海南", "河北", "河南",
    "黑龙江", "湖北", "湖南", "吉林", "江苏", "江西", "辽宁", "内蒙古", "宁夏", "青海",
    "山东", "山西", "陕西", "上海", "四川", "天津", "西藏", "新疆", "云南", "重庆",
    "浙江",
]

SINA_TO_SOHU_MAP = {
    "交通运输": "交通运输", "传媒娱乐": "传媒", "农林牧渔": "农林牧渔",
    "房地产": "房地产", "有色金属": "有色金属", "汽车制造": "汽车",
    "煤炭行业": "煤炭", "环保行业": "环保", "电子信息": "电子",
    "电子器件": "电子", "钢铁行业": "钢铁", "综合行业": "综合",
    "家电行业": "家用电器", "机械行业": "机械设备", "金融行业": "非银金融",
    "酿酒行业": "食品饮料", "食品行业": "食品饮料", "生物制药": "医药生物",
    "电力行业": "公用事业", "供水供气": "公用事业", "电器行业": "电力设备",
    "发电设备": "电力设备", "化工行业": "基础化工", "农药化肥": "基础化工",
    "化工纤": "基础化工", "石油行业": "石油石化", "纺织行业": "纺织服饰",
    "服装鞋类": "纺织服饰", "医药器械": "医药生物", "商业百货": "商贸零售",
    "物资外贸": "商贸零售", "建筑建材": "建筑材料", "水泥行业": "建筑材料",
    "玻璃行业": "建筑材料", "陶瓷行业": "建筑材料", "公路桥梁": "交通运输",
    "酒店旅游": "社会服务", "飞机制造": "国防军工", "船舶制造": "国防军工",
    "印刷包装": "轻工制造", "造纸行业": "轻工制造", "家具行业": "轻工制造",
    "塑料制品": "轻工制造", "开发区": "综合", "其它行业": "综合",
    "摩托车": "汽车", "仪器仪表": "机械设备", "纺织机械": "机械设备",
    "医疗器械": "医药生物", "次新股": "综合",
}

SOHU_SECTOR_CODE_MAP = {
    "银行": "3124", "计算机": "3117", "通信": "3125",
    "建筑装饰": "3105", "美容护理": "5463",
}


def _sf(val, default=0.0):
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def _si(val, default=0):
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return default


def classify_sectors(all_items):
    industries, concepts, regions = [], [], []
    sw_names = {
        "石油石化", "社会服务", "电力设备", "环保", "纺织服饰", "美容护理",
        "基础化工", "商贸零售", "煤炭", "农林牧渔", "钢铁", "有色金属",
        "电子", "汽车", "食品饮料", "家用电器", "医药生物", "公用事业",
        "房地产", "银行", "非银金融", "建筑材料", "建筑装饰",
        "机械设备", "国防军工", "计算机", "传媒", "通信", "综合",
        "轻工制造", "交通运输",
    }
    for code, name in all_items:
        code_int = int(code)
        name = name.strip()
        is_region = any(p in name for p in PROVINCES) and "板块" in name
        if is_region:
            regions.append((code, name))
        elif 3000 <= code_int < 4000:
            if any(p in name for p in PROVINCES):
                regions.append((code, name))
            else:
                industries.append((code, name))
        elif 5000 <= code_int < 6000:
            if name in sw_names or any(k in name for k in ["制造", "服务", "设备", "金融", "工程"]):
                if not any(k in name for k in ["概念", "题材"]):
                    industries.append((code, name))
                else:
                    concepts.append((code, name))
            else:
                concepts.append((code, name))
        else:
            concepts.append((code, name))
    industries = list(dict.fromkeys(industries))
    concepts = list(dict.fromkeys(concepts))
    regions = list(dict.fromkeys(regions))
    return industries, concepts, regions


def fetch_sohu_board_list():
    url = "https://q.stock.sohu.com/cn/bk_list.shtml"
    for attempt in range(3):
        try:
            r = SESSION.get(url, timeout=20, headers={"Referer": "https://q.stock.sohu.com/"})
            r.encoding = "gb2312"
            items = re.findall(r'href="bk_(\d+)\.shtml"[^>]*>([^<]+)</a>', r.text)
            if items:
                return items
        except Exception:
            pass
        time.sleep(3)
    raise RuntimeError("获取搜狐板块列表失败")


def fetch_sina_industry_detail():
    url = "https://vip.stock.finance.sina.com.cn/q/view/newSinaHy.php"
    for attempt in range(3):
        try:
            r = SESSION.get(url, timeout=15, headers={"Referer": "https://finance.sina.com.cn/"})
            r.encoding = "gbk"
            match = re.search(r'=\s*({.*})', r.text, re.DOTALL)
            if not match:
                continue
            data = json.loads(match.group(1))
            result = {}
            for k, v in data.items():
                f = v.split(",")
                if len(f) >= 13:
                    sohu_name = SINA_TO_SOHU_MAP.get(f[1], f[1])
                    result[sohu_name] = {
                        "新浪名称": f[1],
                        "个股数": _si(f[2]),
                        "均价": _sf(f[3]),
                        "涨跌额": _sf(f[4]),
                        "涨跌幅": _sf(f[5]),
                        "成交量": _si(f[6]),
                        "成交额": _si(f[7]),
                        "领涨股代码": f[8],
                        "领涨股涨跌幅": _sf(f[9]),
                        "领涨股换手率": _sf(f[10]),
                        "领涨股名称": f[12] if len(f) > 12 else "",
                    }
            return result
        except Exception:
            pass
        time.sleep(3)
    return {}


def fetch_stock_fund_flow():
    """
    获取全市场个股资金流向(东方财富datacenter)
    字段: SUPERDEAL_INFLOW/OUTFLOW(超大单), BIGDEAL_INFLOW/OUTFLOW(大单), PRIME_INFLOW(主力净流入)
    明盘=超大+大单, 暗盘=中单+小单
    """
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        "reportName": "RPT_DMSK_TS_STOCKNEW",
        "columns": "SECURITY_CODE,SECURITY_NAME_ABBR,SECUCODE,TRADE_DATE,CLOSE_PRICE,CHANGE_RATE,"
                   "SUPERDEAL_INFLOW,SUPERDEAL_OUTFLOW,BIGDEAL_INFLOW,BIGDEAL_OUTFLOW,"
                   "PRIME_INFLOW,TURNOVERRATE",
        "pageNumber": "1",
        "pageSize": "5000",
        "sortTypes": "-1",
        "sortColumns": "PRIME_INFLOW",
        "source": "WEB",
        "client": "WEB",
    }

    all_stocks = []
    page = 1
    while True:
        params["pageNumber"] = str(page)
        try:
            r = SESSION.get(url, params=params, timeout=15, headers={"Referer": "https://data.eastmoney.com/"})
            d = r.json()
            if not d.get("success"):
                break
            result = d.get("result", {})
            items = result.get("data", [])
            if not items:
                break
            all_stocks.extend(items)
            total = result.get("count", 0)
            fetched = page * 5000
            print(f"  资金流向: 已获取 {min(fetched, total)}/{total} 只个股")
            if fetched >= total:
                break
            page += 1
        except Exception as e:
            print(f"  资金流向请求失败: {e}")
            break
    return all_stocks


def fetch_index_quotes():
    """获取大盘指数行情(腾讯接口)"""
    codes = {
        "sh000001": "上证指数",
        "sz399001": "深证成指",
        "sz399006": "创业板指",
        "sh000688": "科创50",
    }
    code_str = ",".join(codes.keys())
    try:
        r = SESSION.get(f"https://qt.gtimg.cn/q={code_str}", timeout=10)
        result = {}
        for line in r.text.strip().split(";"):
            line = line.strip()
            if not line or "~" not in line:
                continue
            parts = line.split("~")
            if len(parts) < 40:
                continue
            code = parts[2]
            name = codes.get(f"sh{code}" if parts[0].endswith('sh"') else f"sz{code}", parts[1])
            result[name] = {
                "现价": _sf(parts[3]),
                "昨收": _sf(parts[4]),
                "开盘": _sf(parts[5]),
                "涨跌": _sf(parts[31]),
                "涨跌幅": _sf(parts[32]),
                "最高": _sf(parts[33]),
                "最低": _sf(parts[34]),
                "成交额(万)": _sf(parts[37]),
            }
        return result
    except Exception:
        return {}


def compute_market_fund_summary(stocks):
    if not stocks:
        return None, {}
    s_in = s_out = b_in = b_out = prime = 0.0
    for st in stocks:
        s_in += _sf(st.get("SUPERDEAL_INFLOW", 0))
        s_out += _sf(st.get("SUPERDEAL_OUTFLOW", 0))
        b_in += _sf(st.get("BIGDEAL_INFLOW", 0))
        b_out += _sf(st.get("BIGDEAL_OUTFLOW", 0))
        prime += _sf(st.get("PRIME_INFLOW", 0))
    open_in = s_in + b_in
    open_out = s_out + b_out
    total_summary = {
        "超大单流入": s_in, "超大单流出": s_out, "超大单净流入": s_in - s_out,
        "大单流入": b_in, "大单流出": b_out, "大单净流入": b_in - b_out,
        "明盘流入": open_in, "明盘流出": open_out, "明盘净流入": open_in - open_out,
        "主力净流入": prime,
        "暗盘净流入": (open_out - open_in) - prime,
    }

    board_summary = {}
    boards = {
        "沪市主板": ["6"],
        "深市主板": ["0"],
        "创业板": ["3"],
        "科创板": ["68"],
    }
    for bname, prefixes in boards.items():
        bs_in = bs_out = bb_in = bb_out = bprime = 0.0
        bcount = 0
        for st in stocks:
            code = st.get("SECURITY_CODE", "")
            if not code:
                continue
            if any(code.startswith(p) for p in prefixes):
                bs_in += _sf(st.get("SUPERDEAL_INFLOW", 0))
                bs_out += _sf(st.get("SUPERDEAL_OUTFLOW", 0))
                bb_in += _sf(st.get("BIGDEAL_INFLOW", 0))
                bb_out += _sf(st.get("BIGDEAL_OUTFLOW", 0))
                bprime += _sf(st.get("PRIME_INFLOW", 0))
                bcount += 1
        bo_in = bs_in + bb_in
        bo_out = bs_out + bb_out
        board_summary[bname] = {
            "count": bcount,
            "超大单净流入": bs_in - bs_out,
            "大单净流入": bb_in - bb_out,
            "明盘流入": bo_in, "明盘流出": bo_out,
            "明盘净流入": bo_in - bo_out,
            "主力净流入": bprime,
            "暗盘净流入": (bo_out - bo_in) - bprime,
        }
    board_summary["北交所"] = {
        "count": 0, "note": "数据源不含北交所个股",
        "超大单净流入": 0, "大单净流入": 0,
        "明盘流入": 0, "明盘流出": 0, "明盘净流入": 0,
        "主力净流入": 0, "暗盘净流入": 0,
    }

    return total_summary, board_summary


def _display_width(s):
    w = 0
    for ch in s:
        if '\u4e00' <= ch <= '\u9fff' or ch in '，。、；：？！""''（）【】《》—…万亿':
            w += 2
        else:
            w += 1
    return w


def _pad(s, width, align='left'):
    dw = _display_width(s)
    pad = max(0, width - dw)
    if align == 'right':
        return ' ' * pad + s
    elif align == 'center':
        return ' ' * (pad // 2) + s + ' ' * (pad - pad // 2)
    return s + ' ' * pad


def fmt(val, width=14):
    if abs(val) >= 1e8:
        s = f"{val/1e8:.2f}亿"
    elif abs(val) >= 1e4:
        s = f"{val/1e4:.2f}万"
    else:
        s = f"{val:.2f}"
    return _pad(s, width, 'right')


def main():
    t0 = datetime.now()
    print(f"A股板块信息 + 大盘行情 + 明盘暗盘资金流向 - {t0.strftime('%Y-%m-%d %H:%M:%S')}\n")

    print("[1/4] 获取板块列表 (搜狐)...")
    all_items = fetch_sohu_board_list()
    industries, concepts, regions = classify_sectors(all_items)
    print(f"  行业 {len(industries)}, 概念 {len(concepts)}, 地域 {len(regions)}")

    print("\n[2/4] 获取行业板块行情 (新浪)...")
    sina_detail = fetch_sina_industry_detail()
    print(f"  获取到 {len(sina_detail)} 个行业行情数据")

    print("\n[3/4] 获取个股资金流向 (东方财富)...")
    stocks = fetch_stock_fund_flow()
    summary, board_summary = compute_market_fund_summary(stocks)
    print(f"  获取到 {len(stocks)} 只个股资金数据")

    print("\n[4/4] 获取大盘指数行情 (腾讯)...")
    index_quotes = fetch_index_quotes()
    print(f"  获取到 {len(index_quotes)} 个指数行情")

    industry_rows = []
    for code, name in industries:
        row = {"板块代码": code, "板块名称": name}
        m = sina_detail.get(name)
        if m:
            row.update({
                "个股数": m["个股数"], "涨跌幅(%)": round(m["涨跌幅"], 2),
                "成交额(亿)": round(m["成交额"] / 1e8, 2) if m["成交额"] else 0,
                "领涨股": m["领涨股名称"], "领涨股涨跌幅(%)": round(m["领涨股涨跌幅"], 2),
            })
        else:
            row.update({"个股数": 0, "涨跌幅(%)": 0, "成交额(亿)": 0, "领涨股": "", "领涨股涨跌幅(%)": 0})
        industry_rows.append(row)

    industry_df = pd.DataFrame(industry_rows)
    concept_df = pd.DataFrame(concepts, columns=["板块代码", "板块名称"])
    region_df = pd.DataFrame(regions, columns=["板块代码", "板块名称"])

    if index_quotes:
        total_amount_yi = 0.0
        print("\n" + "=" * 70)
        print("大盘指数行情")
        print("-" * 70)
        iw, nw, pw = 12, 10, 10
        print(f"{_pad('指数', iw)} {_pad('现价', pw, 'right')} {_pad('涨跌', pw, 'right')} {_pad('涨跌幅%', pw, 'right')} {_pad('成交额(亿)', pw, 'right')}")
        print("-" * 70)
        for iname, idata in index_quotes.items():
            amt_yi = idata["成交额(万)"] / 1e4
            total_amount_yi += amt_yi
            p_now = f'{idata["现价"]:.2f}'
            p_chg = f'{idata["涨跌"]:.2f}'
            p_pct = f'{idata["涨跌幅"]:.2f}'
            p_amt = f'{amt_yi:.2f}'
            print(f"{_pad(iname, iw)} {_pad(p_now, pw, 'right')} {_pad(p_chg, pw, 'right')} {_pad(p_pct, pw, 'right')} {_pad(p_amt, pw, 'right')}")
        print(f"\n  总成交额(沪深合计): {total_amount_yi:.2f} 亿")

    if board_summary:
        print("\n" + "=" * 70)
        print("各市场明盘暗盘资金流向")
        print("-" * 70)
        bw, cw, rw = 12, 8, 14
        print(f"{_pad('市场', bw)} {_pad('个股', cw, 'right')} {_pad('明盘流入', rw, 'right')} {_pad('明盘流出', rw, 'right')} {_pad('主力净流入', rw, 'right')} {_pad('暗盘净流入', rw, 'right')}")
        print("-" * 70)
        for bname, bdata in board_summary.items():
            print(f"{_pad(bname, bw)} {_pad(str(bdata['count']), cw, 'right')} {fmt(bdata['明盘流入'], rw)} {fmt(bdata['明盘流出'], rw)} {fmt(bdata['主力净流入'], rw)} {fmt(bdata['暗盘净流入'], rw)}")

    print("\n" + "=" * 70)
    print(f"行业板块 ({len(industry_df)} 个) - 按成交额排序")
    show = industry_df.sort_values("成交额(亿)", ascending=False)
    print(show.head(20).to_string(index=False))

    print("\n" + "=" * 70)
    print(f"概念板块 ({len(concept_df)} 个) - 前20")
    print(concept_df.head(20).to_string(index=False))

    print("\n" + "=" * 70)
    print(f"地域板块 ({len(region_df)} 个)")
    print(region_df.to_string(index=False))

    if summary:
        print("\n" + "=" * 70)
        print("全市场明盘暗盘资金流向")
        print("-" * 70)
        lw, rw = 18, 14
        print(f"{_pad('', lw)} {_pad('流入', rw, 'center')} {_pad('流出', rw, 'center')} {_pad('净流入', rw, 'center')}")
        print("-" * 70)
        print(f"{_pad('超大单(明盘)', lw)} {fmt(summary['超大单流入'], rw)} {fmt(summary['超大单流出'], rw)} {fmt(summary['超大单净流入'], rw)}")
        print(f"{_pad('大单(明盘)', lw)} {fmt(summary['大单流入'], rw)} {fmt(summary['大单流出'], rw)} {fmt(summary['大单净流入'], rw)}")
        print(f"{_pad('明盘合计', lw)} {fmt(summary['明盘流入'], rw)} {fmt(summary['明盘流出'], rw)} {fmt(summary['明盘净流入'], rw)}")
        print(f"{_pad('暗盘(散户)', lw)} {_pad('', rw)} {_pad('', rw)} {fmt(summary['暗盘净流入'], rw)}")
        print("-" * 70)
        print(f"{_pad('主力净流入合计', lw)} {_pad('', rw)} {_pad('', rw)} {fmt(summary['主力净流入'], rw)}")

    industry_df.to_csv("industry_sectors.csv", index=False, encoding="utf-8-sig")
    concept_df.to_csv("concept_sectors.csv", index=False, encoding="utf-8-sig")
    region_df.to_csv("region_sectors.csv", index=False, encoding="utf-8-sig")
    if summary:
        pd.DataFrame([summary]).to_csv("market_fund_flow.csv", index=False, encoding="utf-8-sig")

    print(f"\n耗时: {(datetime.now()-t0).total_seconds():.1f}秒")
    print("完成！")


if __name__ == "__main__":
    main()
