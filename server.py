"""
A股板块信息 + 资金流向 Flask API 服务
启动: python server.py
访问: http://localhost:5000
"""

import os
import sys
import re
import json
import time
import logging
import threading
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed, wait
from flask import Flask, jsonify, send_from_directory, request
from dotenv import load_dotenv

load_dotenv()

for key in ["http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY", "all_proxy", "ALL_PROXY"]:
    os.environ.pop(key, None)
os.environ["NO_PROXY"] = "*"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("caiyun")

import requests
import pandas as pd

app = Flask(__name__, static_folder="static", static_url_path="/static")

SESSION = requests.Session()
SESSION.trust_env = False
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
})

PROVINCES = [
    "安徽", "北京", "福建", "甘肃", "广东", "广西", "贵州", "海南", "河北", "河南",
    "黑龙江", "湖北", "湖南", "吉林", "江苏", "江西", "辽宁", "内蒙古", "宁夏", "青海",
    "山东", "山西", "陕西", "上海", "四川", "天津", "西藏", "新疆", "云南", "重庆", "浙江",
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
    "传媒": "3098", "非银金融": "3100", "机械设备": "3101",
    "家用电器": "3102", "建筑材料": "3103", "建筑装饰": "3105",
    "国防军工": "3106", "交通运输": "3107", "汽车": "3109",
    "轻工制造": "3110", "公用事业": "3111", "综合": "3114",
    "医药生物": "3116", "计算机": "3117", "房地产": "3118",
    "有色金属": "3119", "农林牧渔": "3120", "钢铁": "3121",
    "食品饮料": "3122", "电子": "3123", "银行": "3124",
    "通信": "3125", "石油石化": "5458", "社会服务": "5459",
    "电力设备": "5460", "环保": "5461", "纺织服饰": "5462",
    "美容护理": "5463", "基础化工": "5464", "商贸零售": "5465",
    "煤炭": "5466",
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


def fmt_val(v):
    if abs(v) >= 1e8:
        return f"{v/1e8:.2f}亿"
    elif abs(v) >= 1e4:
        return f"{v/1e4:.2f}万"
    return f"{v:.2f}"


SEARCHAPI_TOKEN = "D43BF722C8E33BDC906FB84D85E326E8"
_security_search_cache = {"data": {}, "lock": threading.Lock()}


def _infer_security_market(code):
    code = str(code or "").strip()
    if code.startswith(("4", "8")) or code.startswith("920"):
        return {"tencent": "bj", "eastmoney": "0"}
    if code.startswith(("5", "6", "9", "11")):
        return {"tencent": "sh", "eastmoney": "1"}
    return {"tencent": "sz", "eastmoney": "0"}


def _tencent_stock_prefix(symbol):
    return _infer_security_market(symbol)["tencent"]


def _eastmoney_secid(symbol):
    market = _infer_security_market(symbol)
    return f"{market['eastmoney']}.{symbol}"


def _normalize_security_type(row):
    name = str(row.get("Name", "") or "")
    classify = str(row.get("Classify", "") or "")
    sec_name = str(row.get("SecurityTypeName", "") or "")
    upper_name = name.upper()
    if classify == "Fund":
        if "ETF" in upper_name:
            return "ETF"
        if "LOF" in upper_name:
            return "LOF"
        if "REIT" in upper_name:
            return "REIT"
        return "基金"
    if classify == "AStock":
        return "股票"
    return sec_name or classify or "证券"


def _fetch_security_candidates(query, count=20):
    query = str(query or "").strip()
    if not query:
        return []
    count = max(5, min(int(count or 20), 30))
    cache_key = f"{query}|{count}"
    now_ts = time.time()
    with _security_search_cache["lock"]:
        cached = _security_search_cache["data"].get(cache_key)
        if cached and now_ts - cached["time"] < 30:
            return list(cached["items"])
    items = []
    try:
        r = SESSION.get(
            "https://searchapi.eastmoney.com/api/suggest/get",
            params={
                "input": query,
                "type": "14",
                "token": SEARCHAPI_TOKEN,
                "count": str(count),
            },
            timeout=10,
            headers={"Referer": "https://quote.eastmoney.com/"},
        )
        rows = r.json().get("QuotationCodeTable", {}).get("Data") or []
        seen = set()
        for row in rows:
            code = str(row.get("Code", "") or "").strip()
            if not _CODE_PATTERN.match(code):
                continue
            classify = str(row.get("Classify", "") or "")
            if classify not in {"AStock", "Fund"}:
                continue
            if code in seen:
                continue
            seen.add(code)
            items.append({
                "code": code,
                "name": str(row.get("Name", "") or ""),
                "pinyin": str(row.get("PinYin", "") or ""),
                "quote_id": str(row.get("QuoteID", "") or ""),
                "classify": classify,
                "security_type": _normalize_security_type(row),
            })
    except Exception as e:
        log.warning("证券搜索联想失败: %s", e)
    with _security_search_cache["lock"]:
        _security_search_cache["data"][cache_key] = {"time": now_ts, "items": list(items)}
    return items


def _lookup_security_meta(stock_code):
    if not _CODE_PATTERN.match(stock_code):
        return None
    candidates = _fetch_security_candidates(stock_code, count=12)
    for item in candidates:
        if item.get("code") == stock_code:
            return item
    return None


def _parse_tencent_line(line):
    if not line or "~" not in line:
        return None
    parts = line.split("~")
    if len(parts) < 40 or not parts[3]:
        return None
    return {
        "raw_code": parts[2],
        "name": parts[1],
        "price": _sf(parts[3]),
        "prev_close": _sf(parts[4]),
        "open": _sf(parts[5]) if len(parts) > 5 else 0,
        "change_pct": _sf(parts[32]),
        "change": _sf(parts[31]),
        "amount": _sf(parts[37]),
        "volume": _sf(parts[36]) if len(parts) > 36 else 0,
        "turnover": _sf(parts[38]) if len(parts) > 38 else 0,
        "high": _sf(parts[33]) if len(parts) > 33 else 0,
        "low": _sf(parts[34]) if len(parts) > 34 else 0,
    }


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
        except Exception as e:
            log.warning("搜狐板块列表获取失败(第%d次): %s", attempt + 1, e)
        time.sleep(3)
    log.warning("搜狐板块列表全部失败，使用SOHU_SECTOR_CODE_MAP作为fallback")
    return [(code, name) for name, code in SOHU_SECTOR_CODE_MAP.items()]


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
                        "个股数": _si(f[2]),
                        "涨跌幅": _sf(f[5]),
                        "成交额": _si(f[7]),
                        "领涨股名称": f[12] if len(f) > 12 else "",
                        "领涨股涨跌幅": _sf(f[9]),
                    }
            return result
        except Exception as e:
            log.warning("新浪行业数据获取失败(第%d次): %s", attempt + 1, e)
        time.sleep(3)
    return {}


def _fetch_tencent_batch(codes):
    result = {}
    qt_codes = []
    code_map = {}
    for code in codes:
        c = str(code).strip()
        if c.startswith("sh") or c.startswith("sz"):
            qt = c
            raw = c[2:]
        elif c.startswith("bj"):
            qt = c
            raw = c[2:]
        else:
            qt = f"{_tencent_stock_prefix(c)}{c}"
            raw = c
        qt_codes.append(qt)
        code_map[qt] = raw
    for i in range(0, len(qt_codes), 50):
        batch = qt_codes[i:i + 50]
        try:
            r = SESSION.get(f"https://qt.gtimg.cn/q={','.join(batch)}", timeout=10)
            for line in r.text.strip().split(";"):
                line = line.strip()
                parsed = _parse_tencent_line(line)
                if not parsed:
                    continue
                raw_code = parsed["raw_code"]
                code = code_map.get(raw_code, raw_code)
                result[code] = parsed
        except Exception as e:
            log.error("腾讯行情批量获取失败: %s", e)
    return result


def fetch_stock_fund_flow():
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    page_size = 500
    params = {
        "reportName": "RPT_DMSK_TS_STOCKNEW",
        "columns": "SECURITY_CODE,SECURITY_NAME_ABBR,SECUCODE,"
                   "SUPERDEAL_INFLOW,SUPERDEAL_OUTFLOW,BIGDEAL_INFLOW,BIGDEAL_OUTFLOW,"
                   "PRIME_INFLOW,CHANGE_RATE,CLOSE_PRICE,TURNOVERRATE,"
                   "PE_DYNAMIC,ORG_PARTICIPATE,RATIO,RATIO_3DAYS,RATIO_50DAYS,"
                   "TOTALSCORE,RANK_UP,RANK,FOCUS,BUY_SUPERDEAL_RATIO,BUY_BIGDEAL_RATIO",
        "pageNumber": "1", "pageSize": str(page_size),
        "sortTypes": "-1", "sortColumns": "PRIME_INFLOW",
        "source": "WEB", "client": "WEB",
    }
    all_stocks = []
    page = 1
    total_count = 0
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
            total_count = result.get("count", 0)
            if len(items) < page_size or (total_count and len(all_stocks) >= total_count):
                break
            page += 1
        except Exception:
            break
    log.info("个股资金流 %d/%d (%d页)", len(all_stocks), total_count, page)

    qt_codes = []
    for st in all_stocks:
        code = st.get("SECURITY_CODE", "")
        qt_codes.append(f"{_tencent_stock_prefix(code)}{code}")

    if qt_codes:
        real_quotes = _fetch_tencent_batch(qt_codes)
        for st in all_stocks:
            code = st.get("SECURITY_CODE", "")
            q = real_quotes.get(code)
            if q and q["price"] > 0:
                st["CLOSE_PRICE_REAL"] = q["price"]
                st["CHANGE_RATE_REAL"] = q["change_pct"]
                st["TURNOVERRATE_REAL"] = q["turnover"]
                st["AMOUNT_REAL"] = q["amount"]
                st["VOLUME_REAL"] = q["volume"]
                st["HIGH_REAL"] = q["high"]
                st["LOW_REAL"] = q["low"]
                st["OPEN_REAL"] = q["open"]
                st["PREV_CLOSE_REAL"] = q["prev_close"]

    return all_stocks


def fetch_index_quotes():
    codes = {"sh000001": "上证指数", "sz399001": "深证成指", "sz399006": "创业板指", "sh000688": "科创50"}
    result = {}
    try:
        r = SESSION.get(f"https://qt.gtimg.cn/q={','.join(codes.keys())}", timeout=10)
        for line in r.text.strip().split(";"):
            line = line.strip()
            if not line or "~" not in line:
                continue
            parts = line.split("~")
            if len(parts) < 40:
                continue
            name = parts[1]
            result[name] = {
                "现价": _sf(parts[3]), "昨收": _sf(parts[4]),
                "涨跌": _sf(parts[31]), "涨跌幅": _sf(parts[32]),
                "成交额万": _sf(parts[37]),
            }
        if len(result) >= 4:
            return result
    except Exception:
        pass
    indices = [
        ("1.000001", "上证指数"), ("0.399001", "深证成指"),
        ("0.399006", "创业板指"), ("1.000688", "科创50"),
    ]
    for secid, name in indices:
        if name in result:
            continue
        try:
            r = SESSION.get(
                f"https://push2.eastmoney.com/api/qt/stock/get?secid={secid}"
                f"&fields=f43,f44,f45,f46,f47,f48,f57,f58,f169,f170,f171",
                timeout=10, headers={"Referer": "https://quote.eastmoney.com/"},
            )
            d = r.json().get("data", {})
            if d and d.get("f43"):
                result[name] = {
                    "现价": round(d["f43"] / 100, 2),
                    "昨收": round(d["f44"] / 100, 2),
                    "涨跌": round(d["f169"] / 100, 2),
                    "涨跌幅": round(d["f170"] / 100, 2),
                    "成交额万": round(d.get("f48", 0) / 1e4, 2),
                }
        except Exception:
            pass
    return result


def fetch_sohu_sector_stocks(sector_code):
    url = f"https://q.stock.sohu.com/cn/bk_{sector_code}.shtml"
    try:
        r = SESSION.get(url, timeout=15, headers={"Referer": "https://q.stock.sohu.com/"})
        r.encoding = "gb2312"
        codes = list(set(re.findall(r'href="/cn/(\d{6})', r.text)))
        if codes:
            return codes
    except Exception:
        pass
    return _fetch_eastmoney_sector_stocks(sector_code)


def _fetch_eastmoney_sector_stocks(sector_code):
    url = "https://push2.eastmoney.com/api/qt/clist/get"
    params = {
        "fs": f"b:{sector_code}f:!50",
        "fields": "f12",
        "pn": "1",
        "pz": "500",
    }
    try:
        r = SESSION.get(url, params=params, timeout=15, headers={"Referer": "https://quote.eastmoney.com/"})
        d = r.json()
        items = d.get("data", {}).get("diff", [])
        return [it.get("f12", "") for it in items if it.get("f12")]
    except Exception as e:
        log.warning("东方财富板块成分股获取失败(bk_%s): %s", sector_code, e)
        return []


def compute_industry_fund_flow(industries, stocks, sina_detail, collect_sector_map=False):
    if not stocks:
        return ([], {}) if collect_sector_map else []
    stock_map = {}
    for st in stocks:
        code = st.get("SECURITY_CODE", "")
        if code:
            stock_map[code] = st
    sector_stocks_cache = {}
    stock_sector_map = {}
    rows = []
    for sec_code, name in industries:
        row = {"code": sec_code, "name": name}
        sohu_code = SOHU_SECTOR_CODE_MAP.get(name)
        m_sina = sina_detail.get(name)
        if sohu_code and sohu_code not in sector_stocks_cache:
            sector_stocks_cache[sohu_code] = fetch_sohu_sector_stocks(sohu_code)
        member_codes = sector_stocks_cache.get(sohu_code, []) if sohu_code else []
        matched = [c for c in member_codes if c in stock_map] if member_codes else []
        if collect_sector_map:
            for c in matched:
                stock_sector_map.setdefault(c, []).append(name)
        if m_sina:
            row["count"] = m_sina["个股数"]
            row["change_pct"] = round(m_sina["涨跌幅"], 2)
            row["amount_yi"] = round(m_sina["成交额"] / 1e8, 2) if m_sina["成交额"] else 0
            row["lead_stock"] = m_sina["领涨股名称"]
            row["lead_pct"] = round(m_sina["领涨股涨跌幅"], 2)
        elif matched:
            rates = [_sf(stock_map[c].get("CHANGE_RATE_REAL", 0) or stock_map[c].get("CHANGE_RATE", 0)) for c in matched]
            best_idx = max(range(len(rates)), key=lambda i: rates[i]) if rates else 0
            row["count"] = len(member_codes)
            row["change_pct"] = round(sum(rates) / len(rates), 2) if rates else 0
            row["amount_yi"] = 0
            row["lead_stock"] = stock_map[matched[best_idx]].get("SECURITY_NAME_ABBR", "") if matched else ""
            row["lead_pct"] = round(rates[best_idx], 2) if rates else 0
        else:
            row.update({"count": 0, "change_pct": 0, "amount_yi": 0, "lead_stock": "", "lead_pct": 0})
        if matched:
            s_in = s_out = b_in = b_out = prime = 0.0
            for c in matched:
                st = stock_map[c]
                s_in += _sf(st.get("SUPERDEAL_INFLOW", 0))
                s_out += _sf(st.get("SUPERDEAL_OUTFLOW", 0))
                b_in += _sf(st.get("BIGDEAL_INFLOW", 0))
                b_out += _sf(st.get("BIGDEAL_OUTFLOW", 0))
                prime += _sf(st.get("PRIME_INFLOW", 0))
            open_in, open_out = s_in + b_in, s_out + b_out
            row["明盘流入"] = open_in
            row["明盘流出"] = open_out
            row["明盘净流入"] = open_in - open_out
            row["主力净流入"] = prime
            row["暗盘净流入"] = (open_out - open_in) - prime
        else:
            row["明盘流入"] = 0.0
            row["明盘流出"] = 0.0
            row["明盘净流入"] = 0.0
            row["主力净流入"] = 0.0
            row["暗盘净流入"] = 0.0
        rows.append(row)
    return (rows, stock_sector_map) if collect_sector_map else rows


def compute_fund_summary(stocks):
    if not stocks:
        return None, {}
    s_in = s_out = b_in = b_out = prime = 0.0
    for st in stocks:
        s_in += _sf(st.get("SUPERDEAL_INFLOW", 0))
        s_out += _sf(st.get("SUPERDEAL_OUTFLOW", 0))
        b_in += _sf(st.get("BIGDEAL_INFLOW", 0))
        b_out += _sf(st.get("BIGDEAL_OUTFLOW", 0))
        prime += _sf(st.get("PRIME_INFLOW", 0))
    open_in, open_out = s_in + b_in, s_out + b_out
    total = {
        "超大单流入": s_in, "超大单流出": s_out, "超大单净流入": s_in - s_out,
        "大单流入": b_in, "大单流出": b_out, "大单净流入": b_in - b_out,
        "明盘流入": open_in, "明盘流出": open_out, "明盘净流入": open_in - open_out,
        "主力净流入": prime, "暗盘净流入": (open_out - open_in) - prime,
    }
    board_summary = {}
    boards = {"沪市主板": ["6"], "深市主板": ["0"], "创业板": ["3"], "科创板": ["68"]}
    for bname, prefixes in boards.items():
        bs_in = bs_out = bb_in = bb_out = bprime = 0.0
        bcount = 0
        for st in stocks:
            code = st.get("SECURITY_CODE", "")
            if code and any(code.startswith(p) for p in prefixes):
                bs_in += _sf(st.get("SUPERDEAL_INFLOW", 0))
                bs_out += _sf(st.get("SUPERDEAL_OUTFLOW", 0))
                bb_in += _sf(st.get("BIGDEAL_INFLOW", 0))
                bb_out += _sf(st.get("BIGDEAL_OUTFLOW", 0))
                bprime += _sf(st.get("PRIME_INFLOW", 0))
                bcount += 1
        bo_in, bo_out = bs_in + bb_in, bs_out + bb_out
        board_summary[bname] = {
            "count": bcount,
            "明盘流入": bo_in, "明盘流出": bo_out,
            "主力净流入": bprime, "暗盘净流入": (bo_out - bo_in) - bprime,
        }
    board_summary["北交所"] = {
        "count": 0,
        "明盘流入": 0.0, "明盘流出": 0.0,
        "主力净流入": 0.0, "暗盘净流入": 0.0,
        "note": "数据源不含北交所个股",
    }
    return total, board_summary


CACHE_INTERVAL = int(os.getenv("CACHE_INTERVAL", "60"))
SERVER_PORT = int(os.getenv("SERVER_PORT", "5000"))
_cached_data = {"data": None, "lock": threading.Lock(), "refreshing": False}
_picks520_cache = {"data": None, "lock": threading.Lock(), "time": None, "refresh_count": 0}
_picks_cache = {"data": None, "lock": threading.Lock(), "time": None, "refresh_count": 0}
_surveillance_cache = {"data": None, "lock": threading.Lock(), "time": None}
PICKS_CACHE_REFRESH_INTERVAL = 5
PICKS_CACHE_TTL = 300
SURVEILLANCE_CACHE_TTL = 900

import db as db_module


_CODE_PATTERN = re.compile(r"^\d{6}$")
_SECTOR_CODE_PATTERN = re.compile(r"^\d{3,6}$")
_rate_limit_lock = threading.Lock()
_rate_limit_map = {}


def _check_rate_limit(key, max_calls=30, window=60):
    now = time.time()
    with _rate_limit_lock:
        calls = _rate_limit_map.get(key, [])
        calls = [t for t in calls if now - t < window]
        if len(calls) >= max_calls:
            return False
        calls.append(now)
        _rate_limit_map[key] = calls
    return True


def _get_current_user():
    auth = request.headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        token_str = auth[7:]
        user = db_module.get_user_from_token(token_str)
        if user:
            return user
    return None


def _require_user():
    user = _get_current_user()
    if not user:
        return None, jsonify({"success": False, "error": "请先登录"}), 401
    return user, None, None


@app.route("/api/auth/register", methods=["POST"])
def api_register():
    d = request.get_json(silent=True) or {}
    username = (d.get("username") or "").strip()
    password = (d.get("password") or "").strip()
    result, err = db_module.register_user(username, password)
    if err:
        return jsonify({"success": False, "error": err})
    return jsonify({"success": True, "data": result})


@app.route("/api/auth/login", methods=["POST"])
def api_login():
    d = request.get_json(silent=True) or {}
    username = (d.get("username") or "").strip()
    password = (d.get("password") or "").strip()
    result, err = db_module.login_user(username, password)
    if err:
        return jsonify({"success": False, "error": err})
    return jsonify({"success": True, "data": result})


@app.route("/api/auth/me", methods=["GET"])
def api_me():
    user = _get_current_user()
    if not user:
        return jsonify({"success": False, "error": "未登录"})
    return jsonify({"success": True, "data": user})


_PUBLIC_PATHS = {"/", "/api/auth/login", "/api/auth/register"}


@app.before_request
def _check_login():
    if request.path.startswith("/static/") or request.path.endswith((".css", ".js", ".ico", ".png", ".jpg")):
        return None
    if request.path in _PUBLIC_PATHS:
        return None
    if request.path.startswith("/api/auth/"):
        return None
    if request.path == "/test520":
        return None
    user = _get_current_user()
    if not user:
        return jsonify({"success": False, "error": "请先登录", "need_login": True}), 401


def _require_admin():
    user = _get_current_user()
    if not user:
        return None, jsonify({"success": False, "error": "请先登录"}), 401
    if not user.get("is_admin"):
        return None, jsonify({"success": False, "error": "需要管理员权限"}), 403
    return user, None, None


@app.route("/api/admin/users", methods=["GET"])
def admin_list_users():
    user, err, code = _require_admin()
    if err:
        return err, code
    users = db_module.list_users()
    return jsonify({"success": True, "users": users})


@app.route("/api/admin/users/<int:uid>/toggle", methods=["POST"])
def admin_toggle_user(uid):
    user, err, code = _require_admin()
    if err:
        return err, code
    ok, err2 = db_module.toggle_user_disabled(uid, True)
    if not ok:
        return jsonify({"success": False, "error": err2 or "操作失败"})
    return jsonify({"success": True})


@app.route("/api/admin/users/<int:uid>/enable", methods=["POST"])
def admin_enable_user(uid):
    user, err, code = _require_admin()
    if err:
        return err, code
    ok, err2 = db_module.toggle_user_disabled(uid, False)
    if not ok:
        return jsonify({"success": False, "error": err2 or "操作失败"})
    return jsonify({"success": True})


@app.route("/api/admin/users/<int:uid>/delete", methods=["POST"])
def admin_delete_user(uid):
    user, err, code = _require_admin()
    if err:
        return err, code
    ok, err2 = db_module.delete_user(uid)
    if not ok:
        return jsonify({"success": False, "error": err2 or "删除失败"})
    return jsonify({"success": True})


def _save_picks_history(result, user_id=0):
    db_module.save_picks_history(result, user_id)


def _save_picks520_history(result, user_id=0):
    db_module.save_picks520_history(result, user_id)


def _load_picks_history(max_days=30, user_id=0):
    return db_module.load_picks_history(max_days, user_id)


def _load_picks520_history(max_days=30, user_id=0):
    return db_module.load_picks520_history(max_days, user_id)


def _save_diag_history(result, user_id=0):
    db_module.save_diag_history(result, user_id)


def _diag_history_entry_id(entry):
    if entry.get("history_id"):
        return str(entry.get("history_id"))
    return f"{entry.get('date', '')}|{entry.get('update_time', '')}|{entry.get('code', '')}|{entry.get('name', '')}"


def _load_diag_history(max_days=90, limit=200, user_id=0):
    return db_module.load_diag_history(max_days, limit, user_id)


def _delete_diag_history(history_id, user_id=0):
    return db_module.delete_diag_history(history_id, user_id)


def _build_response():
    try:
        with ThreadPoolExecutor(max_workers=3) as executor:
            f_sohu = executor.submit(fetch_sohu_board_list)
            f_sina = executor.submit(fetch_sina_industry_detail)
            f_stocks = executor.submit(fetch_stock_fund_flow)
            f_index = executor.submit(fetch_index_quotes)
            all_items = f_sohu.result()
            sina_detail = {}
            try:
                sina_detail = f_sina.result()
            except Exception as e:
                log.warning("新浪行业数据获取失败(已容错): %s", e)
            stocks = []
            try:
                stocks = f_stocks.result()
            except Exception as e:
                log.warning("个股资金流获取失败(已容错): %s", e)
            index_quotes = {}
            try:
                index_quotes = f_index.result()
            except Exception as e:
                log.warning("指数行情获取失败(已容错): %s", e)
        industries, concepts, regions = classify_sectors(all_items)
        summary, board_summary = compute_fund_summary(stocks)

        industry_rows, stock_sector_map = compute_industry_fund_flow(
            industries, stocks, sina_detail, collect_sector_map=True
        )
        industry_rows.sort(key=lambda x: x.get("主力净流入", 0), reverse=True)

        if summary:
            for k, v in summary.items():
                summary[k] = fmt_val(v)
        for bname in board_summary:
            note = board_summary[bname].pop("note", None)
            for k, v in board_summary[bname].items():
                if k == "count":
                    continue
                board_summary[bname][k] = fmt_val(v)
            if note:
                board_summary[bname]["note"] = note
        for row in industry_rows:
            for k in ["明盘流入", "明盘流出", "明盘净流入", "主力净流入", "暗盘净流入"]:
                if k in row:
                    row[k] = fmt_val(row[k])

        index_data = {}
        total_amount_yi = 0.0
        for iname, idata in index_quotes.items():
            amt_yi = idata["成交额万"] / 1e4
            total_amount_yi += amt_yi
            index_data[iname] = {
                "price": f"{idata['现价']:.2f}",
                "change": f"{idata['涨跌']:.2f}",
                "change_pct": f"{idata['涨跌幅']:.2f}",
                "amount_yi": f"{amt_yi:.2f}",
            }

        resp = {
            "success": True,
            "update_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "index": index_data,
            "total_amount_yi": f"{total_amount_yi:.2f}",
            "amount_detail": {iname: idata["amount_yi"] for iname, idata in index_data.items()},
            "board_fund": board_summary,
            "industries": industry_rows,
            "concepts": [{"code": c, "name": n} for c, n in concepts[:100]],
            "concepts_total": len(concepts),
            "regions": [{"code": c, "name": n} for c, n in regions],
            "summary": summary,
        }
        return resp, stocks, stock_sector_map
    except Exception as e:
        return {"success": False, "error": str(e)}, [], {}


def _refresh_data():
    with _cached_data["lock"]:
        if _cached_data["refreshing"]:
            return
        _cached_data["refreshing"] = True
    try:
        data, stocks, stock_sector_map = _build_response()
        with _cached_data["lock"]:
            _cached_data["data"] = data
            _cached_data["stocks"] = stocks
            _cached_data["stock_sector_map"] = stock_sector_map
        log.info("stocks数量: %d", len(stocks) if stocks else 0)
    finally:
        with _cached_data["lock"]:
            _cached_data["refreshing"] = False


def _background_refresh():
    cycle = 0
    while True:
        cycle += 1
        try:
            t0 = datetime.now()
            log.info("缓存刷新开始 %s", t0.strftime('%H:%M:%S'))
            _refresh_data()
            elapsed = (datetime.now() - t0).total_seconds()
            log.info("缓存刷新完成 耗时%.1f秒", elapsed)
        except Exception as e:
            log.error("缓存刷新失败: %s", e)
        if cycle % PICKS_CACHE_REFRESH_INTERVAL == 0:
            try:
                with _picks520_cache["lock"]:
                    need_520 = _picks520_cache["data"] is not None
                if need_520:
                    log.info("开始刷新520战法缓存...")
                    t1 = datetime.now()
                    r = _compute_picks520()
                    if r:
                        with _picks520_cache["lock"]:
                            _picks520_cache["data"] = r
                            _picks520_cache["time"] = datetime.now()
                        _save_picks520_history(r)
                        log.info("520战法缓存刷新完成 耗时%.1f秒", (datetime.now()-t1).total_seconds())
            except Exception as e:
                log.error("520战法缓存刷新失败: %s", e)
            try:
                with _picks_cache["lock"]:
                    need_picks = _picks_cache["data"] is not None
                if need_picks:
                    r = _compute_picks()
                    if r:
                        with _picks_cache["lock"]:
                            _picks_cache["data"] = r
                            _picks_cache["time"] = datetime.now()
                        log.info("智能选股缓存刷新完成")
            except Exception as e:
                log.error("智能选股缓存刷新失败: %s", e)
        time.sleep(CACHE_INTERVAL)


@app.route("/")
def index():
    resp = send_from_directory("static", "index.html")
    resp.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    resp.headers["Pragma"] = "no-cache"
    return resp

@app.route("/test520")
def test520():
    return send_from_directory("static", "test520.html")


def _refresh_index_inplace(data):
    if not data or not data.get("success"):
        return data
    try:
        index_quotes = fetch_index_quotes()
        if not index_quotes:
            return data
        index_data = {}
        total_amount_yi = 0.0
        for iname, idata in index_quotes.items():
            amt_yi = idata["成交额万"] / 1e4
            total_amount_yi += amt_yi
            index_data[iname] = {
                "price": f"{idata['现价']:.2f}",
                "change": f"{idata['涨跌']:.2f}",
                "change_pct": f"{idata['涨跌幅']:.2f}",
                "amount_yi": f"{amt_yi:.2f}",
            }
        data["index"] = index_data
        data["total_amount_yi"] = f"{total_amount_yi:.2f}"
        data["amount_detail"] = {iname: idata["amount_yi"] for iname, idata in index_data.items()}
        data["update_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    except Exception as e:
        log.warning("实时指数刷新失败: %s", e)
    return data


@app.route("/api/data")
def get_data():
    with _cached_data["lock"]:
        data = _cached_data["data"]
    if data is None:
        return jsonify({"success": False, "error": "数据加载中，请稍后刷新"})
    data = _refresh_index_inplace(dict(data))
    return jsonify(data)


@app.route("/api/sector/<sector_code>")
def get_sector_detail(sector_code):
    if not _SECTOR_CODE_PATTERN.match(sector_code):
        return jsonify({"success": False, "error": "无效的板块代码"}), 400
    if not _check_rate_limit(f"sector:{request.remote_addr}", max_calls=20, window=60):
        return jsonify({"success": False, "error": "请求过于频繁，请稍后"}), 429
    try:
        member_codes = fetch_sohu_sector_stocks(sector_code)
        if not member_codes:
            return jsonify({"success": True, "stocks": [], "total": 0})
        qt_codes = []
        for c in member_codes:
            if c.startswith("6") or c.startswith("9"):
                qt_codes.append(f"sh{c}")
            else:
                qt_codes.append(f"sz{c}")
        all_stocks = []
        batch_size = 50
        for i in range(0, len(qt_codes), batch_size):
            batch = qt_codes[i:i + batch_size]
            try:
                r = SESSION.get(f"https://qt.gtimg.cn/q={','.join(batch)}", timeout=10)
                for line in r.text.strip().split(";"):
                    parsed = _parse_tencent_line(line.strip())
                    if not parsed:
                        continue
                    all_stocks.append({
                        "code": parsed["raw_code"],
                        "name": parsed["name"],
                        "price": str(parsed["price"]),
                        "change_pct": str(parsed["change_pct"]),
                        "change": str(parsed["change"]),
                        "amount_wan": str(parsed["amount"]),
                        "volume": str(parsed["volume"]),
                        "turnover": str(parsed["turnover"]),
                        "high": str(parsed["high"]),
                        "low": str(parsed["low"]),
                        "open": str(parsed["open"]),
                        "prev_close": str(parsed["prev_close"]),
                    })
            except Exception as e:
                pass
        all_stocks.sort(key=lambda x: float(x.get("change_pct", 0) or 0), reverse=True)
        return jsonify({"success": True, "stocks": all_stocks, "total": len(member_codes)})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


def fetch_kline(symbol, scale=5, datalen=48):
    secid = _eastmoney_secid(symbol)
    prefix = _tencent_stock_prefix(symbol)
    try:
        url = (f"https://push2his.eastmoney.com/api/qt/stock/kline/get"
               f"?secid={secid}&fields1=f1,f2,f3,f4,f5,f6"
               f"&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61"
               f"&klt={scale}&fqt=0&beg=0&end=20500101&lmt={datalen}")
        r = SESSION.get(url, timeout=10, headers={"Referer": "https://quote.eastmoney.com/"})
        d = r.json()
        klines = d.get("data", {}).get("klines", [])
        if klines:
            result = []
            for line in klines:
                parts = line.split(",")
                if len(parts) >= 6:
                    result.append({
                        "day": parts[0],
                        "open": float(parts[1]),
                        "close": float(parts[2]),
                        "high": float(parts[3]),
                        "low": float(parts[4]),
                        "volume": float(parts[5]),
                    })
            if len(result) >= 3:
                return result
    except Exception:
        pass
    try:
        url = (f"https://quotes.sina.cn/cn/api/jsonp.php/var%20k_{scale}=/"
               f"CN_MarketDataService.getKLineData?symbol={prefix}{symbol}&scale={scale}&ma=no&datalen={datalen}")
        r = SESSION.get(url, timeout=10, headers={"Referer": "https://finance.sina.com.cn/"})
        text = r.text
        json_start = text.index("(") + 1
        json_end = text.rindex(")")
        data = json.loads(text[json_start:json_end])
        if data and len(data) >= 3:
            return data
    except Exception:
        pass
    return []


def fetch_timeline(symbol):
    secid = _eastmoney_secid(symbol)
    try:
        url = (f"https://push2his.eastmoney.com/api/qt/stock/trends2/get"
               f"?secid={secid}&fields1=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13"
               f"&fields2=f51,f52,f53,f54,f55,f56,f57,f58&iscr=0")
        r = SESSION.get(url, timeout=10, headers={"Referer": "https://quote.eastmoney.com/"})
        d = r.json()
        data = d.get("data", {})
        trends = data.get("trends", [])
        pre_close = _sf(data.get("preClose", 0))
        if trends:
            result = []
            for line in trends:
                parts = line.split(",")
                if len(parts) >= 8:
                    result.append({
                        "time": parts[0],
                        "open": float(parts[1]),
                        "close": float(parts[4]),
                        "high": float(parts[2]),
                        "low": float(parts[3]),
                        "volume": float(parts[5]),
                        "amount": float(parts[6]),
                        "avg_price": float(parts[7]),
                    })
            if result:
                return result, pre_close
    except Exception:
        pass
    return _fetch_tencent_timeline(symbol)


def _format_minute_time(day, minute):
    if day and len(day) == 8 and minute and len(minute) == 4:
        return f"{day[:4]}-{day[4:6]}-{day[6:]} {minute[:2]}:{minute[2:]}"
    if minute and len(minute) == 4:
        return f"{minute[:2]}:{minute[2:]}"
    return minute or ""


def _fetch_tencent_timeline(symbol):
    prefix = _tencent_stock_prefix(symbol)
    qt_code = f"{prefix}{symbol}"
    try:
        url = f"https://web.ifzq.gtimg.cn/appstock/app/minute/query?code={qt_code}"
        r = SESSION.get(url, timeout=10, headers={"Referer": "https://gu.qq.com/"})
        d = r.json()
        node = d.get("data", {}).get(qt_code, {})
        data_node = node.get("data", {})
        rows = data_node.get("data", []) or []
        day = data_node.get("date", "")
        qt = node.get("qt", {}).get(qt_code, [])
        pre_close = _sf(qt[4], 0) if len(qt) > 4 else 0
        result = []
        prev_cum_vol = 0.0
        for line in rows:
            parts = str(line).split()
            if len(parts) < 2:
                continue
            minute, price_s = parts[:2]
            cum_vol_s = parts[2] if len(parts) > 2 else "0"
            cum_amount_s = parts[3] if len(parts) > 3 else "0"
            price = _sf(price_s, 0)
            cum_vol = _sf(cum_vol_s, 0)
            cum_amount = _sf(cum_amount_s, 0)
            if price <= 0:
                continue
            volume = max(cum_vol - prev_cum_vol, 0)
            prev_cum_vol = cum_vol
            avg_price = price
            if cum_amount > 0 and cum_vol > 0:
                avg_by_share = cum_amount / cum_vol
                avg_by_hand = cum_amount / (cum_vol * 100)
                avg_price = avg_by_share if abs(avg_by_share - price) < abs(avg_by_hand - price) else avg_by_hand
            result.append({
                "time": _format_minute_time(day, minute),
                "open": price,
                "close": price,
                "high": price,
                "low": price,
                "volume": volume,
                "amount": cum_amount,
                "avg_price": avg_price,
            })
        if result:
            return result, pre_close
    except Exception:
        pass
    return [], 0


def _ema(data, period):
    if not data:
        return []
    result = [data[0]]
    k = 2.0 / (period + 1)
    for i in range(1, len(data)):
        result.append(data[i] * k + result[-1] * (1 - k))
    return result


def _score_kline_trend(bars):
    if len(bars) < 3:
        return 0.0, {}
    closes = [float(b["close"]) for b in bars]
    highs = [float(b["high"]) for b in bars]
    lows = [float(b["low"]) for b in bars]
    volumes = [float(b["volume"]) for b in bars]
    n = len(closes)
    score = 0.0
    detail = {}

    # --- 动量 ---
    if n >= 2:
        short_mom = (closes[-1] - closes[-2]) / closes[-2] * 100 if closes[-2] else 0
        detail["短期动量"] = round(short_mom, 3)
        score += min(max(short_mom * 10, -20), 20)
    if n >= 5:
        mid_mom = (closes[-1] - closes[-5]) / closes[-5] * 100 if closes[-5] else 0
        detail["中期动量"] = round(mid_mom, 3)
        score += min(max(mid_mom * 8, -15), 15)
    if n >= 10:
        long_mom = (closes[-1] - closes[-10]) / closes[-10] * 100 if closes[-10] else 0
        detail["长期动量"] = round(long_mom, 3)
        score += min(max(long_mom * 5, -10), 10)

    # --- MA偏离 ---
    if n >= 3:
        ma3 = sum(closes[-3:]) / 3
        detail["MA3偏离"] = round((closes[-1] - ma3) / ma3 * 100, 3) if ma3 else 0
        score += min(max(detail["MA3偏离"] * 6, -8), 8)
    if n >= 5:
        ma5 = sum(closes[-5:]) / 5
        detail["MA5偏离"] = round((closes[-1] - ma5) / ma5 * 100, 3) if ma5 else 0
        score += min(max(detail["MA5偏离"] * 4, -6), 6)

    # --- 均线排列 ---
    if n >= 20:
        ma5_v = sum(closes[-5:]) / 5
        ma10_v = sum(closes[-10:]) / 10
        ma20_v = sum(closes[-20:]) / 20
        if ma5_v > ma10_v > ma20_v:
            detail["均线"] = "多头排列"
            score += 6
        elif ma5_v < ma10_v < ma20_v:
            detail["均线"] = "空头排列"
            score -= 6
        else:
            detail["均线"] = "交叉"

    # --- MACD ---
    if n >= 35:
        ema12 = _ema(closes, 12)
        ema26 = _ema(closes, 26)
        dif = [ema12[i] - ema26[i] for i in range(len(ema26))]
        dea = _ema(dif, 9)
        macd_hist = [(dif[i] - dea[i]) * 2 for i in range(len(dea))]
        if len(macd_hist) >= 2:
            if macd_hist[-1] > 0 and macd_hist[-2] <= 0:
                detail["MACD"] = "金叉"
                score += 8
            elif macd_hist[-1] < 0 and macd_hist[-2] >= 0:
                detail["MACD"] = "死叉"
                score -= 8
            elif macd_hist[-1] > 0:
                detail["MACD"] = "多头"
                score += 3
            else:
                detail["MACD"] = "空头"
                score -= 3

    # --- RSI ---
    if n >= 14:
        gains, losses = [], []
        for i in range(1, min(15, n)):
            delta = closes[-i] - closes[-i - 1]
            gains.append(max(delta, 0))
            losses.append(max(-delta, 0))
        avg_gain = sum(gains) / 14
        avg_loss = sum(losses) / 14
        if avg_loss > 0:
            rs = avg_gain / avg_loss
            rsi = 100 - 100 / (1 + rs)
        else:
            rsi = 100
        detail["RSI"] = round(rsi, 1)
        if rsi < 30:
            score += 5
        elif rsi < 45:
            score += 2
        elif rsi > 80:
            score -= 5
        elif rsi > 70:
            score -= 2

    # --- 布林带 ---
    if n >= 20:
        ma20_v = sum(closes[-20:]) / 20
        std20 = (sum((c - ma20_v) ** 2 for c in closes[-20:]) / 20) ** 0.5
        upper = ma20_v + 2 * std20
        lower = ma20_v - 2 * std20
        if upper > lower:
            bbr = (closes[-1] - lower) / (upper - lower)
            detail["BBI"] = round(bbr, 2)
            if bbr < 0.2:
                score += 4
            elif bbr > 0.9:
                score -= 4
            elif 0.4 < bbr < 0.7:
                score += 1

    # --- KDJ ---
    if n >= 9:
        k_val, d_val, j_val = 50, 50, 50
        for i in range(-min(9, n), 0):
            period_highs = highs[max(0, n + i - 9):n + i + 1]
            period_lows = lows[max(0, n + i - 9):n + i + 1]
            if not period_highs or not period_lows:
                continue
            hn = max(period_highs)
            ln = min(period_lows)
            rsv = (closes[n + i] - ln) / (hn - ln) * 100 if hn != ln else 50
            k_val = 2 / 3 * k_val + 1 / 3 * rsv
            d_val = 2 / 3 * d_val + 1 / 3 * k_val
            j_val = 3 * k_val - 2 * d_val
        detail["KDJ"] = f"K={round(k_val,0)} D={round(d_val,0)}"
        if k_val > d_val and k_val < 30:
            score += 5
        elif k_val < d_val and k_val > 70:
            score -= 5

    # --- ATR ---
    if n >= 14:
        trs = []
        for i in range(1, min(15, n)):
            tr = max(highs[-i] - lows[-i], abs(highs[-i] - closes[-i - 1]), abs(lows[-i] - closes[-i - 1]))
            trs.append(tr)
        atr = sum(trs) / 14
        detail["ATR%"] = round(atr / closes[-1] * 100, 2) if closes[-1] else 0

    # --- 量比/量价 ---
    if n >= 5 and sum(volumes[-5:]) > 0:
        vol_avg = sum(volumes[:-5]) / max(sum(1 for v in volumes[:-5] if v > 0), 1)
        vol_ratio = sum(volumes[-5:]) / 5 / vol_avg if vol_avg else 1
        detail["量比"] = round(vol_ratio, 2)
        score += min(max((vol_ratio - 1) * 5, -5), 10)
    if n >= 2 and volumes[-1] > 0 and volumes[-2] > 0:
        price_up = closes[-1] > closes[-2]
        vol_up = volumes[-1] > volumes[-2]
        if price_up and vol_up:
            detail["量价"] = "放量上涨"
            score += 8
        elif price_up and not vol_up:
            detail["量价"] = "缩量上涨"
            score += 3
        elif not price_up and vol_up:
            detail["量价"] = "放量下跌"
            score -= 8
        else:
            detail["量价"] = "缩量下跌"
            score -= 3

    # --- 最大回撤 ---
    if n >= 10:
        peak = closes[0]
        max_dd = 0
        for c in closes:
            peak = max(peak, c)
            dd = (peak - c) / peak if peak else 0
            max_dd = max(max_dd, dd)
        detail["回撤%"] = round(max_dd * 100, 1)
        if max_dd > 0.15:
            score -= 5
        elif max_dd > 0.1:
            score -= 2

    # --- 价格位置 ---
    if n >= 5:
        range_hi = max(highs[-5:])
        range_lo = min(lows[-5:])
        price_range = range_hi - range_lo
        if price_range > 0 and closes[-1] > 0:
            pos = (closes[-1] - range_lo) / price_range
            detail["位置"] = round(pos, 2)
            if 0.4 <= pos <= 0.75:
                score += 5
            elif 0.25 <= pos < 0.4:
                score += 3
            elif 0.75 < pos <= 0.85:
                score += 2
            elif pos > 0.85:
                score -= 2
            elif pos < 0.15:
                score -= 3

    return score, detail


_north_bound_cache = {"data": None, "time": None}
_NORTH_BOUND_TTL = 300


def _fetch_north_bound_flow():
    now = time.time()
    if _north_bound_cache["data"] is not None and _north_bound_cache["time"] and now - _north_bound_cache["time"] < _NORTH_BOUND_TTL:
        return _north_bound_cache["data"]
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        "reportName": "RPT_MUTUAL_DEAL_STK",
        "columns": "SECURITY_CODE,DEAL_AMT,DEAL_VOL,DEAL_TYPE",
        "pageNumber": "1", "pageSize": "500",
        "sortTypes": "-1", "sortColumns": "DEAL_AMT",
        "source": "WEB", "client": "WEB",
    }
    result = {}
    try:
        r = SESSION.get(url, params=params, timeout=15, headers={"Referer": "https://data.eastmoney.com/"})
        d = r.json()
        if d.get("success") and d.get("result"):
            for item in d["result"].get("data", []):
                code = item.get("SECURITY_CODE", "")
                amt = _sf(item.get("DEAL_AMT", 0))
                d_type = item.get("DEAL_TYPE", 0)
                if code not in result:
                    result[code] = {"buy": 0.0, "sell": 0.0}
                if d_type == 1:
                    result[code]["buy"] += amt
                else:
                    result[code]["sell"] += amt
    except Exception as e:
        log.warning("北向资金获取失败: %s", e)
    _north_bound_cache["data"] = result
    _north_bound_cache["time"] = now
    return result


_fund_history_cache = {"data": None, "time": None}
_FUND_HISTORY_TTL = 600


def _fetch_multi_day_fund_flow(codes):
    now = time.time()
    if _fund_history_cache["data"] is not None and _fund_history_cache["time"] and now - _fund_history_cache["time"] < _FUND_HISTORY_TTL:
        return _fund_history_cache["data"]
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    result = {}
    try:
        params = {
            "reportName": "RPT_DMSK_TS_STOCKNEW",
            "columns": "SECURITY_CODE,PRIME_INFLOW",
            "pageNumber": "1", "pageSize": "500",
            "sortTypes": "-1", "sortColumns": "PRIME_INFLOW",
            "source": "WEB", "client": "WEB",
        }
        r = SESSION.get(url, params=params, timeout=15, headers={"Referer": "https://data.eastmoney.com/"})
        d = r.json()
        if d.get("success") and d.get("result"):
            for item in d["result"].get("data", []):
                code = item.get("SECURITY_CODE", "")
                prime = _sf(item.get("PRIME_INFLOW", 0))
                result[code] = {"prime_today": prime, "consecutive_days": 1}
    except Exception as e:
        log.warning("多日资金流获取失败: %s", e)
    _fund_history_cache["data"] = result
    _fund_history_cache["time"] = now
    return result


_unlock_cache = {"data": None, "time": None}
_UNLOCK_TTL = 86400


def _fetch_unlock_stocks():
    now = time.time()
    if _unlock_cache["data"] is not None and _unlock_cache["time"] and now - _unlock_cache["time"] < _UNLOCK_TTL:
        return _unlock_cache["data"]
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    codes = set()
    try:
        params = {
            "reportName": "RPT_SHARE_BUSINESS_UNLOCK",
            "columns": "SECURITY_CODE",
            "pageNumber": "1", "pageSize": "200",
            "source": "WEB", "client": "WEB",
        }
        r = SESSION.get(url, params=params, timeout=15, headers={"Referer": "https://data.eastmoney.com/"})
        d = r.json()
        if d.get("success") and d.get("result"):
            for item in d["result"].get("data", []):
                c = item.get("SECURITY_CODE", "")
                if c:
                    codes.add(c)
    except Exception as e:
        log.warning("限售解禁获取失败: %s", e)
    _unlock_cache["data"] = codes
    _unlock_cache["time"] = now
    return codes


_market_breadth_cache = {"data": None, "time": None}
_BREADTH_TTL = 120


def _calc_market_breadth(stocks):
    now = time.time()
    if _market_breadth_cache["data"] is not None and _market_breadth_cache["time"] and now - _market_breadth_cache["time"] < _BREADTH_TTL:
        return _market_breadth_cache["data"]
    total = 0
    up_count = 0
    limit_up = 0
    limit_down = 0
    for st in stocks:
        rate = _sf(st.get("CHANGE_RATE_REAL", 0) or st.get("CHANGE_RATE", 0))
        name = st.get("SECURITY_NAME_ABBR", "")
        if rate == 0 and not name:
            continue
        total += 1
        if rate > 0:
            up_count += 1
        if rate >= 9.8 and "ST" not in name:
            limit_up += 1
        elif rate <= -9.8 and "ST" not in name:
            limit_down += 1
    breadth = up_count / total if total > 0 else 0.5
    result = {"breadth": breadth, "limit_up": limit_up, "limit_down": limit_down, "total": total}
    _market_breadth_cache["data"] = result
    _market_breadth_cache["time"] = now
    return result


def _detect_divergence(daily_bars):
    if not daily_bars or len(daily_bars) < 30:
        return {"macd_divergence": "none", "rsi_divergence": "none", "vol_price_divergence": "none"}
    closes = [float(b["close"]) for b in daily_bars]
    volumes = [float(b["volume"]) for b in daily_bars]
    n = len(closes)
    result = {"macd_divergence": "none", "rsi_divergence": "none", "vol_price_divergence": "none"}

    ema12 = _ema(closes, 12)
    ema26 = _ema(closes, 26)
    dif = [ema12[i] - ema26[i] for i in range(len(ema26))]
    dea = _ema(dif, 9)
    macd_hist = [(dif[i] - dea[i]) * 2 for i in range(len(dea))]

    if len(macd_hist) >= 15:
        recent = macd_hist[-15:]
        price_recent = closes[-15:]
        peaks_price = []
        peaks_macd = []
        for i in range(1, len(recent) - 1):
            if recent[i] > recent[i - 1] and recent[i] > recent[i + 1]:
                peaks_price.append(price_recent[i])
                peaks_macd.append(recent[i])
        if len(peaks_price) >= 2:
            if peaks_price[-1] > peaks_price[-2] and peaks_macd[-1] < peaks_macd[-2]:
                result["macd_divergence"] = "top"
            elif peaks_price[-1] < peaks_price[-2] and peaks_macd[-1] > peaks_macd[-2]:
                result["macd_divergence"] = "bottom"

    if n >= 28:
        rsi_vals = []
        for start in range(n - 28, n - 13):
            gains, losses = [], []
            for i in range(start + 1, min(start + 15, n)):
                delta = closes[i] - closes[i - 1]
                gains.append(max(delta, 0))
                losses.append(max(-delta, 0))
            avg_g = sum(gains) / 14
            avg_l = sum(losses) / 14
            rsi_vals.append(100 - 100 / (1 + avg_g / avg_l) if avg_l > 0 else 100)
        if len(rsi_vals) >= 10:
            r1, r2 = rsi_vals[:5], rsi_vals[5:]
            p1, p2 = closes[-28:-23], closes[-18:-13]
            avg_r1, avg_r2 = sum(r1) / len(r1), sum(r2) / len(r2)
            avg_p1, avg_p2 = sum(p1) / len(p1), sum(p2) / len(p2)
            if avg_p2 > avg_p1 and avg_r2 < avg_r1:
                result["rsi_divergence"] = "top"
            elif avg_p2 < avg_p1 and avg_r2 > avg_r1:
                result["rsi_divergence"] = "bottom"

    if n >= 10:
        p1_avg = sum(closes[-10:-5]) / 5
        p2_avg = sum(closes[-5:]) / 5
        v1_avg = sum(volumes[-10:-5]) / 5
        v2_avg = sum(volumes[-5:]) / 5
        if p2_avg > p1_avg and v2_avg < v1_avg * 0.7:
            result["vol_price_divergence"] = "price_up_vol_down"
        elif p2_avg < p1_avg and v2_avg > v1_avg * 1.3:
            result["vol_price_divergence"] = "price_down_vol_up"

    return result


def _fetch_pick_context_batch(code):
    bars_5 = fetch_kline(code, scale=5, datalen=48)
    bars_15 = fetch_kline(code, scale=15, datalen=32)
    bars_30 = fetch_kline(code, scale=30, datalen=24)
    daily_bars = _fetch_daily_kline(code, 80)
    return code, bars_5, bars_15, bars_30, daily_bars


SMART_PICK_COUNT = 16
SMART_PICK_WEIGHTS = {
    "fund": 0.22,
    "vol_price": 0.13,
    "liquidity": 0.07,
    "trend": 0.15,
    "tech_pos": 0.10,
    "daily_trend": 0.10,
    "sector": 0.06,
    "risk_control": 0.02,
    "sentiment": 0.01,
    "value": 0.02,
    "north_bound": 0.05,
    "fund_persist": 0.04,
    "divergence": 0.03,
}

SMART_SCORE_CAPS = {
    "fund": 40.0,
    "vol_price": 18.0,
    "liquidity": 18.0,
    "trend": 20.0,
    "tech_pos": 85.0,
    "daily_trend": 28.0,
    "sector": 12.0,
    "risk_control": 24.0,
    "sentiment": 8.0,
    "value": 10.0,
    "north_bound": 15.0,
    "fund_persist": 12.0,
    "divergence": 18.0,
}


def _normalize_factor_score(score, cap):
    if cap <= 0:
        return 0.0
    return max(min(score / cap * 100.0, 100.0), -100.0)


def _limit_up_threshold(code, name):
    if "ST" in name or "*ST" in name:
        return 4.8
    if code.startswith("30") or code.startswith("68"):
        return 19.8
    if code.startswith("8") or code.startswith("4") or code.startswith("920"):
        return 29.8
    return 9.8


def _pick_intraday_shape(c):
    price = _sf(c.get("price", 0))
    open_price = _sf(c.get("open", 0))
    high_price = _sf(c.get("high", 0))
    low_price = _sf(c.get("low", 0))
    prev_close = _sf(c.get("prev_close", 0))
    amplitude = (high_price - low_price) / prev_close * 100 if prev_close > 0 and high_price >= low_price else 0.0
    upper_shadow = (high_price - max(open_price, price)) / prev_close * 100 if prev_close > 0 and high_price > 0 else 0.0
    lower_shadow = (min(open_price, price) - low_price) / prev_close * 100 if prev_close > 0 and low_price > 0 else 0.0
    body = abs(price - open_price) / prev_close * 100 if prev_close > 0 else 0.0
    close_pos = (price - low_price) / (high_price - low_price) if high_price > low_price else 0.5
    gap_pct = (open_price - prev_close) / prev_close * 100 if prev_close > 0 else 0.0
    return {
        "amplitude": amplitude,
        "upper_shadow": upper_shadow,
        "lower_shadow": lower_shadow,
        "body": body,
        "close_pos": close_pos,
        "gap_pct": gap_pct,
    }


def _append_score_note(bucket, key, points, label, value=None):
    if abs(points) < 0.05:
        return
    sign = "+" if points > 0 else ""
    text = f"{sign}{round(points, 1)} {label}"
    if value not in (None, ""):
        text += f" ({value})"
    bucket.setdefault(key, []).append(text)


def _candidate_prefilter_score(c):
    amount_yi = c.get("amount_wan", 0.0) / 10000.0
    prime_yi = c.get("prime", 0.0) / 1e8
    rate = _sf(c.get("rate", 0.0))
    turnover = max(_sf(c.get("turnover", 0.0)), 0.0)
    score = (
        prime_yi * 3.0
        + min(max(amount_yi, 0.0), 40.0) * 0.55
        + max(c.get("rank_up", 0), 0) / 100.0 * 3.5
    )
    if 0.5 <= rate <= 7.0:
        score += 8.0
    elif 7.0 < rate <= 11.0:
        score += 3.0
    elif rate > 11.0:
        score -= min((rate - 11.0) * 0.8, 5.0)
    elif -1.0 <= rate < 0.5:
        score += 2.0
    elif rate < -1.0:
        score -= 4.0
    if 2.0 <= turnover <= 12.0:
        score += 5.0
    elif 12.0 < turnover <= 20.0:
        score += 2.0
    elif turnover > 20.0:
        score -= min((turnover - 20.0) * 0.35, 4.0)
    return score


def _passes_smart_pick_trend_gate(c, daily_metrics):
    if not daily_metrics:
        return False, "missing_daily_metrics"
    trend_name = str(daily_metrics.get("trend", "") or "")
    ret20 = _sf(daily_metrics.get("ret20", 0.0))
    ret60 = _sf(daily_metrics.get("ret60", 0.0))
    pos20 = _sf(daily_metrics.get("pos20", 0.5))
    ma20 = _sf(daily_metrics.get("ma20", 0.0))
    ma60 = _sf(daily_metrics.get("ma60", 0.0))
    price = _sf(c.get("price", 0.0))

    if trend_name in {"空头压制", "短中期空头"}:
        return False, "bearish_trend"
    if ma20 > 0 and price < ma20:
        return False, "below_ma20"
    if ret20 < -4:
        return False, "weak_ret20"
    if ma60 > 0 and ma20 > 0 and ma20 < ma60 and ret60 < -3:
        return False, "weak_ret60"
    if pos20 < 0.35 and ret20 < 2:
        return False, "too_low_in_range"
    return True, ""


def compute_smart_picks(stocks, top_n=SMART_PICK_COUNT, market_change_pct=0.0):
    if not stocks:
        return []
    north_bound_data = _fetch_north_bound_flow()
    fund_history_data = _fetch_multi_day_fund_flow([])
    unlock_codes = _fetch_unlock_stocks()
    market_breadth = _calc_market_breadth(stocks)
    is_weak_market = market_breadth["breadth"] < 0.35 or market_breadth["limit_down"] > market_breadth["limit_up"] * 2
    turnover_vals = []
    amount_vals = []
    for st in stocks:
        t = _sf(st.get("TURNOVERRATE_REAL", 0) or st.get("TURNOVERRATE", 0))
        if t > 0:
            turnover_vals.append(t)
        amount = _sf(st.get("AMOUNT_REAL", 0))
        if amount > 0:
            amount_vals.append(amount)
    avg_turnover = sum(turnover_vals) / len(turnover_vals) if turnover_vals else 3.0
    avg_amount = sum(amount_vals) / len(amount_vals) if amount_vals else 0.0

    candidates = []
    for st in stocks:
        code = st.get("SECURITY_CODE", "")
        name = st.get("SECURITY_NAME_ABBR", "")
        prime = _sf(st.get("PRIME_INFLOW", 0))
        rate = _sf(st.get("CHANGE_RATE_REAL", 0) or st.get("CHANGE_RATE", 0))
        s_in = _sf(st.get("SUPERDEAL_INFLOW", 0))
        s_out = _sf(st.get("SUPERDEAL_OUTFLOW", 0))
        b_in = _sf(st.get("BIGDEAL_INFLOW", 0))
        b_out = _sf(st.get("BIGDEAL_OUTFLOW", 0))
        open_in = s_in + b_in
        open_out = s_out + b_out
        dark_net = (open_out - open_in) - prime
        close_price = _sf(st.get("CLOSE_PRICE_REAL", 0) or st.get("CLOSE_PRICE", 0))
        turnover = _sf(st.get("TURNOVERRATE_REAL", 0) or st.get("TURNOVERRATE", 0))
        amount_wan = _sf(st.get("AMOUNT_REAL", 0))
        pe = _sf(st.get("PE_DYNAMIC", 0))
        org_part = _sf(st.get("ORG_PARTICIPATE", 0))
        focus = _sf(st.get("FOCUS", 0))
        rank_up = _si(st.get("RANK_UP", 0))
        ratio = _sf(st.get("RATIO", 0))
        high_price = _sf(st.get("HIGH_REAL", 0))
        low_price = _sf(st.get("LOW_REAL", 0))
        open_price = _sf(st.get("OPEN_REAL", 0))
        prev_close = _sf(st.get("PREV_CLOSE_REAL", 0))
        is_st = "ST" in name or "*ST" in name
        is_limit_up = rate >= _limit_up_threshold(code, name)
        if close_price <= 0:
            continue
        if is_limit_up:
            continue
        candidates.append({
            "code": code, "name": name,
            "prime": prime, "rate": rate,
            "open_in": open_in, "open_out": open_out,
            "dark_net": dark_net, "price": close_price,
            "turnover": turnover, "amount_wan": amount_wan, "pe": pe,
            "org_part": org_part, "focus": focus,
            "rank_up": rank_up, "ratio": ratio,
            "high": high_price, "low": low_price, "open": open_price, "prev_close": prev_close,
            "is_st": is_st, "is_limit_up": is_limit_up,
        })
    candidates.sort(key=_candidate_prefilter_score, reverse=True)
    top = candidates[:min(top_n * 8, 120)]

    pick_context_cache = {}
    codes_to_fetch = [c["code"] for c in top]
    executor = ThreadPoolExecutor(max_workers=8)
    try:
        futures = {executor.submit(_fetch_pick_context_batch, code): code for code in codes_to_fetch}
        done, not_done = wait(futures, timeout=75)
        for future in done:
            try:
                code, bars_5, bars_15, bars_30, daily_bars = future.result()
                pick_context_cache[code] = (bars_5, bars_15, bars_30, daily_bars)
            except Exception:
                pass
        for future in not_done:
            future.cancel()
        if not_done:
            log.warning("智能选股K线超时: %d/%d 未完成，已跳过技术分", len(not_done), len(futures))
    finally:
        executor.shutdown(wait=False, cancel_futures=True)

    scored = []
    for c in top:
        fund_score = 0.0
        vol_price_score = 0.0
        liquidity_score = 0.0
        trend_score = 0.0
        tech_pos_score = 0.0
        daily_trend_score = 0.0
        sector_score = 0.0
        risk_control_score = 0.0
        sentiment_score = 0.0
        value_score = 0.0
        kline_detail = {}
        signals = []
        score_notes = {
            "fund": [],
            "vol_price": [],
            "liquidity": [],
            "trend": [],
            "tech_pos": [],
            "daily_trend": [],
            "sector": [],
            "risk_control": [],
            "sentiment": [],
            "value": [],
        }

        # ============================================================
        # 因子1: 资金异动 (权重30%) - 量化第一优先级
        # ============================================================
        prime = c["prime"]
        prime_yi = prime / 1e8
        if prime_yi > 5:
            fund_score += 15
            signals.append("巨量流入")
            _append_score_note(score_notes, "fund", 15, "主力净流入强", f"{prime_yi:.2f}亿")
        elif prime_yi > 2:
            fund_score += 10
            _append_score_note(score_notes, "fund", 10, "主力净流入较强", f"{prime_yi:.2f}亿")
        elif prime_yi > 0.5:
            fund_score += 5
            _append_score_note(score_notes, "fund", 5, "主力净流入", f"{prime_yi:.2f}亿")
        elif prime_yi < -3:
            fund_score -= 12
            _append_score_note(score_notes, "fund", -12, "主力净流出大", f"{prime_yi:.2f}亿")
        elif prime_yi < -1:
            fund_score -= 6
            _append_score_note(score_notes, "fund", -6, "主力净流出", f"{prime_yi:.2f}亿")

        if 0 < c["rate"] <= 8.0 and prime > 0:
            pts = min(c["rate"] * 1.0, 6)
            fund_score += pts
            _append_score_note(score_notes, "fund", pts, "涨幅与资金同步", f"{c['rate']:.2f}%")
        elif c["rate"] > 11 and prime > 0:
            pts = min((c["rate"] - 11) * 0.5, 4)
            fund_score -= pts
            _append_score_note(score_notes, "fund", -pts, "涨幅过大需防追高", f"{c['rate']:.2f}%")

        if c["org_part"] > 0.4:
            fund_score += 8
            signals.append("机构高参与")
            _append_score_note(score_notes, "fund", 8, "机构参与高", f"{c['org_part']:.2f}")
        elif c["org_part"] > 0.2:
            fund_score += 4
            _append_score_note(score_notes, "fund", 4, "机构有参与", f"{c['org_part']:.2f}")
        elif c["org_part"] < 0.05:
            fund_score -= 3
            _append_score_note(score_notes, "fund", -3, "机构参与弱", f"{c['org_part']:.2f}")

        if c["dark_net"] < 0 and prime > 0:
            fund_score += 5
            signals.append("散户出逃")
            _append_score_note(score_notes, "fund", 5, "暗盘承接较好")

        if c["ratio"] > 0.6:
            fund_score += 5
            _append_score_note(score_notes, "fund", 5, "大单买入力度高", f"{c['ratio']:.2f}")
        elif c["ratio"] > 0.4:
            fund_score += 2
            _append_score_note(score_notes, "fund", 2, "大单买入力度偏强", f"{c['ratio']:.2f}")
        elif c["ratio"] < 0.15:
            fund_score -= 4
            _append_score_note(score_notes, "fund", -4, "大单买入力度偏弱", f"{c['ratio']:.2f}")

        # ============================================================
        # 因子2: 量价配合 (权重25%) - 量在价先
        # ============================================================
        if 4 <= c["turnover"] <= 15:
            vol_price_score += 6
            signals.append("换手适中")
            _append_score_note(score_notes, "vol_price", 6, "换手结构健康", f"{c['turnover']:.2f}%")
        elif 15 < c["turnover"] <= 22:
            vol_price_score += 2
            _append_score_note(score_notes, "vol_price", 2, "换手偏活跃", f"{c['turnover']:.2f}%")
        elif c["turnover"] > 4:
            vol_price_score += 3
            _append_score_note(score_notes, "vol_price", 3, "有一定换手", f"{c['turnover']:.2f}%")
        elif c["turnover"] < 0.5:
            vol_price_score -= 3
            _append_score_note(score_notes, "vol_price", -3, "换手不足", f"{c['turnover']:.2f}%")

        if avg_turnover > 0:
            turnover_z = (c["turnover"] - avg_turnover) / avg_turnover
            if turnover_z > 2:
                vol_price_score += 5
                signals.append("换手异常")
                _append_score_note(score_notes, "vol_price", 5, "换手显著强于市场", f"{turnover_z:.2f}x")
            elif turnover_z > 1:
                vol_price_score += 2
                _append_score_note(score_notes, "vol_price", 2, "换手高于市场均值", f"{turnover_z:.2f}x")
            elif turnover_z < -1.5:
                vol_price_score -= 3
                _append_score_note(score_notes, "vol_price", -3, "换手弱于市场均值", f"{turnover_z:.2f}x")

        if 0 < c["rate"] <= 8.0 and c["turnover"] > 2:
            vol_price_score += 4
            _append_score_note(score_notes, "vol_price", 4, "量价配合良好", f"{c['rate']:.2f}% / {c['turnover']:.2f}%")
        elif c["rate"] > 11 and c["turnover"] > 12:
            vol_price_score -= 4
            signals.append("追涨过热")
            _append_score_note(score_notes, "vol_price", -4, "涨幅和换手都过热", f"{c['rate']:.2f}% / {c['turnover']:.2f}%")
        elif c["rate"] < -3 and c["turnover"] > 5:
            vol_price_score -= 6
            signals.append("放量下跌")
            _append_score_note(score_notes, "vol_price", -6, "放量下跌", f"{c['rate']:.2f}%")

        # ============================================================
        # Factor: liquidity / tradability.
        # ============================================================
        amount_yi = c["amount_wan"] / 10000.0 if c["amount_wan"] else 0.0
        if amount_yi >= 20:
            liquidity_score += 8
            signals.append("成交活跃")
            _append_score_note(score_notes, "liquidity", 8, "成交活跃", f"{amount_yi:.2f}亿")
        elif amount_yi >= 8:
            liquidity_score += 5
            _append_score_note(score_notes, "liquidity", 5, "成交额较好", f"{amount_yi:.2f}亿")
        elif amount_yi >= 3:
            liquidity_score += 2
            _append_score_note(score_notes, "liquidity", 2, "成交额达标", f"{amount_yi:.2f}亿")
        elif amount_yi > 0:
            liquidity_score -= 4
            _append_score_note(score_notes, "liquidity", -4, "成交额偏小", f"{amount_yi:.2f}亿")

        if avg_amount > 0 and c["amount_wan"] > 0:
            amount_z = (c["amount_wan"] - avg_amount) / avg_amount
            if amount_z > 2:
                liquidity_score += 5
                _append_score_note(score_notes, "liquidity", 5, "成交额显著高于均值", f"{amount_z:.2f}x")
            elif amount_z > 1:
                liquidity_score += 2
                _append_score_note(score_notes, "liquidity", 2, "成交额高于均值", f"{amount_z:.2f}x")
            elif amount_z < -0.8:
                liquidity_score -= 2
                _append_score_note(score_notes, "liquidity", -2, "成交额低于均值", f"{amount_z:.2f}x")

        if c["turnover"] > 24:
            liquidity_score -= 5
            signals.append("换手过热")
            _append_score_note(score_notes, "liquidity", -5, "换手过热", f"{c['turnover']:.2f}%")
        elif 2 <= c["turnover"] <= 18:
            liquidity_score += 3
            _append_score_note(score_notes, "liquidity", 3, "换手可交易性好", f"{c['turnover']:.2f}%")

        # ============================================================
        # 因子3: 趋势动能 (权重20%) - 趋势延续
        # ============================================================
        if 2 < c["rate"] <= 8.5:
            trend_score += 6
            _append_score_note(score_notes, "trend", 6, "当日强势但未过热", f"{c['rate']:.2f}%")
        elif 0 < c["rate"] <= 2:
            trend_score += 3
            _append_score_note(score_notes, "trend", 3, "温和走强", f"{c['rate']:.2f}%")
        elif 8.5 < c["rate"] <= 12:
            trend_score += 1
            _append_score_note(score_notes, "trend", 1, "强势突破", f"{c['rate']:.2f}%")
        elif c["rate"] > 12:
            trend_score -= 3
            _append_score_note(score_notes, "trend", -3, "单日涨幅过大", f"{c['rate']:.2f}%")
        elif c["rate"] < -5:
            trend_score -= 6
            _append_score_note(score_notes, "trend", -6, "当日走弱明显", f"{c['rate']:.2f}%")
        elif c["rate"] < -2:
            trend_score -= 3
            _append_score_note(score_notes, "trend", -3, "当日偏弱", f"{c['rate']:.2f}%")

        if c["rank_up"] > 90:
            trend_score += 5
            signals.append("排名急升")
            _append_score_note(score_notes, "trend", 5, "资金排名急升", f"{c['rank_up']}")
        elif c["rank_up"] > 70:
            trend_score += 2
            _append_score_note(score_notes, "trend", 2, "资金排名抬升", f"{c['rank_up']}")

        rs = c["rate"] - market_change_pct
        if 1 < rs <= 6:
            trend_score += 5
            signals.append("相对强势")
            _append_score_note(score_notes, "trend", 5, "相对大盘更强", f"{rs:.2f}%")
        elif 0 < rs <= 1:
            trend_score += 2
            _append_score_note(score_notes, "trend", 2, "略强于大盘", f"{rs:.2f}%")
        elif rs > 8:
            trend_score -= 2
            _append_score_note(score_notes, "trend", -2, "相对大盘偏热", f"{rs:.2f}%")
        elif rs < -3:
            trend_score -= 4
            _append_score_note(score_notes, "trend", -4, "明显弱于大盘", f"{rs:.2f}%")

        # ============================================================
        # 因子4: 技术位置 (权重15%) - K线技术指标
        # ============================================================
        daily_metrics = {}
        pick_shape = _pick_intraday_shape(c)
        sector_ctx = _get_sector_context(c["code"])
        klines = pick_context_cache.get(c["code"])
        if klines:
            bars_5, bars_15, bars_30, daily_bars = klines
            if bars_5:
                s5, d5 = _score_kline_trend(bars_5)
                tech_pos_score += s5 * 0.5
                kline_detail["5min"] = d5
                _append_score_note(score_notes, "tech_pos", s5 * 0.5, "5分钟结构")
            if bars_15:
                s15, d15 = _score_kline_trend(bars_15)
                tech_pos_score += s15 * 0.3
                kline_detail["15min"] = d15
                _append_score_note(score_notes, "tech_pos", s15 * 0.3, "15分钟结构")
            if bars_30:
                s30, d30 = _score_kline_trend(bars_30)
                tech_pos_score += s30 * 0.2
                kline_detail["30min"] = d30
                _append_score_note(score_notes, "tech_pos", s30 * 0.2, "30分钟结构")
            if daily_bars:
                daily_metrics = _get_daily_trend_metrics(daily_bars)

        if daily_metrics:
            trend_name = daily_metrics.get("trend", "")
            ret5 = daily_metrics.get("ret5", 0.0)
            ret20 = daily_metrics.get("ret20", 0.0)
            pos20 = daily_metrics.get("pos20", 0.5)
            drawdown20 = daily_metrics.get("drawdown20", 0.0)
            vol_ratio = daily_metrics.get("vol_ratio_5_20", 1.0)
            ma20 = daily_metrics.get("ma20")
            if trend_name == "多头扩散":
                daily_trend_score += 8
                signals.append("日线多头")
                _append_score_note(score_notes, "daily_trend", 8, "日线多头扩散")
            elif trend_name == "短中期多头":
                daily_trend_score += 5
                _append_score_note(score_notes, "daily_trend", 5, "短中期多头")
            elif trend_name == "空头压制":
                daily_trend_score -= 8
                _append_score_note(score_notes, "daily_trend", -8, "日线空头压制")
            elif trend_name == "短中期空头":
                daily_trend_score -= 5
                _append_score_note(score_notes, "daily_trend", -5, "短中期空头")
            if 1 <= ret5 <= 8:
                daily_trend_score += 5
                _append_score_note(score_notes, "daily_trend", 5, "5日涨幅健康", f"{ret5:.2f}%")
            elif 8 < ret5 <= 14:
                daily_trend_score += 2
                _append_score_note(score_notes, "daily_trend", 2, "5日涨幅偏热", f"{ret5:.2f}%")
            elif ret5 > 16:
                daily_trend_score -= 4
                risk_control_score -= 3
                signals.append("5日涨幅过大")
                _append_score_note(score_notes, "daily_trend", -4, "5日涨幅过大", f"{ret5:.2f}%")
                _append_score_note(score_notes, "risk_control", -3, "短线过热", f"{ret5:.2f}%")
            elif ret5 < -5:
                daily_trend_score -= 4
                _append_score_note(score_notes, "daily_trend", -4, "5日走弱", f"{ret5:.2f}%")
            if 3 <= ret20 <= 18:
                daily_trend_score += 4
                _append_score_note(score_notes, "daily_trend", 4, "20日趋势健康", f"{ret20:.2f}%")
            elif ret20 > 30:
                daily_trend_score -= 3
                risk_control_score -= 2
                _append_score_note(score_notes, "daily_trend", -3, "20日涨幅偏大", f"{ret20:.2f}%")
                _append_score_note(score_notes, "risk_control", -2, "中期累积涨幅偏大", f"{ret20:.2f}%")
            elif ret20 < -12:
                daily_trend_score -= 5
                _append_score_note(score_notes, "daily_trend", -5, "20日趋势偏弱", f"{ret20:.2f}%")
            if 0.55 <= pos20 <= 0.85:
                daily_trend_score += 4
                _append_score_note(score_notes, "daily_trend", 4, "20日位置合适", f"{pos20:.2f}")
            elif pos20 > 0.97:
                daily_trend_score -= 3
                risk_control_score -= 3
                signals.append("接近20日高位")
                _append_score_note(score_notes, "daily_trend", -3, "20日位置过高", f"{pos20:.2f}")
                _append_score_note(score_notes, "risk_control", -3, "接近20日高位", f"{pos20:.2f}")
            elif pos20 < 0.2:
                daily_trend_score += 1
                _append_score_note(score_notes, "daily_trend", 1, "位置偏低有修复空间", f"{pos20:.2f}")
            if drawdown20 > -6:
                daily_trend_score += 2
                _append_score_note(score_notes, "daily_trend", 2, "回撤控制较好", f"{drawdown20:.2f}%")
            elif drawdown20 < -15:
                daily_trend_score -= 3
                _append_score_note(score_notes, "daily_trend", -3, "20日回撤偏大", f"{drawdown20:.2f}%")
            if 0.9 <= vol_ratio <= 1.8:
                daily_trend_score += 3
                _append_score_note(score_notes, "daily_trend", 3, "量能自然放大", f"{vol_ratio:.2f}")
            elif vol_ratio > 3.2 and ret5 > 6:
                daily_trend_score -= 2
                risk_control_score -= 2
                _append_score_note(score_notes, "daily_trend", -2, "量能放大过快", f"{vol_ratio:.2f}")
                _append_score_note(score_notes, "risk_control", -2, "放量偏激进", f"{vol_ratio:.2f}")
            if ma20 and ma20 > 0:
                ma20_dev = (c["price"] / ma20 - 1) * 100
                if 0 < ma20_dev <= 6:
                    tech_pos_score += 6
                    _append_score_note(score_notes, "tech_pos", 6, "贴近MA20上方运行", f"{ma20_dev:.2f}%")
                elif 6 < ma20_dev <= 10:
                    tech_pos_score += 2
                    _append_score_note(score_notes, "tech_pos", 2, "略偏离MA20", f"{ma20_dev:.2f}%")
                elif ma20_dev > 15:
                    tech_pos_score -= 4
                    risk_control_score -= 2
                    signals.append("偏离均线过大")
                    _append_score_note(score_notes, "tech_pos", -4, "偏离MA20过大", f"{ma20_dev:.2f}%")
                    _append_score_note(score_notes, "risk_control", -2, "均线乖离过大", f"{ma20_dev:.2f}%")

        passed_gate, gate_reason = _passes_smart_pick_trend_gate(c, daily_metrics)
        if not passed_gate:
            log.info("智能选股趋势门控过滤: %s %s %s", c["code"], c["name"], gate_reason)
            continue

        avg_sector_change = sector_ctx.get("avg_change", 0.0)
        if avg_sector_change >= 2.0:
            sector_score += 5
            signals.append("板块共振")
            _append_score_note(score_notes, "sector", 5, "板块共振强", f"{avg_sector_change:.2f}%")
        elif avg_sector_change >= 0.8:
            sector_score += 3
            _append_score_note(score_notes, "sector", 3, "板块偏强", f"{avg_sector_change:.2f}%")
        elif avg_sector_change <= -1.5:
            sector_score -= 5
            _append_score_note(score_notes, "sector", -5, "板块偏弱", f"{avg_sector_change:.2f}%")
        elif avg_sector_change <= -0.8:
            sector_score -= 2
            _append_score_note(score_notes, "sector", -2, "板块走弱", f"{avg_sector_change:.2f}%")
        sector_score += min(sector_ctx.get("strong_count", 0), 3)
        sector_score -= min(sector_ctx.get("weak_count", 0), 2) * 2
        if sector_ctx.get("strong_count", 0):
            _append_score_note(score_notes, "sector", min(sector_ctx.get("strong_count", 0), 3), "强势板块数量", sector_ctx.get("strong_count", 0))
        if sector_ctx.get("weak_count", 0):
            _append_score_note(score_notes, "sector", -min(sector_ctx.get("weak_count", 0), 2) * 2, "弱势板块数量", sector_ctx.get("weak_count", 0))

        if 0.5 <= pick_shape["upper_shadow"] <= 2.5 and pick_shape["close_pos"] >= 0.68:
            risk_control_score += 2
            _append_score_note(score_notes, "risk_control", 2, "收盘位置稳", f"{pick_shape['close_pos']:.2f}")
        if pick_shape["upper_shadow"] > 3.5 and c["rate"] > 3:
            risk_control_score -= 5
            signals.append("长上影")
            _append_score_note(score_notes, "risk_control", -5, "长上影", f"{pick_shape['upper_shadow']:.2f}%")
        if pick_shape["close_pos"] < 0.45 and c["rate"] > 3:
            risk_control_score -= 4
            signals.append("尾盘回落")
            _append_score_note(score_notes, "risk_control", -4, "尾盘回落", f"{pick_shape['close_pos']:.2f}")
        if pick_shape["gap_pct"] > 5 and pick_shape["close_pos"] < 0.55:
            risk_control_score -= 3
            _append_score_note(score_notes, "risk_control", -3, "高开低走", f"{pick_shape['gap_pct']:.2f}%")
        if pick_shape["amplitude"] > 15 and c["rate"] < 4:
            risk_control_score -= 2
            _append_score_note(score_notes, "risk_control", -2, "振幅过大", f"{pick_shape['amplitude']:.2f}%")
        if c["turnover"] > 18 and c["rate"] > 8:
            risk_control_score -= 3
            _append_score_note(score_notes, "risk_control", -3, "高换手高涨幅", f"{c['turnover']:.2f}% / {c['rate']:.2f}%")
        if 1 <= c["rate"] <= 6 and 2 <= c["turnover"] <= 12 and pick_shape["close_pos"] >= 0.7:
            risk_control_score += 4
            signals.append("收盘结构稳")
            _append_score_note(score_notes, "risk_control", 4, "收盘结构稳", f"{pick_shape['close_pos']:.2f}")
        elif c["rate"] > 12:
            risk_control_score -= 3
            _append_score_note(score_notes, "risk_control", -3, "单日过热", f"{c['rate']:.2f}%")

        # ============================================================
        # 因子5: 市场情绪 (权重5%) - 关注度+板块地位
        # ============================================================
        if c["focus"] > 90:
            sentiment_score += 4
            _append_score_note(score_notes, "sentiment", 4, "关注度高", f"{c['focus']:.0f}")
        elif c["focus"] > 60:
            sentiment_score += 2
            _append_score_note(score_notes, "sentiment", 2, "关注度较高", f"{c['focus']:.0f}")

        # ============================================================
        # 因子6: 基本面过滤 (权重5%) - PE估值
        # ============================================================
        pe = c["pe"]
        if 0 < pe < 15:
            value_score += 6
            _append_score_note(score_notes, "value", 6, "估值偏低", f"PE {pe:.1f}")
        elif 15 <= pe < 30:
            value_score += 3
            _append_score_note(score_notes, "value", 3, "估值合理", f"PE {pe:.1f}")
        elif 60 <= pe < 100:
            value_score -= 2
            _append_score_note(score_notes, "value", -2, "估值偏高", f"PE {pe:.1f}")
        elif pe > 100 or pe <= 0:
            value_score -= 4
            _append_score_note(score_notes, "value", -4, "估值不友好", f"PE {pe:.1f}")

        # ============================================================
        # 因子7: 北向资金 (权重5%) - 外资独立alpha
        # ============================================================
        north_bound_score = 0.0
        nb = north_bound_data.get(c["code"])
        if nb:
            nb_net = nb["buy"] - nb["sell"]
            nb_total = nb["buy"] + nb["sell"]
            if nb_net > 0 and nb_total > 0:
                nb_ratio = nb_net / nb_total
                if nb_ratio > 0.5:
                    north_bound_score += 12
                    signals.append("北向大幅净买入")
                    _append_score_note(score_notes, "north_bound", 12, "北向大幅净买入", f"{nb_ratio:.0%}")
                elif nb_ratio > 0.2:
                    north_bound_score += 7
                    signals.append("北向净买入")
                    _append_score_note(score_notes, "north_bound", 7, "北向净买入", f"{nb_ratio:.0%}")
                elif nb_ratio > 0:
                    north_bound_score += 3
                    _append_score_note(score_notes, "north_bound", 3, "北向小幅净买入", f"{nb_ratio:.0%}")
            elif nb_net < 0 and nb_total > 0:
                nb_ratio = abs(nb_net) / nb_total
                if nb_ratio > 0.4:
                    north_bound_score -= 8
                    _append_score_note(score_notes, "north_bound", -8, "北向大幅净卖出", f"{nb_ratio:.0%}")
                elif nb_ratio > 0.15:
                    north_bound_score -= 3
                    _append_score_note(score_notes, "north_bound", -3, "北向净卖出", f"{nb_ratio:.0%}")

        # ============================================================
        # 因子8: 资金连续性 (权重4%) - 多日持续流入更可靠
        # ============================================================
        fund_persist_score = 0.0
        fh = fund_history_data.get(c["code"])
        if fh:
            prime_today = fh.get("prime_today", 0)
            prime_yi_today = prime_today / 1e8
            if prime_yi_today > 1:
                fund_persist_score += 5
                _append_score_note(score_notes, "fund_persist", 5, "当日主力净流入强", f"{prime_yi_today:.2f}亿")
            elif prime_yi_today > 0.3:
                fund_persist_score += 2
                _append_score_note(score_notes, "fund_persist", 2, "当日主力净流入", f"{prime_yi_today:.2f}亿")
            elif prime_yi_today < -1:
                fund_persist_score -= 4
                _append_score_note(score_notes, "fund_persist", -4, "当日主力净流出", f"{prime_yi_today:.2f}亿")
        if c["prime"] > 0 and prime_yi > 0.5:
            fund_persist_score += 4
            signals.append("资金持续流入")
            _append_score_note(score_notes, "fund_persist", 4, "资金持续流入信号")

        # ============================================================
        # 因子9: 技术背离 (权重3%) - 反转前兆信号
        # ============================================================
        divergence_score = 0.0
        divergence_detail = {}
        if daily_bars:
            divergence_detail = _detect_divergence(daily_bars)
        macd_div = divergence_detail.get("macd_divergence", "none")
        rsi_div = divergence_detail.get("rsi_divergence", "none")
        vol_div = divergence_detail.get("vol_price_divergence", "none")
        if macd_div == "bottom":
            divergence_score += 10
            signals.append("MACD底背离")
            _append_score_note(score_notes, "divergence", 10, "MACD底背离，反转信号")
        elif macd_div == "top":
            divergence_score -= 10
            signals.append("MACD顶背离")
            _append_score_note(score_notes, "divergence", -10, "MACD顶背离，注意风险")
        if rsi_div == "bottom":
            divergence_score += 7
            signals.append("RSI底背离")
            _append_score_note(score_notes, "divergence", 7, "RSI底背离")
        elif rsi_div == "top":
            divergence_score -= 7
            signals.append("RSI顶背离")
            _append_score_note(score_notes, "divergence", -7, "RSI顶背离")
        if vol_div == "price_up_vol_down":
            divergence_score -= 5
            _append_score_note(score_notes, "divergence", -5, "量价背离(价涨量缩)")
        elif vol_div == "price_down_vol_up":
            divergence_score += 3
            _append_score_note(score_notes, "divergence", 3, "量价背离(价跌量增，可能见底)")

        # ============================================================
        # 风控过滤: 限售解禁 → 直接淘汰
        # ============================================================
        if c["code"] in unlock_codes:
            log.info("限售解禁过滤: %s %s", c["code"], c["name"])
            continue

        # ============================================================
        # 综合评分 (弱势大盘降权)
        # ============================================================
        normalized_scores = {
            "fund": _normalize_factor_score(fund_score, SMART_SCORE_CAPS["fund"]),
            "vol_price": _normalize_factor_score(vol_price_score, SMART_SCORE_CAPS["vol_price"]),
            "liquidity": _normalize_factor_score(liquidity_score, SMART_SCORE_CAPS["liquidity"]),
            "trend": _normalize_factor_score(trend_score, SMART_SCORE_CAPS["trend"]),
            "tech_pos": _normalize_factor_score(tech_pos_score, SMART_SCORE_CAPS["tech_pos"]),
            "daily_trend": _normalize_factor_score(daily_trend_score, SMART_SCORE_CAPS["daily_trend"]),
            "sector": _normalize_factor_score(sector_score, SMART_SCORE_CAPS["sector"]),
            "risk_control": _normalize_factor_score(risk_control_score, SMART_SCORE_CAPS["risk_control"]),
            "sentiment": _normalize_factor_score(sentiment_score, SMART_SCORE_CAPS["sentiment"]),
            "value": _normalize_factor_score(value_score, SMART_SCORE_CAPS["value"]),
            "north_bound": _normalize_factor_score(north_bound_score, SMART_SCORE_CAPS["north_bound"]),
            "fund_persist": _normalize_factor_score(fund_persist_score, SMART_SCORE_CAPS["fund_persist"]),
            "divergence": _normalize_factor_score(divergence_score, SMART_SCORE_CAPS["divergence"]),
        }
        total_score = sum(normalized_scores[k] * SMART_PICK_WEIGHTS[k] for k in SMART_PICK_WEIGHTS)
        if is_weak_market:
            total_score *= 0.8
            _append_score_note(score_notes, "risk_control", 0, "弱势大盘降权", f"市场宽度{market_breadth['breadth']:.0%}")
        c["fund_score"] = round(normalized_scores["fund"], 1)
        c["vol_price_score"] = round(normalized_scores["vol_price"], 1)
        c["liquidity_score"] = round(normalized_scores["liquidity"], 1)
        c["trend_score"] = round(normalized_scores["trend"], 1)
        c["tech_pos_score"] = round(normalized_scores["tech_pos"], 1)
        c["daily_trend_score"] = round(normalized_scores["daily_trend"], 1)
        c["sector_score"] = round(normalized_scores["sector"], 1)
        c["risk_control_score"] = round(normalized_scores["risk_control"], 1)
        c["sentiment_score"] = round(normalized_scores["sentiment"], 1)
        c["value_score"] = round(normalized_scores["value"], 1)
        c["north_bound_score"] = round(normalized_scores["north_bound"], 1)
        c["fund_persist_score"] = round(normalized_scores["fund_persist"], 1)
        c["divergence_score"] = round(normalized_scores["divergence"], 1)
        c["total_score"] = round(total_score, 1)
        c["kline_detail"] = kline_detail
        c["signals"] = list(dict.fromkeys(signals))
        c["score_notes"] = score_notes
        scored.append(c)
    scored.sort(key=lambda x: (x["total_score"], x.get("risk_control_score", 0), x.get("daily_trend_score", 0)), reverse=True)
    return scored[:top_n]


def _compute_picks():
    with _cached_data["lock"]:
        data = _cached_data["data"]
        stock_sector_map = _cached_data.get("stock_sector_map", {})
    if data is None:
        return None
    stocks = _cached_data.get("stocks")
    if not stocks:
        stocks = fetch_stock_fund_flow()
        _cached_data["stocks"] = stocks
    market_change_pct = 0.0
    if data and isinstance(data, dict):
        for idx_name, idx_data in data.get("index", {}).items():
            if idx_name == "上证指数":
                try:
                    market_change_pct = float(idx_data.get("change_pct", 0))
                except (ValueError, TypeError):
                    pass
                break
    try:
        picks = compute_smart_picks(stocks, top_n=SMART_PICK_COUNT, market_change_pct=market_change_pct)
        result = []
        for p in picks:
            sectors = stock_sector_map.get(p["code"], [])
            r = {
                "code": p["code"], "name": p["name"],
                "price": p["price"], "rate": round(p["rate"], 2),
                "prime": fmt_val(p["prime"]),
                "dark_net": fmt_val(p["dark_net"]),
                "turnover": round(p["turnover"], 2),
                "pe": round(p["pe"], 1) if p["pe"] > 0 else 0,
                "is_st": p.get("is_st", False),
                "fund_score": p["fund_score"],
                "vol_price_score": p["vol_price_score"],
                "liquidity_score": p.get("liquidity_score", 0),
                "trend_score": p["trend_score"],
                "tech_pos_score": p["tech_pos_score"],
                "daily_trend_score": p.get("daily_trend_score", 0),
                "sector_score": p.get("sector_score", 0),
                "risk_control_score": p.get("risk_control_score", 0),
                "sentiment_score": p["sentiment_score"],
                "value_score": p["value_score"],
                "north_bound_score": p.get("north_bound_score", 0),
                "fund_persist_score": p.get("fund_persist_score", 0),
                "divergence_score": p.get("divergence_score", 0),
                "total_score": p["total_score"],
                "kline_detail": p.get("kline_detail", {}),
                "signals": p.get("signals", []),
                "score_notes": p.get("score_notes", {}),
                "sectors": sectors[:3],
            }
            result.append(r)
        return {"success": True, "picks": result, "update_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.route("/api/picks/backtest")
def picks_backtest():
    user = _get_current_user()
    user_id = user["user_id"] if user else 0
    if user and user.get("is_admin"):
        history = db_module.load_picks_history_all()
    else:
        history = _load_picks_history(user_id=user_id)
    if not history:
        return jsonify({"success": False, "error": "无历史选股记录，请先使用智能选股功能积累数据"})
    history = sorted(history, key=lambda x: (x.get("date", ""), x.get("update_time", "")))
    results = []
    all_codes = set()
    for record in history:
        for p in record.get("picks", []):
            all_codes.add(p.get("code", ""))
    kline_data = {}
    code_list = list(all_codes)
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {}
        for idx, code in enumerate(code_list):
            futures[executor.submit(_fetch_daily_kline, code, 60)] = code
        for future in as_completed(futures, timeout=120):
            code = futures[future]
            try:
                bars = future.result()
                if bars and len(bars) >= 2:
                    kline_data[code] = bars
            except Exception:
                pass
    log.info("回测K线获取 %d/%d", len(kline_data), len(code_list))
    day_total_map = {}
    for record in history:
        day_total_map[record.get("date", "")] = day_total_map.get(record.get("date", ""), 0) + 1
    day_seen_map = {}
    for record in history:
        date_str = record.get("date", "")
        update_time = record.get("update_time", "")
        picks = record.get("picks", [])
        if not picks or not date_str:
            continue
        seen_idx = day_seen_map.get(date_str, 0)
        day_seen_map[date_str] = seen_idx + 1
        total_for_day = day_total_map.get(date_str, 1)
        if total_for_day <= 1:
            kind = "single"
            kind_label = "单次"
        elif seen_idx == 0:
            kind = "first"
            kind_label = "首次"
        else:
            kind = "last"
            kind_label = "尾盘"
        day_results = []
        for p in picks:
            code = p.get("code", "")
            name = p.get("name", "")
            price = _sf(p.get("price", 0))
            rate = _sf(p.get("rate", 0))
            score = _sf(p.get("total_score", 0))
            next_rate = 0
            next_close = 0
            next_date = ""
            bars = kline_data.get(code, [])
            verified = False
            for i in range(len(bars) - 1, -1, -1):
                if bars[i].get("day") <= date_str:
                    if i + 1 < len(bars):
                        next_bar = bars[i + 1]
                        nc = float(next_bar.get("close", 0))
                        if nc > 0 and price > 0:
                            next_close = nc
                            next_rate = (next_close - price) / price * 100
                            next_date = next_bar.get("day", "")
                            verified = True
                    break
            day_results.append({
                "code": code,
                "name": name,
                "price": price,
                "rate": rate,
                "score": score,
                "next_rate": round(next_rate, 2),
                "next_close": round(next_close, 2),
                "hit": verified and next_rate > 0,
                "verified": verified,
                "next_date": next_date,
            })
        if not day_results:
            continue
        verified_rows = [r for r in day_results if r["verified"]]
        verified_count = len(verified_rows)
        hit_count = sum(1 for r in verified_rows if r["hit"])
        avg_next_rate = (sum(r["next_rate"] for r in verified_rows) / verified_count) if verified_count else 0.0
        results.append({
            "date": update_time or date_str,
            "date_only": date_str,
            "update_time": update_time,
            "kind": kind,
            "kind_label": kind_label,
            "count": verified_count,
            "raw_count": len(day_results),
            "hit_count": hit_count,
            "hit_rate": round(hit_count / verified_count * 100, 1) if verified_count else 0.0,
            "avg_next_rate": round(avg_next_rate, 2),
            "pending_count": len(day_results) - verified_count,
            "picks": day_results,
        })
    results.sort(key=lambda x: x["date"])
    verified_results = [r for r in results if r["count"] > 0]
    if not verified_results:
        return jsonify({"success": False, "error": "回测数据不足，历史记录中无有效对比数据"})
    total_hits = sum(r["hit_count"] for r in verified_results)
    total_picks = sum(r["count"] for r in verified_results)
    overall_rate = round(total_hits / total_picks * 100, 1) if total_picks > 0 else 0
    overall_avg = round(sum(r["avg_next_rate"] for r in verified_results) / len(verified_results), 2)

    def _group_summary(rows):
        valid = [r for r in rows if r["count"] > 0]
        if not valid:
            return {"days": 0, "pick_count": 0, "hit_rate": 0.0, "avg_rate": 0.0}
        pick_count = sum(r["count"] for r in valid)
        hit_total = sum(r["hit_count"] for r in valid)
        return {
            "days": len(valid),
            "pick_count": pick_count,
            "hit_rate": round(hit_total / pick_count * 100, 1) if pick_count else 0.0,
            "avg_rate": round(sum(r["avg_next_rate"] for r in valid) / len(valid), 2),
        }

    first_rows = [r for r in results if r["kind"] in {"first", "single"}]
    last_rows = [r for r in results if r["kind"] == "last"]
    return jsonify({
        "success": True,
        "total_days": len(verified_results),
        "overall_hit_rate": overall_rate,
        "overall_avg_rate": overall_avg,
        "group_summary": {
            "first": _group_summary(first_rows),
            "last": _group_summary(last_rows),
        },
        "results": results,
    })


@app.route("/api/picks/history_count")
def picks_history_count():
    user = _get_current_user()
    user_id = user["user_id"] if user else 0
    if user and user.get("is_admin"):
        history = db_module.load_picks_history_all()
    else:
        history = _load_picks_history(user_id=user_id)
    dates = [r.get("date", "") for r in history]
    return jsonify({"success": True, "count": len(history), "dates": sorted(set(dates))})


@app.route("/api/picks520/history")
def picks520_history():
    user = _get_current_user()
    user_id = user["user_id"] if user else 0
    if user and user.get("is_admin"):
        history = db_module.load_picks520_history_all()
    else:
        history = _load_picks520_history(user_id=user_id)
    records = []
    for idx, record in enumerate(history):
        picks = record.get("picks", [])
        rec = {
            "date": record.get("date", ""),
            "update_time": record.get("update_time", ""),
            "kind": "first" if idx == 0 or record.get("date") != history[idx - 1].get("date") else "last",
            "count": len(picks),
            "picks": picks,
            "sample": [
                {"code": p.get("code", ""), "name": p.get("name", "")}
                for p in picks[:5]
            ],
        }
        if record.get("username"):
            rec["username"] = record["username"]
        records.append(rec)
    records.sort(key=lambda x: x.get("update_time") or x.get("date"), reverse=True)
    dates = sorted({r.get("date", "") for r in records if r.get("date")})
    return jsonify({"success": True, "count": len(records), "dates": dates, "records": records})


@app.route("/api/picks")
def get_picks():
    user = _get_current_user()
    user_id = user["user_id"] if user else 0
    force_refresh = request.args.get("force", "").strip().lower() in {"1", "true", "yes"}
    if not force_refresh:
        with _picks_cache["lock"]:
            if _picks_cache["data"] is not None and _picks_cache["time"] is not None:
                elapsed = (datetime.now() - _picks_cache["time"]).total_seconds()
                if elapsed < 30:
                    return jsonify(_picks_cache["data"])
    result = _compute_picks()
    if result is None:
        return jsonify({"success": False, "error": "数据加载中，请稍后"})
    with _picks_cache["lock"]:
        _picks_cache["data"] = result
        _picks_cache["time"] = datetime.now()
    _save_picks_history(result, user_id)
    return jsonify(result)


def _get_stocks():
    with _cached_data["lock"]:
        stocks = _cached_data.get("stocks")
    if not stocks:
        stocks = fetch_stock_fund_flow()
        with _cached_data["lock"]:
            _cached_data["stocks"] = stocks
    return stocks


@app.route("/api/surveillance-risk")
def surveillance_risk():
    result = _get_surveillance_risk()
    if not result:
        return jsonify({"success": False, "error": "数据加载中"})
    return jsonify(result)


def _get_market_change_pct():
    with _cached_data["lock"]:
        data = _cached_data.get("data")
    if data and isinstance(data, dict):
        for idx_name, idx_data in data.get("index", {}).items():
            if idx_name == "上证指数":
                try:
                    return float(idx_data.get("change_pct", 0))
                except (ValueError, TypeError):
                    return 0.0
    return 0.0


def _get_sector_context(stock_code):
    with _cached_data["lock"]:
        data = _cached_data.get("data")
        stock_sector_map = _cached_data.get("stock_sector_map", {})
    if (not data or not stock_sector_map) and not _cached_data.get("refreshing"):
        try:
            _refresh_data()
        except Exception:
            pass
        with _cached_data["lock"]:
            data = _cached_data.get("data")
            stock_sector_map = _cached_data.get("stock_sector_map", {})
    sectors = list(stock_sector_map.get(stock_code, []))[:3]
    industry_map = {}
    if data and isinstance(data, dict):
        for row in data.get("industries", []) or []:
            if isinstance(row, dict) and row.get("name"):
                industry_map[row["name"]] = row
    matched = [industry_map[s] for s in sectors if s in industry_map]
    avg_change = 0.0
    strong_count = 0
    weak_count = 0
    if matched:
        changes = [_sf(row.get("change_pct", 0)) for row in matched]
        avg_change = sum(changes) / len(changes) if changes else 0.0
        strong_count = sum(1 for v in changes if v >= 1.5)
        weak_count = sum(1 for v in changes if v <= -1.5)
    return {
        "sectors": sectors,
        "avg_change": round(avg_change, 2),
        "strong_count": strong_count,
        "weak_count": weak_count,
    }


def _get_daily_trend_metrics(daily_bars):
    if not daily_bars or len(daily_bars) < 20:
        return {}
    closes = [float(b.get("close", 0)) for b in daily_bars if _sf(b.get("close", 0)) > 0]
    highs = [float(b.get("high", 0)) for b in daily_bars if _sf(b.get("close", 0)) > 0]
    lows = [float(b.get("low", 0)) for b in daily_bars if _sf(b.get("close", 0)) > 0]
    volumes = [float(b.get("volume", 0)) for b in daily_bars if _sf(b.get("close", 0)) > 0]
    if len(closes) < 20 or len(highs) < 20 or len(lows) < 20:
        return {}
    latest = closes[-1]
    ma5 = sum(closes[-5:]) / 5
    ma20 = sum(closes[-20:]) / 20
    ma60 = sum(closes[-60:]) / 60 if len(closes) >= 60 else None
    ret5 = (latest / closes[-6] - 1) * 100 if len(closes) >= 6 and closes[-6] > 0 else 0.0
    ret20 = (latest / closes[-20] - 1) * 100 if closes[-20] > 0 else 0.0
    ret60 = (latest / closes[-60] - 1) * 100 if ma60 and closes[-60] > 0 else 0.0
    high20 = max(highs[-20:])
    low20 = min(lows[-20:])
    pos20 = (latest - low20) / (high20 - low20) if high20 > low20 else 0.5
    drawdown20 = (latest / high20 - 1) * 100 if high20 > 0 else 0.0
    vol5 = sum(volumes[-5:]) / 5 if len(volumes) >= 5 else 0.0
    vol20 = sum(volumes[-20:]) / 20 if len(volumes) >= 20 else 0.0
    vol_ratio_5_20 = vol5 / vol20 if vol20 > 0 else 1.0
    if ma60 and latest > ma5 > ma20 > ma60:
        trend = "多头扩散"
    elif ma60 and latest < ma5 < ma20 < ma60:
        trend = "空头压制"
    elif latest > ma5 > ma20:
        trend = "短中期多头"
    elif latest < ma5 < ma20:
        trend = "短中期空头"
    else:
        trend = "震荡"
    return {
        "trend": trend,
        "ma5": ma5,
        "ma20": ma20,
        "ma60": ma60,
        "ret5": ret5,
        "ret20": ret20,
        "ret60": ret60,
        "pos20": pos20,
        "drawdown20": drawdown20,
        "vol_ratio_5_20": vol_ratio_5_20,
    }


def _is_st_name(name):
    upper_name = str(name or "").upper()
    return upper_name.startswith("ST") or upper_name.startswith("*ST") or "ST" in upper_name


def _surveillance_profile(code, name=""):
    code = str(code or "").strip()
    is_st = _is_st_name(name)
    if code.startswith("68"):
        return {
            "board": "科创板",
            "index_code": "sh000688",
            "index_name": "科创50",
            "abnormal_threshold": 30.0,
            "repeat_required": 3,
            "severe10_threshold": 100.0,
            "severe30_threshold": 200.0,
            "limit_up_pct": 20.0,
            "is_st": False,
        }
    if code.startswith("3"):
        return {
            "board": "创业板",
            "index_code": "sz399006",
            "index_name": "创业板指",
            "abnormal_threshold": 30.0,
            "repeat_required": 3,
            "severe10_threshold": 100.0,
            "severe30_threshold": 200.0,
            "limit_up_pct": 20.0,
            "is_st": False,
        }
    if code.startswith("6"):
        return {
            "board": "沪市主板",
            "index_code": "sh000001",
            "index_name": "上证指数",
            "abnormal_threshold": 12.0 if is_st else 20.0,
            "repeat_required": 4,
            "severe10_threshold": 100.0,
            "severe30_threshold": 200.0,
            "limit_up_pct": 5.0 if is_st else 10.0,
            "is_st": is_st,
        }
    if code.startswith(("0", "001", "002")):
        return {
            "board": "深市主板",
            "index_code": "sz399001",
            "index_name": "深证成指",
            "abnormal_threshold": 12.0 if is_st else 20.0,
            "repeat_required": 4,
            "severe10_threshold": 100.0,
            "severe30_threshold": 200.0,
            "limit_up_pct": 5.0 if is_st else 10.0,
            "is_st": is_st,
        }
    return None


def _fetch_index_daily_bars(index_code, datalen=80):
    secid_map = {
        "sh000001": "1.000001",
        "sz399001": "0.399001",
        "sz399006": "0.399006",
        "sh000688": "1.000688",
    }
    try:
        url = f"https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={index_code},day,,,{datalen},qfq"
        r = SESSION.get(url, timeout=10, headers={"Referer": "https://gu.qq.com/"})
        d = r.json()
        node = d.get("data", {}).get(index_code, {})
        rows = node.get("day", []) or node.get("qfqday", [])
        result = []
        for item in rows:
            if len(item) >= 6:
                result.append({
                    "day": item[0],
                    "open": float(item[1]),
                    "close": float(item[2]),
                    "high": float(item[3]),
                    "low": float(item[4]),
                    "volume": float(item[5]),
                })
        if len(result) >= 20:
            return result
    except Exception:
        pass

    secid = secid_map.get(index_code)
    if not secid:
        return []
    try:
        url = (f"https://push2his.eastmoney.com/api/qt/stock/kline/get"
               f"?secid={secid}&fields1=f1,f2,f3,f4,f5,f6"
               f"&fields2=f51,f52,f53,f54,f55,f56&klt=101&fqt=0&beg=0&end=20500101&lmt={datalen}")
        r = SESSION.get(url, timeout=10, headers={"Referer": "https://quote.eastmoney.com/"})
        d = r.json()
        klines = d.get("data", {}).get("klines", [])
        result = []
        for line in klines:
            parts = line.split(",")
            if len(parts) >= 6:
                result.append({
                    "day": parts[0],
                    "open": float(parts[1]),
                    "close": float(parts[2]),
                    "high": float(parts[3]),
                    "low": float(parts[4]),
                    "volume": float(parts[5]),
                })
        return result
    except Exception as e:
        log.warning("指数日K获取失败 %s: %s", index_code, e)
        return []


def _align_close_series(stock_bars, index_bars):
    stock_map = {}
    for bar in stock_bars or []:
        day = str(bar.get("day", ""))[:10]
        close = _sf(bar.get("close", 0))
        if day and close > 0:
            stock_map[day] = close
    index_map = {}
    for bar in index_bars or []:
        day = str(bar.get("day", ""))[:10]
        close = _sf(bar.get("close", 0))
        if day and close > 0:
            index_map[day] = close
    dates = sorted(set(stock_map.keys()) & set(index_map.keys()))
    return [(day, stock_map[day], index_map[day]) for day in dates]


def _period_deviation_pct(series, end_idx, window_days):
    if end_idx >= len(series) or end_idx - window_days < 0:
        return None
    _, stock_close, index_close = series[end_idx]
    _, stock_start, index_start = series[end_idx - window_days]
    if stock_close <= 0 or stock_start <= 0 or index_close <= 0 or index_start <= 0:
        return None
    stock_ret = stock_close / stock_start - 1
    index_ret = index_close / index_start - 1
    return (stock_ret - index_ret) * 100


def _next_day_required_rise_pct(series, window_days, target_deviation_pct):
    if len(series) <= window_days - 1:
        return None
    end_idx = len(series) - 1
    start_idx = end_idx - (window_days - 1)
    if start_idx < 0:
        return None
    _, stock_close, index_close = series[end_idx]
    _, stock_start, index_start = series[start_idx]
    if stock_close <= 0 or stock_start <= 0 or index_close <= 0 or index_start <= 0:
        return None
    current_dev = (stock_close / stock_start - 1 - (index_close / index_start - 1)) * 100
    stock_ratio = stock_close / stock_start
    if stock_ratio <= 0:
        return None
    return (target_deviation_pct - current_dev) / stock_ratio


def _count_recent_abnormal_events(series, abnormal_threshold):
    if len(series) < 4:
        return {"positive": 0, "negative": 0}
    start_idx = max(3, len(series) - 9)
    positive = 0
    negative = 0
    for end_idx in range(start_idx, len(series)):
        dev = _period_deviation_pct(series, end_idx, 3)
        if dev is None:
            continue
        if dev >= abnormal_threshold:
            positive += 1
        elif dev <= -abnormal_threshold:
            negative += 1
    return {"positive": positive, "negative": negative}


def _next_trade_day_label():
    dt = datetime.now()
    while True:
        dt += timedelta(days=1)
        if dt.weekday() < 5:
            return dt.strftime("%Y-%m-%d")


def _surveillance_candidate_score(stock):
    rate = max(_sf(stock.get("CHANGE_RATE_REAL", 0) or stock.get("CHANGE_RATE", 0)), 0)
    turnover = max(_sf(stock.get("TURNOVERRATE_REAL", 0) or stock.get("TURNOVERRATE", 0)), 0)
    focus = max(_sf(stock.get("FOCUS", 0)), 0)
    rank_up = max(_si(stock.get("RANK_UP", 0)), 0)
    total = max(_sf(stock.get("TOTALSCORE", 0)), 0)
    prime = max(_sf(stock.get("PRIME_INFLOW", 0)) / 1e8, 0)
    return rate * 6 + turnover * 0.5 + focus * 0.08 + rank_up * 0.06 + total * 0.05 + prime * 0.6


def _compute_surveillance_risk():
    stocks = _get_stocks()
    if not stocks:
        return {"success": False, "error": "数据加载中"}
    with _cached_data["lock"]:
        stock_sector_map = _cached_data.get("stock_sector_map", {})

    candidate_stocks = []
    for st in stocks:
        code = str(st.get("SECURITY_CODE", "") or "").strip()
        name = str(st.get("SECURITY_NAME_ABBR", "") or "").strip()
        profile = _surveillance_profile(code, name)
        if not profile:
            continue
        if _sf(st.get("CHANGE_RATE_REAL", 0) or st.get("CHANGE_RATE", 0)) <= -2 and _sf(st.get("FOCUS", 0)) < 20:
            continue
        candidate_stocks.append((code, name, profile, _surveillance_candidate_score(st), st))

    candidate_stocks.sort(key=lambda x: x[3], reverse=True)
    candidate_stocks = candidate_stocks[:600]

    index_codes = sorted({item[2]["index_code"] for item in candidate_stocks})
    index_history = {idx: _fetch_index_daily_bars(idx, 80) for idx in index_codes}
    results = []

    def _scan_candidate(item):
        code, name, profile, _, stock = item
        daily_bars = _fetch_daily_kline(code, 45)
        if len(daily_bars) < 35:
            return None
        series = _align_close_series(daily_bars, index_history.get(profile["index_code"], []))
        if len(series) < 35:
            return None

        dev10 = _period_deviation_pct(series, len(series) - 1, 10)
        dev30 = _period_deviation_pct(series, len(series) - 1, 30)
        repeat_stats = _count_recent_abnormal_events(series, profile["abnormal_threshold"])
        next10 = _next_day_required_rise_pct(series, 10, profile["severe10_threshold"])
        next30 = _next_day_required_rise_pct(series, 30, profile["severe30_threshold"])
        next_abn = _next_day_required_rise_pct(series, 3, profile["abnormal_threshold"])
        limit_up = profile["limit_up_pct"]

        triggers = []
        if next10 is not None and 0 < next10 <= limit_up + 0.35:
            triggers.append({
                "mode": "10d",
                "label": "10日偏离值监管线",
                "needed_pct": round(next10, 2),
                "current_value": round(dev10 or 0, 2),
                "threshold": profile["severe10_threshold"],
            })
        if next30 is not None and 0 < next30 <= limit_up + 0.35:
            triggers.append({
                "mode": "30d",
                "label": "30日偏离值监管线",
                "needed_pct": round(next30, 2),
                "current_value": round(dev30 or 0, 2),
                "threshold": profile["severe30_threshold"],
            })
        if next_abn is not None and 0 < next_abn <= limit_up + 0.35:
            triggers.append({
                "mode": "abnormal",
                "label": "3日异动披露线",
                "needed_pct": round(next_abn, 2),
                "current_value": round(_period_deviation_pct(series, len(series) - 1, 2) or 0, 2),
                "threshold": profile["abnormal_threshold"],
            })
        if repeat_stats["positive"] == profile["repeat_required"] - 1 and next_abn is not None and 0 < next_abn <= limit_up + 0.35:
            triggers.append({
                "mode": "repeat",
                "label": "再现一次异常波动",
                "needed_pct": round(next_abn, 2),
                "current_value": repeat_stats["positive"],
                "threshold": profile["repeat_required"],
            })
        if not triggers:
            return None

        triggers.sort(key=lambda x: x["needed_pct"])
        primary = triggers[0]
        pct = _sf(stock.get("CHANGE_RATE_REAL", 0) or stock.get("CHANGE_RATE", 0))
        turnover = _sf(stock.get("TURNOVERRATE_REAL", 0) or stock.get("TURNOVERRATE", 0))
        price = _sf(stock.get("CLOSE_PRICE_REAL", 0) or stock.get("CLOSE_PRICE", 0))
        prime_yi = round(_sf(stock.get("PRIME_INFLOW", 0)) / 1e8, 2)
        if primary["mode"] == "abnormal":
            risk_level = "提示" if primary["needed_pct"] > min(6.0, limit_up * 0.6) else "中"
        else:
            risk_level = "高" if primary["needed_pct"] <= min(3.0, limit_up * 0.3) else ("中" if primary["needed_pct"] <= min(6.0, limit_up * 0.6) else "观察")
        tips = []
        if any(t["mode"] == "abnormal" for t in triggers):
            tips.append(f"3日偏离值再涨约{next_abn:.2f}%将触发异动披露")
        if any(t["mode"] == "repeat" for t in triggers):
            tips.append(f"近9个交易日已出现{repeat_stats['positive']}次同向异常波动")
        if dev10 is not None and dev10 >= 85:
            tips.append(f"10日偏离值已到{dev10:.1f}%")
        if dev30 is not None and dev30 >= 170:
            tips.append(f"30日偏离值已到{dev30:.1f}%")
        if pct >= limit_up * 0.8:
            tips.append("当日已接近涨停")
        if turnover >= 25:
            tips.append("换手偏高，波动风险大")
        if profile["is_st"]:
            tips.append("ST股票监管阈值更低")
        return {
            "code": code,
            "name": name,
            "board": profile["board"],
            "index_name": profile["index_name"],
            "is_st": profile["is_st"],
            "price": round(price, 2),
            "rate": round(pct, 2),
            "turnover": round(turnover, 2),
            "prime_yi": prime_yi,
            "limit_up_pct": limit_up,
            "next_trigger_pct": primary["needed_pct"],
            "trigger_label": primary["label"],
            "trigger_modes": [t["label"] for t in triggers],
            "dev10": round(dev10 or 0, 2),
            "dev30": round(dev30 or 0, 2),
            "recent_abnormal_count": repeat_stats["positive"],
            "repeat_required": profile["repeat_required"],
            "risk_level": risk_level,
            "tips": tips,
            "sectors": stock_sector_map.get(code, [])[:3],
        }

    executor = ThreadPoolExecutor(max_workers=8)
    try:
        futures = [executor.submit(_scan_candidate, item) for item in candidate_stocks]
        done, not_done = wait(futures, timeout=120)
        for future in done:
            try:
                row = future.result()
                if row:
                    results.append(row)
            except Exception:
                continue
        for future in not_done:
            future.cancel()
        if not_done:
            log.warning("监管预警K线超时: %d/%d 未完成，已跳过", len(not_done), len(futures))
    finally:
        executor.shutdown(wait=False, cancel_futures=True)

    risk_rank = {"高": 0, "中": 1, "提示": 2, "观察": 3}
    results.sort(key=lambda x: (
        risk_rank.get(x.get("risk_level", "观察"), 9),
        x["next_trigger_pct"],
        -x["recent_abnormal_count"],
        -x["dev10"],
    ))
    board_counts = {}
    for row in results:
        board_counts[row["board"]] = board_counts.get(row["board"], 0) + 1
    return {
        "success": True,
        "update_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "next_trade_day": _next_trade_day_label(),
        "assumption": "按次日对应指数涨跌幅约为0%估算；指数若同步上涨，触发所需个股涨幅会略降。",
        "rule_note": "按截至2026-06-05上交所、深交所现行异常波动与严重异常波动规则估算。",
        "count": len(results),
        "board_counts": board_counts,
        "items": results[:80],
    }


def _get_surveillance_risk():
    with _surveillance_cache["lock"]:
        data = _surveillance_cache.get("data")
        ts = _surveillance_cache.get("time")
        if data is not None and ts is not None:
            if (datetime.now() - ts).total_seconds() < SURVEILLANCE_CACHE_TTL:
                return data
    result = _compute_surveillance_risk()
    if result and result.get("success"):
        with _surveillance_cache["lock"]:
            _surveillance_cache["data"] = result
            _surveillance_cache["time"] = datetime.now()
    return result


def _build_search_items(query, limit=15):
    q = str(query or "").strip()
    if not q:
        return []
    stocks = _get_stocks()
    stocks_by_code = {str(st.get("SECURITY_CODE", "")).strip(): st for st in stocks}
    candidates = _fetch_security_candidates(q, count=max(limit * 2, 20))
    if not candidates:
        q_lower = q.lower()
        seen = set()
        for st in stocks:
            code = str(st.get("SECURITY_CODE", "")).strip()
            name = str(st.get("SECURITY_NAME_ABBR", "")).strip()
            if not code or code in seen:
                continue
            if q_lower in code.lower() or q_lower in name.lower():
                seen.add(code)
                candidates.append({
                    "code": code,
                    "name": name,
                    "security_type": "股票",
                    "classify": "AStock",
                })
                if len(candidates) >= limit:
                    break
    codes = [item["code"] for item in candidates[:limit]]
    quotes = _fetch_tencent_batch(codes) if codes else {}
    items = []
    for item in candidates:
        code = item["code"]
        st = stocks_by_code.get(code)
        quote = quotes.get(code) or {}
        price = _sf(quote.get("price"), None)
        rate = _sf(quote.get("change_pct"), None)
        if price is None and st:
            price = _sf(st.get("CLOSE_PRICE_REAL", 0) or st.get("CLOSE_PRICE", 0))
        if rate is None and st:
            rate = _sf(st.get("CHANGE_RATE_REAL", 0) or st.get("CHANGE_RATE", 0))
        items.append({
            "code": code,
            "name": item.get("name", code),
            "security_type": item.get("security_type", "证券"),
            "price": round(price or 0, 3),
            "rate": round(rate or 0, 2),
        })
        if len(items) >= limit:
            break
    return items


@app.route("/api/stock/search")
def stock_search():
    q = request.args.get("q", "").strip()
    if not q or len(q) < 1:
        return jsonify({"success": True, "items": []})
    items = _build_search_items(q, limit=15)
    return jsonify({"success": True, "items": items})


@app.route("/api/stock/evaluate/history")
def stock_evaluate_history():
    user = _get_current_user()
    user_id = user["user_id"] if user else 0
    limit = min(max(_si(request.args.get("limit", 80), 80), 1), 200)
    if user and user.get("is_admin"):
        records = db_module.load_diag_history_all(limit=limit)
    else:
        records = _load_diag_history(limit=limit, user_id=user_id)
    return jsonify({
        "success": True,
        "count": len(records),
        "records": records,
    })


@app.route("/api/stock/evaluate/history/delete", methods=["POST"])
def stock_evaluate_history_delete():
    user = _get_current_user()
    user_id = user["user_id"] if user else 0
    payload = request.get_json(silent=True) or {}
    history_id = str(payload.get("history_id", "")).strip()
    if not history_id:
        return jsonify({"success": False, "error": "缺少 history_id"}), 400
    deleted = _delete_diag_history(history_id, user_id)
    if not deleted:
        return jsonify({"success": False, "error": "未找到该条记录"}), 404
    return jsonify({"success": True, "history_id": history_id})


def _evaluate_generic_security(stock_code, market_change_pct=0.0):
    meta = _lookup_security_meta(stock_code) or {}
    quote = _fetch_tencent_batch([stock_code]).get(stock_code)
    if not quote:
        return None

    code = stock_code
    name = meta.get("name") or quote.get("name") or code
    security_type = meta.get("security_type", "证券")
    close_price = _sf(quote.get("price", 0))
    rate = _sf(quote.get("change_pct", 0))
    turnover = _sf(quote.get("turnover", 0))
    amount_yi = _sf(quote.get("amount", 0)) / 1e4
    high = _sf(quote.get("high", 0))
    low = _sf(quote.get("low", 0))
    prev_close = _sf(quote.get("prev_close", 0))
    amplitude = ((high - low) / prev_close * 100) if prev_close > 0 else 0.0

    bars_5 = fetch_kline(code, scale=5, datalen=48)
    bars_15 = fetch_kline(code, scale=15, datalen=32)
    bars_30 = fetch_kline(code, scale=30, datalen=24)
    daily_bars = _fetch_daily_kline(code, 40)
    daily_metrics = _get_daily_trend_metrics(daily_bars)
    sector_ctx = _get_sector_context(code)
    sector_text = " / ".join(sector_ctx.get("sectors", [])[:2]) if sector_ctx.get("sectors") else ""

    tech_score = 50
    tech_detail = {"证券类型": security_type}
    kline_scores = {}
    for label, bars in [("5min", bars_5), ("15min", bars_15), ("30min", bars_30)]:
        if bars:
            s, d = _score_kline_trend(bars)
            kline_scores[label] = (s, d)
    if kline_scores:
        weighted = 0
        for label, (s, d) in kline_scores.items():
            w = 0.5 if label == "5min" else (0.3 if label == "15min" else 0.2)
            weighted += s * w
            tech_detail[label] = d
        tech_score += min(max(weighted * 1.5, -40), 40)
    if rate > 3:
        tech_score += 8
    elif rate > 0:
        tech_score += 3
    elif rate < -3:
        tech_score -= 8
    elif rate < 0:
        tech_score -= 3
    if daily_metrics:
        tech_detail["日线趋势"] = daily_metrics["trend"]
        tech_detail["20日位置"] = round(daily_metrics["pos20"] * 100, 1)
        tech_detail["20日回撤"] = round(daily_metrics["drawdown20"], 2)
        if daily_metrics["trend"] in {"多头扩散", "短中期多头"}:
            tech_score += 8
        elif daily_metrics["trend"] in {"空头压制", "短中期空头"}:
            tech_score -= 8
        if daily_metrics["pos20"] >= 0.8:
            tech_score += 4
        elif daily_metrics["pos20"] <= 0.25:
            tech_score -= 4
    tech_detail["涨跌幅"] = round(rate, 2)
    tech_detail["换手率"] = round(turnover, 2)
    tech_score = min(max(tech_score, 0), 100)

    emotion_score = 50
    emotion_detail = {"资金面": "暂无逐笔资金流，使用活跃度代理"}
    if amount_yi > 30:
        emotion_score += 15
        emotion_detail["成交额"] = f"{amount_yi:.2f}亿(放量)"
    elif amount_yi > 10:
        emotion_score += 8
        emotion_detail["成交额"] = f"{amount_yi:.2f}亿(活跃)"
    elif amount_yi > 3:
        emotion_score += 4
        emotion_detail["成交额"] = f"{amount_yi:.2f}亿"
    else:
        emotion_score -= 6
        emotion_detail["成交额"] = f"{amount_yi:.2f}亿(偏低)"
    if daily_metrics:
        emotion_detail["量能比"] = round(daily_metrics["vol_ratio_5_20"], 2)
        if daily_metrics["vol_ratio_5_20"] >= 1.4:
            emotion_score += 6
        elif daily_metrics["vol_ratio_5_20"] <= 0.75:
            emotion_score -= 6
    if turnover > 8:
        emotion_score += 12
        emotion_detail["换手活跃"] = "高"
    elif turnover > 3:
        emotion_score += 6
        emotion_detail["换手活跃"] = "中"
    elif turnover < 0.8:
        emotion_score -= 8
        emotion_detail["换手活跃"] = "低"
    else:
        emotion_detail["换手活跃"] = "一般"
    if amplitude > 4 and rate > 0:
        emotion_score += 6
        emotion_detail["振幅"] = f"{amplitude:.2f}%(强势波动)"
    elif amplitude > 4 and rate < 0:
        emotion_score -= 6
        emotion_detail["振幅"] = f"{amplitude:.2f}%(偏弱波动)"
    else:
        emotion_detail["振幅"] = f"{amplitude:.2f}%"
    emotion_score = min(max(emotion_score, 0), 100)

    msg_score = 50
    msg_detail = {"证券类型": security_type}
    if len(daily_bars) >= 20:
        closes = [float(b.get("close", 0)) for b in daily_bars if _sf(b.get("close", 0)) > 0]
        vols = [float(b.get("volume", 0)) for b in daily_bars if _sf(b.get("close", 0)) > 0]
        if len(closes) >= 20:
            latest = closes[-1]
            ma5 = sum(closes[-5:]) / 5
            ma20 = sum(closes[-20:]) / 20
            ret5 = (latest / closes[-6] - 1) * 100 if len(closes) >= 6 and closes[-6] > 0 else 0.0
            ret20 = (latest / closes[-20] - 1) * 100 if closes[-20] > 0 else 0.0
            vol5 = sum(vols[-5:]) / 5 if len(vols) >= 5 else 0.0
            vol20 = sum(vols[-20:]) / 20 if len(vols) >= 20 else 0.0
            if latest > ma5 > ma20:
                msg_score += 18
                msg_detail["均线结构"] = "多头"
            elif latest > ma20:
                msg_score += 8
                msg_detail["均线结构"] = "偏强"
            elif latest < ma5 < ma20:
                msg_score -= 16
                msg_detail["均线结构"] = "空头"
            else:
                msg_detail["均线结构"] = "震荡"
            if ret5 > 3:
                msg_score += 10
                msg_detail["5日趋势"] = f"+{ret5:.2f}%"
            elif ret5 < -3:
                msg_score -= 10
                msg_detail["5日趋势"] = f"{ret5:.2f}%"
            else:
                msg_detail["5日趋势"] = f"{ret5:.2f}%"
            if ret20 > 8:
                msg_score += 12
                msg_detail["20日趋势"] = f"+{ret20:.2f}%"
            elif ret20 < -8:
                msg_score -= 12
                msg_detail["20日趋势"] = f"{ret20:.2f}%"
            else:
                msg_detail["20日趋势"] = f"{ret20:.2f}%"
            if vol20 > 0 and vol5 / vol20 > 1.3:
                msg_score += 6
                msg_detail["量能"] = f"{vol5 / vol20:.2f}倍"
            elif vol20 > 0 and vol5 / vol20 < 0.8:
                msg_score -= 6
                msg_detail["量能"] = f"{vol5 / vol20:.2f}倍"
            elif vol20 > 0:
                msg_detail["量能"] = f"{vol5 / vol20:.2f}倍"
    else:
        msg_detail["趋势"] = "历史K线不足"
    if daily_metrics:
        msg_detail["20日收益"] = round(daily_metrics["ret20"], 2)
        if daily_metrics["ret20"] >= 12:
            msg_score += 8
        elif daily_metrics["ret20"] <= -10:
            msg_score -= 8
        if daily_metrics.get("ret60"):
            msg_detail["60日收益"] = round(daily_metrics["ret60"], 2)
    if sector_ctx.get("sectors"):
        msg_detail["所属板块"] = sector_text
        msg_detail["板块均涨幅"] = sector_ctx["avg_change"]
        if sector_ctx["avg_change"] >= 1.5:
            msg_score += 6
        elif sector_ctx["avg_change"] <= -1.5:
            msg_score -= 6
    msg_score = min(max(msg_score, 0), 100)

    market_score = 50
    market_detail = {"证券类型": security_type}
    if market_change_pct > 1:
        market_score += 20
        market_detail["大盘"] = "强势上涨"
    elif market_change_pct > 0:
        market_score += 10
        market_detail["大盘"] = "小幅上涨"
    elif market_change_pct > -1:
        market_score -= 5
        market_detail["大盘"] = "小幅回落"
    else:
        market_score -= 15
        market_detail["大盘"] = "偏弱"
    market_detail["大盘涨跌"] = round(market_change_pct, 2)
    rs = rate - market_change_pct
    if rs > 3:
        market_score += 15
        market_detail["相对强弱"] = "显著强于大盘"
    elif rs > 1:
        market_score += 8
        market_detail["相对强弱"] = "强于大盘"
    elif rs < -3:
        market_score -= 15
        market_detail["相对强弱"] = "显著弱于大盘"
    elif rs < -1:
        market_score -= 8
        market_detail["相对强弱"] = "弱于大盘"
    else:
        market_detail["相对强弱"] = "与大盘接近"
    market_detail["RS"] = round(rs, 2)
    close_pos = ((close_price - low) / (high - low)) if high > low else 0.5
    market_detail["振幅"] = round(amplitude, 2)
    if amplitude >= 8 and close_pos < 0.35 and rate > 0:
        market_score -= 8
        market_detail["日内结构"] = "冲高回落"
    elif amplitude <= 3 and close_pos > 0.65 and rate > 0:
        market_score += 4
        market_detail["日内结构"] = "收盘偏强"
    if turnover > 8:
        market_score += 5
        market_detail["流动性"] = "活跃"
    elif turnover > 2:
        market_detail["流动性"] = "正常"
    elif turnover < 0.8:
        market_score -= 8
        market_detail["流动性"] = "偏弱"
    else:
        market_detail["流动性"] = "一般"
    market_detail["估值"] = "ETF/基金不使用PE" if security_type in {"ETF", "基金", "LOF", "REIT"} else "缺少PE数据"
    market_detail["成交额亿"] = round(amount_yi, 2)
    market_score = min(max(market_score, 0), 100)

    total = round(tech_score * 0.30 + emotion_score * 0.25 + msg_score * 0.25 + market_score * 0.20, 1)
    if total >= 75:
        hold_advice = "强势，可继续持有"
        hold_color = "red"
    elif total >= 60:
        hold_advice = "偏强，持有为主"
        hold_color = "red"
    elif total >= 45:
        hold_advice = "中性，建议观察"
        hold_color = "yellow"
    elif total >= 30:
        hold_advice = "偏弱，控制仓位"
        hold_color = "green"
    else:
        hold_advice = "弱势，谨慎参与"
        hold_color = "green"

    buy_signals = []
    if tech_score >= 60:
        buy_signals.append("短线技术结构偏强")
    if msg_score >= 60:
        buy_signals.append("中期趋势转强")
    if emotion_score >= 60:
        buy_signals.append("成交活跃度提升")
    if sector_ctx.get("avg_change", 0) >= 1.5:
        buy_signals.append("所属板块联动偏强")
    if rate > market_change_pct and rate > 0:
        buy_signals.append("走势跑赢大盘")

    sell_signals = []
    if tech_score < 35:
        sell_signals.append("短线技术走弱")
    if msg_score < 40:
        sell_signals.append("中期趋势承压")
    if emotion_score < 40:
        sell_signals.append("成交活跃度不足")
    if sector_ctx.get("avg_change", 0) <= -1.5:
        sell_signals.append("所属板块走弱")
    if rate < -2 and market_change_pct < 0:
        sell_signals.append("弱于大盘，需防回撤")

    return {
        "success": True,
        "code": code,
        "name": name,
        "security_type": security_type,
        "price": close_price,
        "rate": round(rate, 2),
        "turnover": round(turnover, 2),
        "pe": 0,
        "tech_score": round(tech_score, 1),
        "emotion_score": round(emotion_score, 1),
        "msg_score": round(msg_score, 1),
        "market_score": round(market_score, 1),
        "total_score": total,
        "tech_detail": tech_detail,
        "emotion_detail": emotion_detail,
        "msg_detail": msg_detail,
        "market_detail": market_detail,
        "hold_advice": hold_advice,
        "hold_color": hold_color,
        "buy_signals": buy_signals,
        "sell_signals": sell_signals,
        "sectors": sector_ctx.get("sectors", []),
    }


@app.route("/api/stock/evaluate/<stock_code>")
def stock_evaluate(stock_code):
    if not _CODE_PATTERN.match(stock_code):
        return jsonify({"success": False, "error": "无效的股票代码"}), 400
    if not _check_rate_limit(f"eval:{request.remote_addr}", max_calls=20, window=60):
        return jsonify({"success": False, "error": "请求过于频繁，请稍后"}), 429
    user = _get_current_user()
    user_id = user["user_id"] if user else 0
    stocks = _get_stocks()
    stock = None
    for st in stocks:
        if st.get("SECURITY_CODE") == stock_code:
            stock = st
            break
    market_change_pct = _get_market_change_pct()
    if not stock:
        fallback = _evaluate_generic_security(stock_code, market_change_pct=market_change_pct)
        if fallback:
            _save_diag_history(fallback, user_id)
            return jsonify(fallback)
        return jsonify({"success": False, "error": "未找到该个股"})

    code = stock["SECURITY_CODE"]
    name = stock.get("SECURITY_NAME_ABBR", "")
    close_price = _sf(stock.get("CLOSE_PRICE_REAL", 0) or stock.get("CLOSE_PRICE", 0))
    rate = _sf(stock.get("CHANGE_RATE_REAL", 0) or stock.get("CHANGE_RATE", 0))
    turnover = _sf(stock.get("TURNOVERRATE_REAL", 0) or stock.get("TURNOVERRATE", 0))
    prime = _sf(stock.get("PRIME_INFLOW", 0))
    s_in = _sf(stock.get("SUPERDEAL_INFLOW", 0))
    s_out = _sf(stock.get("SUPERDEAL_OUTFLOW", 0))
    b_in = _sf(stock.get("BIGDEAL_INFLOW", 0))
    b_out = _sf(stock.get("BIGDEAL_OUTFLOW", 0))
    open_in = s_in + b_in
    open_out = s_out + b_out
    dark_net = (open_out - open_in) - prime
    pe = _sf(stock.get("PE_DYNAMIC", 0))
    org_part = _sf(stock.get("ORG_PARTICIPATE", 0))
    focus = _sf(stock.get("FOCUS", 0))
    ratio = _sf(stock.get("RATIO", 0))
    rank_up = _si(stock.get("RANK_UP", 0))
    ratio_3d = _sf(stock.get("RATIO_3DAYS", 0))
    ratio_50d = _sf(stock.get("RATIO_50DAYS", 0))
    totalscore = _sf(stock.get("TOTALSCORE", 0))
    amount_yi = _sf(stock.get("AMOUNT_REAL", 0)) / 1e4
    high_real = _sf(stock.get("HIGH_REAL", 0))
    low_real = _sf(stock.get("LOW_REAL", 0))
    open_real = _sf(stock.get("OPEN_REAL", 0))
    prev_close_real = _sf(stock.get("PREV_CLOSE_REAL", 0))
    daily_bars = _fetch_daily_kline(code, 60)
    daily_metrics = _get_daily_trend_metrics(daily_bars)
    sector_ctx = _get_sector_context(code)
    sector_text = " / ".join(sector_ctx.get("sectors", [])[:2]) if sector_ctx.get("sectors") else ""

    # ==============================
    # 技术层评分 (0-100)
    # ==============================
    tech_score = 50
    tech_detail = {}

    bars_5 = fetch_kline(code, scale=5, datalen=48)
    bars_15 = fetch_kline(code, scale=15, datalen=32)
    bars_30 = fetch_kline(code, scale=30, datalen=24)

    kline_scores = {}
    for label, bars in [("5min", bars_5), ("15min", bars_15), ("30min", bars_30)]:
        if bars:
            s, d = _score_kline_trend(bars)
            kline_scores[label] = (s, d)
    if kline_scores:
        weighted = 0
        for label, (s, d) in kline_scores.items():
            w = 0.5 if label == "5min" else (0.3 if label == "15min" else 0.2)
            weighted += s * w
            tech_detail[label] = d
        tech_score += min(max(weighted * 1.5, -40), 40)

    if rate > 3:
        tech_score += 8
    elif rate > 0:
        tech_score += 3
    elif rate < -3:
        tech_score -= 8
    elif rate < 0:
        tech_score -= 3
    if daily_metrics:
        tech_detail["日线趋势"] = daily_metrics["trend"]
        tech_detail["20日位置"] = round(daily_metrics["pos20"] * 100, 1)
        tech_detail["20日回撤"] = round(daily_metrics["drawdown20"], 2)
        if daily_metrics["trend"] in {"多头扩散", "短中期多头"}:
            tech_score += 8
        elif daily_metrics["trend"] in {"空头压制", "短中期空头"}:
            tech_score -= 8
        if daily_metrics["pos20"] >= 0.8:
            tech_score += 4
        elif daily_metrics["pos20"] <= 0.25:
            tech_score -= 4

    tech_detail["涨跌幅"] = round(rate, 2)
    tech_detail["换手率"] = round(turnover, 2)
    tech_score = min(max(tech_score, 0), 100)

    # ==============================
    # 情绪层评分 (0-100)
    # ==============================
    emotion_score = 50
    emotion_detail = {}

    if prime > 0:
        emotion_score += min(prime / 1e8 * 4, 20)
    elif prime < 0:
        emotion_score -= min(abs(prime) / 1e8 * 4, 20)
    emotion_detail["主力净流入亿"] = round(prime / 1e8, 2)
    emotion_detail["成交额亿"] = round(amount_yi, 2)
    if amount_yi > 20:
        emotion_score += 8
    elif amount_yi > 8:
        emotion_score += 4
    elif 0 < amount_yi < 2:
        emotion_score -= 8
    if daily_metrics:
        emotion_detail["量能比"] = round(daily_metrics["vol_ratio_5_20"], 2)
        if daily_metrics["vol_ratio_5_20"] >= 1.4:
            emotion_score += 6
        elif daily_metrics["vol_ratio_5_20"] <= 0.75:
            emotion_score -= 6

    if org_part > 0.4:
        emotion_score += 15
        emotion_detail["机构"] = "高参与"
    elif org_part > 0.2:
        emotion_score += 8
        emotion_detail["机构"] = "中等"
    elif org_part < 0.05:
        emotion_score -= 10
        emotion_detail["机构"] = "低参与"
    else:
        emotion_detail["机构"] = "一般"

    if dark_net < 0 and prime > 0:
        emotion_score += 10
        emotion_detail["散户"] = "出逃(利好)"
    elif dark_net > 0 and prime < 0:
        emotion_score -= 10
        emotion_detail["散户"] = "接盘(利空)"
    else:
        emotion_detail["散户"] = "正常"
    emotion_detail["暗盘净流入亿"] = round(dark_net / 1e8, 2)

    if focus > 90:
        emotion_score += 10
    elif focus > 60:
        emotion_score += 5
    emotion_detail["关注度"] = round(focus, 1)

    if ratio > 0.6:
        emotion_score += 10
    elif ratio < 0.15:
        emotion_score -= 10
    emotion_detail["主力占比"] = round(ratio, 3)

    emotion_score = min(max(emotion_score, 0), 100)

    # ==============================
    # 消息层评分 (0-100)
    # ==============================
    msg_score = 50
    msg_detail = {}

    if rank_up > 90:
        msg_score += 20
        msg_detail["排名变动"] = "急升"
    elif rank_up > 70:
        msg_score += 10
        msg_detail["排名变动"] = "上升"
    elif rank_up < 20:
        msg_score -= 15
        msg_detail["排名变动"] = "下滑"
    else:
        msg_detail["排名变动"] = "平稳"

    if ratio_3d > 0.5:
        msg_score += 10
        msg_detail["3日趋势"] = "资金持续流入"
    elif ratio_3d < 0.2:
        msg_score -= 10
        msg_detail["3日趋势"] = "资金持续流出"
    else:
        msg_detail["3日趋势"] = "资金平衡"

    if ratio_50d > 0.4:
        msg_score += 8
        msg_detail["50日趋势"] = "中期向好"
    elif ratio_50d < 0.2:
        msg_score -= 8
        msg_detail["50日趋势"] = "中期偏弱"
    else:
        msg_detail["50日趋势"] = "中期中性"

    if totalscore > 70:
        msg_score += 12
        msg_detail["东财评分"] = f"{totalscore:.0f}(优)"
    elif totalscore > 50:
        msg_score += 5
        msg_detail["东财评分"] = f"{totalscore:.0f}(中)"
    elif totalscore < 30:
        msg_score -= 10
        msg_detail["东财评分"] = f"{totalscore:.0f}(差)"
    else:
        msg_detail["东财评分"] = f"{totalscore:.0f}(一般)"

    if daily_metrics:
        msg_detail["20日收益"] = round(daily_metrics["ret20"], 2)
        if daily_metrics["ret20"] >= 12:
            msg_score += 8
        elif daily_metrics["ret20"] <= -10:
            msg_score -= 8
        if daily_metrics.get("ret60"):
            msg_detail["60日收益"] = round(daily_metrics["ret60"], 2)
    if sector_ctx.get("sectors"):
        msg_detail["所属板块"] = sector_text
        msg_detail["板块均涨幅"] = sector_ctx["avg_change"]
        if sector_ctx["avg_change"] >= 1.5:
            msg_score += 6
        elif sector_ctx["avg_change"] <= -1.5:
            msg_score -= 6

    msg_score = min(max(msg_score, 0), 100)

    # ==============================
    # 市场层评分 (0-100)
    # ==============================
    market_score = 50
    market_detail = {}

    if market_change_pct > 1:
        market_score += 20
        market_detail["大盘"] = "强势上涨"
    elif market_change_pct > 0:
        market_score += 10
        market_detail["大盘"] = "微涨"
    elif market_change_pct > -1:
        market_score -= 5
        market_detail["大盘"] = "微跌"
    else:
        market_score -= 15
        market_detail["大盘"] = "弱势下跌"
    market_detail["大盘涨跌"] = round(market_change_pct, 2)

    rs = rate - market_change_pct
    if rs > 3:
        market_score += 15
        market_detail["相对强弱"] = "远强于大盘"
    elif rs > 1:
        market_score += 8
        market_detail["相对强弱"] = "强于大盘"
    elif rs < -3:
        market_score -= 15
        market_detail["相对强弱"] = "远弱于大盘"
    elif rs < -1:
        market_score -= 8
        market_detail["相对强弱"] = "弱于大盘"
    else:
        market_detail["相对强弱"] = "与大盘同步"
    market_detail["RS"] = round(rs, 2)
    amplitude = ((high_real - low_real) / prev_close_real * 100) if prev_close_real > 0 else 0.0
    close_pos = ((close_price - low_real) / (high_real - low_real)) if high_real > low_real else 0.5
    gap_pct = ((open_real - prev_close_real) / prev_close_real * 100) if prev_close_real > 0 and open_real > 0 else 0.0
    market_detail["振幅"] = round(amplitude, 2)
    market_detail["开盘缺口"] = round(gap_pct, 2)
    if amplitude >= 8 and close_pos < 0.35 and rate > 0:
        market_score -= 8
        market_detail["日内结构"] = "冲高回落"
    elif amplitude <= 3 and close_pos > 0.65 and rate > 0:
        market_score += 4
        market_detail["日内结构"] = "收盘偏强"

    if turnover > 8:
        market_score += 5
        market_detail["流动性"] = "活跃"
    elif turnover > 3:
        market_detail["流动性"] = "正常"
    elif turnover < 1:
        market_score -= 8
        market_detail["流动性"] = "低迷"
    else:
        market_detail["流动性"] = "偏低"

    if 0 < pe < 20:
        market_score += 5
        market_detail["估值"] = "低PE偏安全"
    elif pe > 80 or pe <= 0:
        market_score -= 8
        market_detail["估值"] = "高PE/亏损风险"
    else:
        market_detail["估值"] = "PE中性"
    market_detail["成交额亿"] = round(amount_yi, 2)

    market_score = min(max(market_score, 0), 100)

    # ==============================
    # 综合评分
    # ==============================
    total = round(tech_score * 0.30 + emotion_score * 0.25 + msg_score * 0.25 + market_score * 0.20, 1)

    # ==============================
    # 持仓建议
    # ==============================
    if total >= 75:
        hold_advice = "强烈建议持有"
        hold_color = "red"
    elif total >= 60:
        hold_advice = "建议持有"
        hold_color = "red"
    elif total >= 45:
        hold_advice = "观望，可轻仓持有"
        hold_color = "yellow"
    elif total >= 30:
        hold_advice = "建议减仓"
        hold_color = "green"
    else:
        hold_advice = "建议清仓"
        hold_color = "green"

    # ==============================
    # 买入指标建议
    # ==============================
    buy_signals = []
    if tech_score >= 60:
        buy_signals.append("技术面支撑买入")
    if emotion_score >= 60 and prime > 0:
        buy_signals.append("主力资金流入确认")
    if org_part > 0.3:
        buy_signals.append("机构高度参与")
    if ratio_3d > 0.4:
        buy_signals.append("3日资金持续流入")
    if sector_ctx.get("avg_change", 0) >= 1.5:
        buy_signals.append("所属板块联动偏强")
    if rs > 1 and rate > 0:
        buy_signals.append("强势跑赢大盘")

    sell_signals = []
    if tech_score < 35:
        sell_signals.append("技术面走弱")
    if prime < 0 and abs(prime) > 1e8:
        sell_signals.append("主力大幅流出")
    if dark_net > 0 and prime < 0:
        sell_signals.append("散户接盘主力出逃")
    if ratio_3d < 0.2:
        sell_signals.append("3日资金持续流出")
    if sector_ctx.get("avg_change", 0) <= -1.5:
        sell_signals.append("所属板块走弱")
    if market_change_pct < -1 and rate < -2:
        sell_signals.append("大盘弱势拖累")

    result = {
        "success": True,
        "code": code, "name": name, "security_type": "股票",
        "price": close_price, "rate": round(rate, 2),
        "turnover": round(turnover, 2), "pe": round(pe, 1) if pe > 0 else 0,
        "tech_score": round(tech_score, 1),
        "emotion_score": round(emotion_score, 1),
        "msg_score": round(msg_score, 1),
        "market_score": round(market_score, 1),
        "total_score": total,
        "tech_detail": tech_detail,
        "emotion_detail": emotion_detail,
        "msg_detail": msg_detail,
        "market_detail": market_detail,
        "hold_advice": hold_advice,
        "hold_color": hold_color,
        "buy_signals": buy_signals,
        "sell_signals": sell_signals,
        "sectors": sector_ctx.get("sectors", []),
    }
    _save_diag_history(result, user_id)
    return jsonify(result)


def _fetch_daily_kline(code, datalen=30):
    prefix = _tencent_stock_prefix(code)
    try:
        url = f"https://ifzq.gtimg.cn/appstock/app/fqkline/get?param={prefix}{code},day,,,{datalen},qfq"
        r = SESSION.get(url, timeout=8, headers={"Referer": "https://gu.qq.com/"})
        d = r.json()
        stock_data = d.get("data", {}).get(f"{prefix}{code}", {})
        qfqday = stock_data.get("qfqday", [])
        if not qfqday:
            qfqday = stock_data.get("day", [])
        if qfqday and len(qfqday) >= 20:
            result = []
            for item in qfqday:
                if len(item) >= 6:
                    result.append({
                        "day": item[0],
                        "open": float(item[1]),
                        "close": float(item[2]),
                        "high": float(item[3]),
                        "low": float(item[4]),
                        "volume": float(item[5]),
                    })
            return result
    except Exception:
        pass
    try:
        secid = _eastmoney_secid(code)
        url = (f"https://push2his.eastmoney.com/api/qt/stock/kline/get"
               f"?secid={secid}&fields1=f1,f2,f3,f4,f5,f6"
               f"&fields2=f51,f52,f53,f54,f55,f56&klt=101&fqt=0&beg=0&end=20500101&lmt={datalen}")
        r = SESSION.get(url, timeout=8, headers={"Referer": "https://quote.eastmoney.com/"})
        d = r.json()
        klines = d.get("data", {}).get("klines", [])
        if len(klines) >= 20:
            result = []
            for line in klines:
                parts = line.split(",")
                if len(parts) >= 6:
                    result.append({
                        "day": parts[0],
                        "open": float(parts[1]),
                        "close": float(parts[2]),
                        "high": float(parts[3]),
                        "low": float(parts[4]),
                        "volume": float(parts[5]),
                    })
            return result
    except Exception:
        pass
    return []


def _check_520_signal(daily_bars):
    if len(daily_bars) < 35:
        return False, {}
    closes = [float(b.get("close", 0)) for b in daily_bars]
    vols = [float(b.get("volume", 0)) for b in daily_bars]
    if any(c <= 0 for c in closes[-35:]):
        return False, {}
    ema12 = closes[0]
    ema26 = closes[0]
    dif_hist = []
    dea = 0.0
    for i, c in enumerate(closes):
        ema12 = c * 2 / 13 + ema12 * 11 / 13
        ema26 = c * 2 / 27 + ema26 * 25 / 27
        dif = ema12 - ema26
        dea = dif * 2 / 10 + dea * 8 / 10
        dif_hist.append((dif, dea, dif - dea))
    dif_now, dea_now, macd_now = dif_hist[-1]
    cross_day = 0
    for d in range(1, 4):
        if len(dif_hist) < d + 1:
            break
        dif_d, dea_d, _ = dif_hist[-d]
        dif_d_prev, dea_d_prev, _ = dif_hist[-d - 1]
        if dif_d > dea_d and dif_d_prev <= dea_d_prev:
            cross_day = d
            break
    if cross_day == 0:
        return False, {}
    zero_pos = "零上" if dif_now > 0 and dea_now > 0 else ("零轴" if abs(dif_now) < abs(dea_now) * 0.3 else "零下")
    vol_5 = sum(vols[-5:]) / 5 if len(vols) >= 5 else 0
    vol_20 = sum(vols[-20:]) / 20 if len(vols) >= 20 else 0
    vol_ratio = vol_5 / vol_20 if vol_20 > 0 else 1
    return True, {
        "dif": round(dif_now, 3),
        "dea": round(dea_now, 3),
        "macd": round(macd_now * 2, 3),
        "cross_day": cross_day,
        "zero_pos": zero_pos,
        "vol_ratio": round(vol_ratio, 2),
    }


def _compute_picks520():
    stocks = _get_stocks()
    if not stocks:
        return None
    mainboard = []
    for st in stocks:
        code = st.get("SECURITY_CODE", "")
        name = st.get("SECURITY_NAME_ABBR", "")
        if "ST" in name or "*ST" in name:
            continue
        if code.startswith("68"):
            continue
        if not (code.startswith("6") or code.startswith("0") or code.startswith("3")):
            continue
        mainboard.append(st)
    mainboard.sort(key=lambda x: abs(float(x.get("PRIME_INFLOW", 0) or 0)), reverse=True)
    mainboard = mainboard[:800]
    log.info("520扫描%d只主力活跃股MACD金叉...", len(mainboard))
    results = []
    fail_count = 0
    codes_to_check = [st["SECURITY_CODE"] for st in mainboard]
    code_to_stock = {st["SECURITY_CODE"]: st for st in mainboard}

    def _scan_one(code):
        try:
            bars = _fetch_daily_kline(code, 40)
            if not bars:
                return code, None
            matched, macd_info = _check_520_signal(bars)
            if not matched:
                return None, None
            stock = code_to_stock.get(code)
            if not stock:
                return None, None
            price = _sf(stock.get("CLOSE_PRICE_REAL", 0) or stock.get("CLOSE_PRICE", 0))
            rate = _sf(stock.get("CHANGE_RATE_REAL", 0) or stock.get("CHANGE_RATE", 0))
            turnover = _sf(stock.get("TURNOVERRATE_REAL", 0) or stock.get("TURNOVERRATE", 0))
            prime = _sf(stock.get("PRIME_INFLOW", 0))
            result = {
                "code": code,
                "name": stock.get("SECURITY_NAME_ABBR", ""),
                "price": price,
                "rate": round(rate, 2),
                "turnover": round(turnover, 2),
                "prime_yi": round(prime / 1e8, 2),
                "dif": macd_info.get("dif", 0),
                "dea": macd_info.get("dea", 0),
                "macd": macd_info.get("macd", 0),
                "cross_day": macd_info.get("cross_day", 0),
                "zero_pos": macd_info.get("zero_pos", ""),
                "vol_ratio": macd_info.get("vol_ratio", 1),
            }
            return code, result
        except Exception:
            return code, None

    BATCH_SIZE = 20
    for batch_start in range(0, len(codes_to_check), BATCH_SIZE):
        batch_codes = codes_to_check[batch_start:batch_start + BATCH_SIZE]
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(_scan_one, c) for c in batch_codes]
            for future in as_completed(futures, timeout=30):
                try:
                    fail_code, result = future.result()
                    if result:
                        results.append(result)
                    elif fail_code:
                        fail_count += 1
                except Exception:
                    fail_count += 1
        if batch_start % 100 == 0 and batch_start > 0:
            log.info("520进度 %d/%d, 金叉%d只, 失败%d只", batch_start, len(codes_to_check), len(results), fail_count)
        time.sleep(0.5)
    results.sort(key=lambda x: (x.get("macd", 0), x.get("vol_ratio", 1)), reverse=True)
    log.info("520金叉%d只, K线失败%d只", len(results), fail_count)
    return {"success": True, "picks": results, "update_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "count": len(results)}


@app.route("/api/picks520")
def get_picks520():
    user = _get_current_user()
    user_id = user["user_id"] if user else 0
    with _picks520_cache["lock"]:
        if _picks520_cache["data"] is not None and _picks520_cache["time"] is not None:
            elapsed = (datetime.now() - _picks520_cache["time"]).total_seconds()
            if elapsed < PICKS_CACHE_TTL:
                cached = _picks520_cache["data"]
                _save_picks520_history(cached, user_id)
                return jsonify(cached)
    stocks = _get_stocks()
    if not stocks:
        return jsonify({"success": False, "error": "数据加载中"})
    result = _compute_picks520()
    if result is None:
        return jsonify({"success": False, "error": "数据加载中"})
    with _picks520_cache["lock"]:
        _picks520_cache["data"] = result
        _picks520_cache["time"] = datetime.now()
    _save_picks520_history(result, user_id)
    return jsonify(result)


@app.route("/api/stock/kline/<stock_code>")
def stock_kline(stock_code):
    if not _CODE_PATTERN.match(stock_code):
        return jsonify({"success": False, "error": "无效的股票代码"}), 400
    bars = _fetch_daily_kline(stock_code, 60)
    qt_code = f"{_tencent_stock_prefix(stock_code)}{stock_code}"
    realtime = {}
    try:
        r = SESSION.get(f"https://qt.gtimg.cn/q={qt_code}", timeout=10)
        for line in r.text.strip().split(";"):
            parsed = _parse_tencent_line(line.strip())
            if parsed:
                realtime = parsed
                break
    except Exception:
        pass
    return jsonify({"success": True, "kline": bars, "realtime": realtime})


@app.route("/api/stock/timeline/<stock_code>")
def stock_timeline(stock_code):
    if not _CODE_PATTERN.match(stock_code):
        return jsonify({"success": False, "error": "无效的股票代码"}), 400
    bars, pre_close = fetch_timeline(stock_code)
    qt_code = f"{_tencent_stock_prefix(stock_code)}{stock_code}"
    realtime = {}
    try:
        r = SESSION.get(f"https://qt.gtimg.cn/q={qt_code}", timeout=10)
        for line in r.text.strip().split(";"):
            parsed = _parse_tencent_line(line.strip())
            if parsed:
                realtime = parsed
                break
    except Exception:
        pass
    return jsonify({"success": True, "timeline": bars, "pre_close": pre_close, "realtime": realtime})


if __name__ == "__main__":
    db_module.init_db()
    log.info("A股板块信息服务器启动: http://localhost:%d", SERVER_PORT)
    log.info("数据缓存间隔: %d秒", CACHE_INTERVAL)
    t = threading.Thread(target=_background_refresh, daemon=True)
    t.start()
    app.run(host="0.0.0.0", port=SERVER_PORT, debug=False, threaded=True)


_app_initialized = False


@app.before_request
def _ensure_init():
    global _app_initialized
    if not _app_initialized:
        _app_initialized = True
        try:
            db_module.init_db()
        except Exception as e:
            log.error("DB初始化失败: %s", e)
        t = threading.Thread(target=_background_refresh, daemon=True)
        t.start()
        log.info("首次请求触发: DB初始化完成, 后台刷新线程已启动")
