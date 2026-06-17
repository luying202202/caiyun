import os, sys
for k in ["http_proxy","https_proxy","HTTP_PROXY","HTTPS_PROXY"]: os.environ.pop(k, None)
os.environ["NO_PROXY"] = "*"
sys.stdout.reconfigure(encoding='utf-8')
import requests

s = requests.Session()
s.trust_env = False
s.headers.update({"User-Agent": "Mozilla/5.0", "Referer": "https://data.eastmoney.com/"})

url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
params = {
    "reportName": "RPT_DMSK_TS_STOCKNEW",
    "columns": "SECURITY_CODE,SECUCODE",
    "pageNumber": "1", "pageSize": "5",
    "sortTypes": "-1", "sortColumns": "PRIME_INFLOW",
    "source": "WEB", "client": "WEB",
    "filter": '(SECUCODE="830001.BJ")',
}
r = s.get(url, params=params, timeout=10)
d = r.json()
print("BJ filter test:", d.get("success"), d.get("result", {}).get("count", 0))

# 尝试另一个报表名看看有没有北交所
params2 = {
    "reportName": "RPT_DMSK_TS_STOCKNEW",
    "columns": "SECURITY_CODE,SECUCODE,SECURITY_NAME_ABBR",
    "pageNumber": "1", "pageSize": "10",
    "sortTypes": "1", "sortColumns": "SECURITY_CODE",
    "source": "WEB", "client": "WEB",
    "filter": '(SECUCODE="430001.BJ")',
}
r2 = s.get(url, params=params2, timeout=10)
d2 = r2.json()
print("BJ 430 test:", d2.get("success"), d2.get("result", {}).get("count", 0))

# 看看这个报表包含什么市场的
params3 = {
    "reportName": "RPT_DMSK_TS_STOCKNEW",
    "columns": "SECURITY_CODE,SECUCODE",
    "pageNumber": "1", "pageSize": "5",
    "sortTypes": "1", "sortColumns": "SECURITY_CODE",
    "source": "WEB", "client": "WEB",
}
r3 = s.get(url, params=params3, timeout=10)
d3 = r3.json()
items = d3.get("result", {}).get("data", [])
print(f"\nFirst 5 stocks (sorted asc):")
for it in items:
    print(f"  {it.get('SECURITY_CODE')} {it.get('SECUCODE')}")
# 最后几只
params3["pageNumber"] = "1037"
params3["pageSize"] = "5"
params3["sortTypes"] = "-1"
r4 = s.get(url, params=params3, timeout=10)
d4 = r4.json()
items2 = d4.get("result", {}).get("data", [])
print(f"\nLast 5 stocks:")
for it in items2:
    print(f"  {it.get('SECURITY_CODE')} {it.get('SECUCODE')}")
