import os, sys
for k in ["http_proxy","https_proxy","HTTP_PROXY","HTTPS_PROXY"]: os.environ.pop(k, None)
os.environ["NO_PROXY"] = "*"
sys.stdout.reconfigure(encoding='utf-8')

import server
import json

print("Testing API...")
with server.app.test_client() as client:
    resp = client.get("/api/data")
    data = resp.get_json()
    print("success:", data.get("success"))
    if not data.get("success"):
        print("error:", data.get("error"))
    else:
        print("update_time:", data.get("update_time"))
        print("index:", json.dumps(data.get("index",{}), ensure_ascii=False))
        print("board_fund:", json.dumps(data.get("board_fund",{}), ensure_ascii=False))
        print("summary:", json.dumps(data.get("summary",{}), ensure_ascii=False))
        print("industries:", len(data.get("industries",[])))
        print("concepts_total:", data.get("concepts_total"))
        print("regions:", len(data.get("regions",[])))
        for d in data.get("industries",[])[:3]:
            print(f"  {d}")
