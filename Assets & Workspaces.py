import requests
import urllib3
import pandas as pd
import time

urllib3.disable_warnings()

BASE_URL = "https://admin.smartsheet.com/api/acs/v1/planinsights/assets"
HEADERS = {
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "Content-Type": "application/json",
    "Cookie": "plan_type=Enterprise; user_id=168335600; __plan_type=Enterprise; __user_id=168335600; _gcl_au=1.1.706355646.1765290700; _ga=GA1.1.461338857.1765290700; coveo_visitorId=50cc1690-85cb-4eb9-8bef-e139ef221628; optimizelyEndUserId=oeu1766044439567r0.7094253087949383; optimizelySession=1766571295224; _rdt_uuid=1766571297460.1675028d-72ee-4003-a77c-e58ce07c677a; _biz_uid=3f1fe33353df4111beb2d4da3851ba94; _biz_nA=1; _biz_pendingA=%5B%5D; AMP_MKTG_708ff590e0=JTdCJTIycmVmZXJyZXIlMjIlM0ElMjJodHRwcyUzQSUyRiUyRnd3dy5nb29nbGUuY29tJTJGJTIyJTJDJTIycmVmZXJyaW5nX2RvbWFpbiUyMiUzQSUyMnd3dy5nb29nbGUuY29tJTIyJTdE; AMP_708ff590e0=JTdCJTIyZGV2aWNlSWQlMjIlM0ElMjI5MDYzMzE0YS1kNjc3LTQ3MTUtYWRjOC0wZjc1YzFkNzEzMTclMjIlMkMlMjJ1c2VySWQlMjIlM0ElMjIxNjgzMzU2MDAlMjIlMkMlMjJzZXNzaW9uSWQlMjIlM0ExNzY2NTcxMjk3OTAxJTJDJTIyb3B0T3V0JTIyJTNBZmFsc2UlMkMlMjJsYXN0RXZlbnRUaW1lJTIyJTNBMTc2NjU3MTI5NzkxNCUyQyUyMmxhc3RFdmVudElkJTIyJTNBMiUyQyUyMnBhZ2VDb3VudGVyJTIyJTNBMSU3RA==; _biz_flagsA=%7B%22Version%22%3A1%2C%22ViewThrough%22%3A%221%22%2C%22XDomain%22%3A%221%22%7D; _swb=aec924cd-4dfe-4801-94c2-3d3563d5c510; _fbp=fb.1.1766571300846.880454121264076527; _ketch_consent_v1_=eyJhbmFseXRpY3MiOnsic3RhdHVzIjoiZ3JhbnRlZCIsImNhbm9uaWNhbFB1cnBvc2VzIjpbImFuYWx5dGljcyJdfSwiZXNzZW50aWFsX3NlcnZpY2VzIjp7InN0YXR1cyI6ImdyYW50ZWQiLCJjYW5vbmljYWxQdXJwb3NlcyI6WyJlbWFpbF9ta3RnIl19LCJ0YXJnZXRlZF9hZHZlcnRpc2luZyI6eyJzdGF0dXMiOiJncmFudGVkIiwiY2Fub25pY2FsUHVycG9zZXMiOlsiYmVoYXZpb3JhbF9hZHZlcnRpc2luZyJdfX0%3D; _ga_ZYH7XNXMZK=GS2.1.s1766571297$o3$g0$t1766571434$j60$l0$h0; _ce.s=v~34cb85c14f318a54f00c923a4eaa9ba2ca856653~lcw~1766571298497~vir~new~lva~1766571297796~vpv~0~as~false~v11.cs~388767~v11.s~669b1c80-e0b1-11f0-9a4b-0925c97a5697~v11.vs~34cb85c14f318a54f00c923a4eaa9ba2ca856653~v11.fsvd~eyJ1cmwiOiJjb21tdW5pdHkuc21hcnRzaGVldC5jb20vZGlzY3Vzc2lvbi8qL2Zvcm11bGEtdG8tb25seS1jb3VudC11bmlxdWUtY2VsbHMtbm8tZHVwbGljYXRlcyIsInJlZiI6Imh0dHBzOi8vd3d3Lmdvb2dsZS5jb20vIiwidXRtIjpbXX0%3D~v11.sla~1766571298382~v11ls~669b1c80-e0b1-11f0-9a4b-0925c97a5697~gtrk.la~mjjv0qzw~lcw~1766571434927; S3S_F=Dk4N_F3tpkQ47CI9hCSE-c; __S3S_F=Dk4N_F3tpkQ47CI9hCSE-c; S3S_F=S3S_F; smar_login=v1%3A%7B%22regions%22%3A%7B%22app.smartsheet.com%22%3A%7B%22count%22%3A392%2C%22latest%22%3A1767584668%7D%7D%2C%22latest%22%3A%7B%22region%22%3A%22app.smartsheet.com%22%2C%22time%22%3A1767584668%7D%7D; S4AC=fp2.AAB1MQAAABDNgDiRGUDVv-kj9FFSfkNM0oA2dHSlLuqwicG_o4oY1ZZuJPA00LXuhGjjUFAyOE-OHBMcjKJp9jPRPPcLp6K_mbsJ5AbmD15SWSI; JSESSIONID=99ABDAFADD18B6573D2E2231D6D547DB; _dd_s=aid=30fdf152-717b-4cfb-a588-c27b92c222e1&rum=2&id=58fd68ad-71e5-46d1-becb-f634d1a7bca7&created=1767584652801&expire=1767585583448",
    "Host": "admin.smartsheet.com",
    "Referer": "https://admin.smartsheet.com/true-up?tab=USERS_P2",
    "Sec-ch-ua": "Google Chrome;v=143, Chromium;v=143, Not A(Brand;v=24",
    "Sec-ch-ua-mobile":"?0",
    "Sec-ch-ua-platform": "Windows",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "Windows",
    "x-smar-trace-id": "9818bad0-9040-4c8f-894e-72fecea97159",
    "x-smar-user-agent": "admin-center/data-client",
    "x-smar-xsrf-token": "UpnS-f_8OL2HZtA5rXvSBek-n1AmWE15vD-RdmGlbM6w"
}
resp = requests.get(BASE_URL, headers=HEADERS, verify=False)
resp.raise_for_status()
result=resp.json()
print(type(result))
print(result.keys())
assets = result["assets"]
data = pd.json_normalize(assets,sep="_")
print(data)
data.to_csv("assets/asset.csv",index=False)