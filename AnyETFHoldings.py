import urllib.request, logging, json, time

import pandas as pd
from lxml import html

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36"
}

def fetch(fund):
    df = pd.DataFrame()
    companies = []
    result = {}

    fund_csv_url = f"https://etfdb.com/etf/{fund.upper()}/"
    req = urllib.request.Request(fund_csv_url, headers=HEADERS)
    res = urllib.request.urlopen(req, timeout=60)
    tree = html.parse(res)
    table = tree.xpath("//table[@data-hash='etf-holdings']")[0]

    for query in [
        "&sort=weight&order=asc",
        "&sort=weight&order=desc",
        "&sort=symbol&order=asc",
        "&sort=symbol&order=desc",
    ]:
        holdings_url = f"https://etfdb.com/{table.get('data-url')}{query}"
        holdings_req = urllib.request.Request(holdings_url, headers=HEADERS)
        holdings_res = urllib.request.urlopen(holdings_req)
        holdings = json.loads(holdings_res.read().decode("utf-8"))
        for row in holdings["rows"]:
            symbol = html.fromstring(row["symbol"]).text_content()
            weight = float(row["weight"].strip("%"))
            if symbol != "N/A":
                result[symbol] = weight
        time.sleep(1)

    df['Ticker'] = list(result.keys())
    df['Weight'] = list(result.values())
    df = df.sort_values(['Weight'], ascending=False)
    return df