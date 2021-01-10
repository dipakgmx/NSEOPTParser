import requests
import sqlite3
from bs4 import BeautifulSoup
import re
import dateutil.parser as dparser
conn = sqlite3.connect('TCS.db')
cursor = conn.cursor()

nse_headers = {'Accept': '*/*',
               'Accept-Encoding': 'gzip, deflate, sdch, br',
               'Accept-Language': 'en-GB,en-US;q=0.8,en;q=0.6',
               'Connection': 'keep-alive',
               'Host': 'www1.nseindia.com',
               'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
               'X-Requested-With': 'XMLHttpRequest'}

optionValues = {'segmentLink': 17,
                'instrument': 'OPTSTK',
                'symbol': 'TCS',
                'date': '28JAN2021'
                }

options_url = 'https://www1.nseindia.com/live_market/dynaContent/live_watch/option_chain/optionKeys.jsp?'

response = requests.get(
    options_url,
    params=optionValues,
    headers=nse_headers,
)

soup = BeautifulSoup(response.text, 'html5lib')
info_table = soup.find("table", attrs={"width": "100%"})
texts = info_table.find_all("span")
currentStockValue = re.findall("\d+\.\d+",texts[0].text)
currentDateAndTime = dparser.parse(texts[1].text,fuzzy=True)

# # Insert a row of data
try:
    conn.execute("INSERT OR IGNORE INTO DATE(date) values (?)",
                [str(currentDateAndTime.date())])
    conn.execute("INSERT OR IGNORE INTO EXPIRY(ExpiryDate,Date) values (?, ?)",
                 ([optionValues.get('date'),str(currentDateAndTime.date())]))

    conn.execute("INSERT OR IGNORE INTO "
                 "PRICES(Time, ExpiryDate) values (?, ?)",
                 ([str(currentDateAndTime.time()),
                   optionValues.get('date')])
                 )
    conn.commit()

except sqlite3.Error as err:
    print(err)
    conn.close()


conn.commit()
options_table = soup.find("table", attrs={"id": "octable"})

rows = options_table.find('tbody').find_all('tr')
data = []
for row in rows:
    cols = row.find_all('td')
    cols = [ele.text.strip() for ele in cols]
    data.append([ele for ele in cols if ele])  # Get rid of empty values

def parseValues(readValues):
    # Writing strike values across DBs
    try:
        callValues = readValues[0:10]

        conn.execute("INSERT OR IGNORE INTO "
                     "CALLS(Time, StrikePrice, OI, ChangeInOI,Volume,IV,LTP,NetChange,BidQty,BidPrice,AskPrice,AskQty) "
                     "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                     ([str(currentDateAndTime.time()),
                       readValues[10],
                       callValues[0],
                       callValues[1],
                       callValues[2],
                       callValues[3],
                       callValues[4],
                       callValues[5],
                       callValues[6],
                       callValues[7],
                       callValues[8],
                       callValues[9]])
                     )

        putValues = readValues[::-1][0:10]
        conn.execute("INSERT OR IGNORE INTO "
                     "PUTS(Time, StrikePrice, OI, ChangeInOI,Volume,IV,LTP,NetChange,BidQty,BidPrice,AskPrice,AskQty) "
                     "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                     ([str(currentDateAndTime.time()),
                       readValues[10],
                       putValues[0],
                       putValues[1],
                       putValues[2],
                       putValues[3],
                       putValues[4],
                       putValues[5],
                       putValues[6],
                       putValues[7],
                       putValues[8],
                       putValues[9]])
                     )
        conn.commit()

    except sqlite3.Error as err:
        print(err)
        conn.close()

for row in data[:-1]:
    parseValues(row)

conn.close()