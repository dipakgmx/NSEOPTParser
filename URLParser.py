import threading
import logging
import time

import requests
from bs4 import BeautifulSoup
import re
import dateutil.parser as dparser
from OptionsDB import OptionsDB

nse_headers = {'Accept': '*/*',
               'Accept-Encoding': 'gzip, deflate, sdch, br',
               'Accept-Language': 'en-GB,en-US;q=0.8,en;q=0.6',
               'Connection': 'keep-alive',
               'Host': 'www1.nseindia.com',
               'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
               'X-Requested-With': 'XMLHttpRequest'}

options_url = 'https://www1.nseindia.com/live_market/dynaContent/live_watch/option_chain/optionKeys.jsp?'


def threaded(fn):
    def wrapper(*args, **kwargs):
        thread = threading.Thread(target=fn, args=args, kwargs=kwargs)
        thread.start()
        return thread

    return wrapper


class URLParser(object):

    def __init__(self, index):
        self.index = index
        self.database = OptionsDB(index)

    @threaded
    def get(self, strikeDate):
        if 'NIFTY' not in self.index:
            optionValues = {'segmentLink': 17,
                            'instrument': 'OPTSTK',
                            'symbol': self.index,
                            'date': strikeDate
                            }
        else:
            optionValues = {'segmentLink': 17,
                            'instrument': 'OPTIDX',
                            'symbol': self.index,
                            'date': strikeDate
                            }
        while True:
            try:
                response = requests.get(
                    options_url,
                    params=optionValues,
                    headers=nse_headers,
                )
                response.raise_for_status()

            except requests.exceptions.HTTPError as errh:
                print("Http Error:", errh)
            except requests.exceptions.ConnectionError as errc:
                print("Error Connecting:", errc)
            except requests.exceptions.Timeout as errt:
                print("Timeout Error:", errt)
            except requests.exceptions.RequestException as err:
                print("OOps: Something Else", err)

            soup = BeautifulSoup(response.text, 'html5lib')
            info_table = soup.find("table", attrs={"width": "100%"})
            texts = info_table.find_all("span")
            currentStockValue = ''.join(re.findall("\d+\.\d+", texts[0].text))
            currentDateAndTime = dparser.parse(texts[1].text, fuzzy=True)

            self.database.execute("INSERT OR IGNORE INTO DATE"
                                  "(LoggingDate) "
                                  "values (?)",
                                  [str(currentDateAndTime.date())])
            self.database.execute("INSERT OR IGNORE INTO EXPIRY"
                                  "(ExpiryDate,LoggingDate) "
                                  "values (?, ?)",
                                  ([optionValues.get('date'),
                                    str(currentDateAndTime.date())]))

            self.database.execute("INSERT OR IGNORE INTO PRICES"
                                  "(Time, ExpiryDate, CurrentPrice) "
                                  "values (?, ?, ?)",
                                  ([str(currentDateAndTime.time()),
                                    optionValues.get('date'),
                                    currentStockValue]))

            options_table = soup.find("table", attrs={"id": "octable"})

            rows = options_table.find('tbody').find_all('tr')

            data = []
            for row in rows:
                cols = row.find_all('td')
                cols = [ele.text.strip() for ele in cols]
                data.append([ele for ele in cols if ele])  # Get rid of empty values

            for row in data[:-1]:
                self.writeCallsAndPuts(row, currentDateAndTime, optionValues, currentStockValue)
            time.sleep(0.5)

    def writeCallsAndPuts(self, readValues, currentDateAndTime, optionValues, currentStockValue):
        # Writing strike values across DBs
        callValues = readValues[0:10]

        self.database.execute("INSERT OR IGNORE INTO "
                              "CALLS(Time, ExpiryDate, StrikePrice, CurrentPrice, OI, ChangeInOI,Volume,IV,LTP,NetChange,BidQty,BidPrice,AskPrice,AskQty) "
                              "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                              ([str(currentDateAndTime.time()),
                                optionValues.get('date'),
                                readValues[10],
                                currentStockValue,
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
        self.database.execute("INSERT OR IGNORE INTO "
                              "PUTS(Time, ExpiryDate, StrikePrice, CurrentPrice, OI, ChangeInOI,Volume,IV,LTP,NetChange,BidQty,BidPrice,AskPrice,AskQty) "
                              "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                              ([str(currentDateAndTime.time()),
                                optionValues.get('date'),
                                readValues[10],
                                currentStockValue,
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