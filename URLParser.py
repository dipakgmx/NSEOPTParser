import threading
import time
import requests
from bs4 import BeautifulSoup
import re
import dateutil.parser as dparser
from datetime import datetime
from pytz import timezone
from requests_html import HTMLSession
import urllib.parse as urlparse
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
        self.timezone = timezone('Asia/Kolkata')
        self.expiryDates = []

    def todayAt(self, hr, min=0, sec=0, micros=0):
        now = datetime.now(self.timezone)
        return now.replace(hour=hr, minute=min, second=sec, microsecond=micros)

    def getExpiryDates(self):
        if 'NIFTY' not in self.index:
            option_values = {'segmentLink': 17,
                            'instrument': 'OPTSTK',
                            'symbol': self.index,
                             'date': 'select'
                             }
        else:
            option_values = {'segmentLink': 17,
                            'instrument': 'OPTIDX',
                            'symbol': self.index,
                             'date': 'select'
                             }

        session = HTMLSession()

        url_parse = urlparse.urlparse(options_url)
        query = url_parse.query
        url_dict = dict(urlparse.parse_qsl(query))
        url_dict.update(option_values)
        url_new_query = urlparse.urlencode(url_dict)
        url_parse = url_parse._replace(query=url_new_query)
        url_with_params = urlparse.urlunparse(url_parse)

        request = session.get(url_with_params)
        soup = BeautifulSoup(request.text, 'html5lib')

        form_values = soup.find("form", attrs={"name": "ocForm"})
        dates = form_values.select('option[value]')
        self.expiryDates = [date['value'] for date in dates[1:]]
        return self.expiryDates

    def startParsing(self):
        expDates = self.getExpiryDates()
        for expDay in expDates:
            self.get(expDay)

    @threaded
    def get(self, strike_date):
        if 'NIFTY' not in self.index:
            option_values = {'segmentLink': 17,
                            'instrument': 'OPTSTK',
                            'symbol': self.index,
                            'date': strike_date
                            }
        else:
            option_values = {'segmentLink': 17,
                            'instrument': 'OPTIDX',
                            'symbol': self.index,
                            'date': strike_date
                            }
        while True:
            # time_now = datetime.now(self.timezone)
            # if (time_now > self.todayAt(9)) and (time_now < self.todayAt(15,31)):
            try:
                response = requests.get(
                    options_url,
                    params=option_values,
                    headers=nse_headers,
                )
                response.raise_for_status()

                soup = BeautifulSoup(response.text, 'html5lib')
                info_table = soup.find("table", attrs={"width": "100%"})
                texts = info_table.find_all("span")
                current_stock_value = ''.join(re.findall("\d+\.\d+", texts[0].text))
                current_date_and_time = dparser.parse(texts[1].text, fuzzy=True)

                self.database.execute("INSERT OR IGNORE INTO DATE"
                                      "(LoggingDate) "
                                      "values (?)",
                                      [str(current_date_and_time.date())])

                options_table = soup.find("table", attrs={"id": "octable"})
                someDate = str(dparser.parse(option_values.get('date')).date()),

                rows = options_table.find('tbody').find_all('tr')

                data = []

                for row in rows:
                    cols = row.find_all('td')
                    cols = [ele.text.strip() for ele in cols]
                    data.append([ele for ele in cols if ele])  # Get rid of empty values

                for row in data[:-1]:
                    self.writeCallsAndPuts(row, current_date_and_time, option_values, current_stock_value)

            except requests.exceptions.HTTPError as errh:
                print("Http Error:", errh)
            except requests.exceptions.ConnectionError as errc:
                print("Error Connecting:", errc)
            except requests.exceptions.Timeout as errt:
                print("Timeout Error:", errt)
            except requests.exceptions.RequestException as err:
                print("OOps: Something Else", err)

            time.sleep(1)

    def writeCallsAndPuts(self, readValues, currentDateAndTime, optionValues, currentStockValue):
        # Writing strike values across DBs
        callValues = readValues[0:10]

        self.database.execute("INSERT OR IGNORE INTO "
                              "CALLS(LoggingDate, Time, ExpiryDate, StrikePrice, CurrentPrice, "
                              "OI, ChangeInOI, Volume, IV, LTP, NetChange, BidQty, BidPrice, "
                              "AskPrice, AskQty) "
                              "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                              ([str(currentDateAndTime.date()),
                                str(currentDateAndTime.time()),
                                str(dparser.parse(optionValues.get('date')).date()),
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
                              "PUTS(LoggingDate, Time, ExpiryDate, StrikePrice, CurrentPrice, "
                              "OI, ChangeInOI, Volume, IV, LTP, NetChange, BidQty, BidPrice,"
                              " AskPrice, AskQty) "
                              "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                              ([str(currentDateAndTime.date()),
                                str(currentDateAndTime.time()),
                                str(dparser.parse(optionValues.get('date')).date()),
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
