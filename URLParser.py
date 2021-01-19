import time
import requests
import dateutil.parser as dparser
from datetime import datetime
from pytz import timezone
from OptionsDB import OptionsDB

options_url = 'https://www.nseindia.com/api/option-chain-indices?'

class URLParser(object):

	def __init__(self, index):
		self.index = index
		self.database = OptionsDB(index)
		self.timezone = timezone('Asia/Kolkata')
		self.url_params = {'symbol': self.index}
		self.nse_headers = {
			'accept': '*/*',
			'accept-encoding': 'gzip, deflate, br',
			'accept-language': 'en-US,en;q=0.9,it;q=0.8,de;q=0.7,kn;q=0.6',
			'Connection': 'keep-alive',
			'Host': 'www.nseindia.com',
			'referer': 'https://www.nseindia.com/option-chain',
			'user-agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) '
						  'AppleWebKit/537.36 (KHTML, like Gecko) '
						  'Chrome/87.0.4280.141 Mobile Safari/537.36'
		}

	def todayAt(self, hr, min=0, sec=0, micros=0):
		"""Convert time to india time zone"""
		now = datetime.now(self.timezone)
		return now.replace(hour=hr, minute=min, second=sec, microsecond=micros)

	def startParsing(self):
		while True:
			time_now = datetime.now(self.timezone)
			if (time_now > self.todayAt(8, 55)) and (time_now < self.todayAt(15, 35)):
				try:
					self.response = requests.get(
						options_url,
						params=self.url_params,
						headers=self.nse_headers,
					)
					self.response.raise_for_status()
					self.data = self.response.json()

					current_date_and_time = dparser.parse(self.data.get('records', {}).get('timestamp', {}), fuzzy=True)
					self.current_date = str(current_date_and_time.date())
					self.current_time = str(current_date_and_time.time())
					self.current_price = self.data.get('records', {}).get('underlyingValue', {})

					self.database.execute("INSERT OR IGNORE INTO DATE"
										  "(LoggingDate) "
										  "values (?)",
										  [self.current_date])

					for row in self.data.get('records', {}).get('data', {}):
						if row.get('CE', {}):
							self.database.execute("INSERT OR IGNORE INTO "
												  "CALLS(LoggingDate, Time, ExpiryDate, StrikePrice,"
												  "CurrentPrice, OI, ChangeInOI, Volume, IV, LTP,"
												  "NetChange, BidQty, BidPrice, AskPrice, AskQty)"
												  "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
													[self.current_date,
													self.current_time,
													str(dparser.parse(row.get('CE', {}).get('expiryDate')).date()),
													row.get('CE', {}).get('strikePrice'),
													self.current_price,
													row.get('CE', {}).get('openInterest'),
													row.get('CE', {}).get('changeinOpenInterest'),
													row.get('CE', {}).get('totalTradedVolume'),
													row.get('CE', {}).get('impliedVolatility'),
													row.get('CE', {}).get('lastPrice'),
													row.get('CE', {}).get('change'),
													row.get('CE', {}).get('bidQty'),
													row.get('CE', {}).get('bidprice'),
													row.get('CE', {}).get('askQty'),
													row.get('CE', {}).get('askPrice')])

						if row.get('PE', {}):
							self.database.execute("INSERT OR IGNORE INTO "
												  "PUTS(LoggingDate, Time, ExpiryDate, StrikePrice,"
												  "CurrentPrice, OI, ChangeInOI, Volume, IV, LTP,"
												  "NetChange, BidQty, BidPrice, AskPrice, AskQty)"
												  "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
												  [self.current_date,
												   self.current_time,
												   str(dparser.parse(row.get('PE', {}).get('expiryDate')).date()),
												   row.get('PE', {}).get('strikePrice'),
												   self.current_price,
												   row.get('PE', {}).get('openInterest'),
												   row.get('PE', {}).get('changeinOpenInterest'),
												   row.get('PE', {}).get('totalTradedVolume'),
												   row.get('PE', {}).get('impliedVolatility'),
												   row.get('PE', {}).get('lastPrice'),
												   row.get('PE', {}).get('change'),
												   row.get('PE', {}).get('bidQty'),
												   row.get('PE', {}).get('bidprice'),
												   row.get('PE', {}).get('askQty'),
												   row.get('PE', {}).get('askPrice')])

					time.sleep(110)

				except requests.exceptions.HTTPError as errh:
					print("Http Error:", errh)
					time.sleep(10)

				except requests.exceptions.ConnectionError as errc:
					print("Error Connecting:", errc)
				except requests.exceptions.Timeout as errt:
					print("Timeout Error:", errt)
				except requests.exceptions.RequestException as err:
					print("OOps: Something Else", err)
