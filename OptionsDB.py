import sqlite3
import threading

lock = threading.Lock()


class OptionsDB(object):
	"""sqlite3 database class that holds testers jobs"""
	_DB_FILENAME = 'TCSTest.db'

	def __init__(self, database_name=None):
		"""Initialize db class variables"""

		if database_name is not None:
			self.connection = sqlite3.connect(database_name, check_same_thread=False)
		else:
			self.connection = sqlite3.connect(self._DB_FILENAME, check_same_thread=False)

		self.cur = self.connection.cursor()

		self.cur.execute("""
		CREATE TABLE IF NOT EXISTS "DATE" (
		"LoggingDate"	TEXT UNIQUE NOT NULL,
		PRIMARY KEY("LoggingDate")
		);
		""")

		self.cur.execute("""
		CREATE TABLE IF NOT EXISTS "CALLS" (
		"LoggingDate"	TEXT NOT NULL,
		"Time"		TEXT NOT NULL,
		"ExpiryDate"	TEXT NOT NULL,
		"StrikePrice"	INTEGER NOT NULL,
		"CurrentPrice"	INTEGER NOT NULL,
		"OI"		INTEGER,
		"ChangeInOI"	INTEGER,
		"Volume"	INTEGER,
		"IV"		INTEGER,
		"LTP"		INTEGER,
		"NetChange"	INTEGER,
		"BidQty"	INTEGER,
		"BidPrice"	INTEGER,
		"AskPrice"	INTEGER,
		"AskQty"	INTEGER,
		FOREIGN KEY("LoggingDate") REFERENCES "DATE"("LoggingDate"),
		UNIQUE("LoggingDate", "Time","StrikePrice","ExpiryDate")
		);
		""")

		self.cur.execute("""
		CREATE TABLE IF NOT EXISTS "PUTS" (
		"LoggingDate"	TEXT NOT NULL,
		"Time"		TEXT NOT NULL,
		"ExpiryDate"	TEXT NOT NULL,
		"StrikePrice"	INTEGER NOT NULL,
		"CurrentPrice"	INTEGER NOT NULL,
		"OI"		INTEGER,
		"ChangeInOI"	INTEGER,
		"Volume"	INTEGER,
		"IV"		INTEGER,
		"LTP"		INTEGER,
		"NetChange"	INTEGER,
		"BidQty"	INTEGER,
		"BidPrice"	INTEGER,
		"AskPrice"	INTEGER,
		"AskQty"	INTEGER,
		FOREIGN KEY("LoggingDate") REFERENCES "DATE"("LoggingDate"),
		UNIQUE("LoggingDate", "Time","StrikePrice","ExpiryDate")
		);
		""")

		self.commit()

	def close(self):
		"""close sqlite3 connection"""
		self.connection.close()

	def execute(self, new_data, args=''):
		try:
			lock.acquire(True)
			"""execute a row of data to current cursor"""
			self.cur.execute(new_data, args)
		finally:
			self.commit()
			lock.release()

	def executemany(self, new_data, args=''):
		try:
			lock.acquire(True)
			"""execute a row of data to current cursor"""
			self.cur.executemany(new_data, args)
		finally:
			self.commit()
			lock.release()

	def commit(self):
		"""commit changes to database"""
		self.connection.commit()

	def __enter__(self):
		return self

	def __exit__(self, ext_type, exc_value, traceback):
		self.cur.close()
		if isinstance(exc_value, Exception):
			self.connection.rollback()
		else:
			self.connection.commit()
		self.connection.close()
