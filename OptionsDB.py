import sqlite3


class OptionsDB(object):
    """sqlite3 database class that holds testers jobs"""
    _DB_FILENAME = 'TCSTest.db'

    def __init__(self, database_name=None):
        """Initialize db class variables"""

        if database_name is not None:
            self.connection = sqlite3.connect(database_name)
        else:
            self.connection = sqlite3.connect(self._DB_FILENAME)

        self.cur = self.connection.cursor()

        self.cur.execute("""
                        CREATE TABLE IF NOT EXISTS "DATE" (
                        "LoggingDate"	TEXT UNIQUE,
                        PRIMARY KEY("LoggingDate"));
                        """)

        self.cur.execute("""
                    CREATE TABLE IF NOT EXISTS "EXPIRY" (
                    "ExpiryDate"	TEXT NOT NULL UNIQUE,
                    "LoggingDate"	TEXT NOT NULL,
                    FOREIGN KEY("LoggingDate") REFERENCES "DATE"("LoggingDate"),
                    PRIMARY KEY("ExpiryDate"),
                    UNIQUE("ExpiryDate","LoggingDate")
                    );
                    """)

        self.cur.execute("""
                    CREATE TABLE IF NOT EXISTS "PRICES" (
                    "Time"	TEXT NOT NULL UNIQUE,
                    "ExpiryDate"	TEXT NOT NULL,
                    PRIMARY KEY("Time"),
                    UNIQUE("Time","ExpiryDate"),
                    FOREIGN KEY("ExpiryDate") REFERENCES "EXPIRY"("ExpiryDate")
                );
                """)

        self.cur.execute("""
                    CREATE TABLE IF NOT EXISTS "CALLS" (
                    "Time"	TEXT NOT NULL,
                    "ExpiryDate"	TEXT,
                    "StrikePrice"	INTEGER NOT NULL,
                    "OI"	INTEGER,
                    "ChangeInOI"	INTEGER,
                    "Volume"	INTEGER,
                    "IV"	INTEGER,
                    "LTP"	INTEGER,
                    "NetChange"	INTEGER,
                    "BidQty"	INTEGER,
                    "BidPrice"	INTEGER,
                    "AskPrice"	INTEGER,
                    "AskQty"	INTEGER,
                    UNIQUE("Time","StrikePrice"),
                    FOREIGN KEY("Time") REFERENCES "PRICES"("Time"),
                    PRIMARY KEY("StrikePrice"),
                    FOREIGN KEY("ExpiryDate") REFERENCES "PRICES"("ExpiryDate"));
                    """)

        self.cur.execute("""
                    CREATE TABLE IF NOT EXISTS "PUTS" (
                    "Time"	TEXT NOT NULL,
                    "ExpiryDate"	TEXT,
                    "StrikePrice"	INTEGER NOT NULL,
                    "OI"	INTEGER,
                    "ChangeInOI"	INTEGER,
                    "Volume"	INTEGER,
                    "IV"	INTEGER,
                    "LTP"	INTEGER,
                    "NetChange"	INTEGER,
                    "BidQty"	INTEGER,
                    "BidPrice"	INTEGER,
                    "AskPrice"	INTEGER,
                    "AskQty"	INTEGER,
                    UNIQUE("Time","StrikePrice"),
                    FOREIGN KEY("Time") REFERENCES "PRICES"("Time"),
                    PRIMARY KEY("StrikePrice"),
                    FOREIGN KEY("ExpiryDate") REFERENCES "PRICES"("ExpiryDate"));
                    """)

        self.commit()

    def close(self):
        """close sqlite3 connection"""
        self.connection.close()

    def execute(self, new_data, args=''):
        """execute a row of data to current cursor"""
        self.cur.execute(new_data, args)
        self.commit()

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