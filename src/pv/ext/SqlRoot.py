'''
Object for Prevayler system that includes connection to SQLite.
Mon Sep 26 17:46:59 EDT 2011
'''
import sqlite3 

class SqlRoot:
    """ This class allows storing data that benefits from using SQL in SQLite DB.
    So you can manipulate data on db connection self.dbconn via normal dbapi 
    in prevalyer transactions and state of db will be persisted.  
    Instance of this class can be used as root of prevalent system.    
    """
    def __init__(self):        
        self.connect()

    def __getstate__(self):
        return ";".join(list(self.dbconn.iterdump()))

    def __setstate__(self, value):            
        self.connect()
        self.dbconn.executescript(value)

    def connect(self):
        self.dbconn = sqlite3.connect(
            ':memory:',
            detect_types = sqlite3.PARSE_DECLTYPES,
            isolation_level = "IMMEDIATE"
            )
