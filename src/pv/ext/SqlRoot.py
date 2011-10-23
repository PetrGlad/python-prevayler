'''
Object for Prevayler system that includes connection to SQLite.
Mon Sep 26 17:46:59 EDT 2011
'''
import sqlite3 

class SqlRoot:
    """ This class allows storing data that benefits from using SQL in SQLite DB.
    So you can manipulate data on db connection self.dbconn via normal dbapi 
    in prevalyer transactions and state of db will be persisted.
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
            u':memory:',
            detect_types=sqlite3.PARSE_DECLTYPES,
            isolation_level=u"IMMEDIATE")


import unittest, os, shutil
from pv.core import init

class TestSqlTn1:
    def __call__(self, r):
        r.dbconn.execute(u"create table test (name varchar(32));")
              
class TestSqlTn2:
    def __call__(self, r):
        r.dbconn.execute(u"insert into test (name) values ('abc');")

class Test(unittest.TestCase):
    tempDir = u'./sqlTestData'

    # TODO dry unittest code (extract directory preparation)
    @classmethod
    def setUpClass(cls):
        super(Test, cls).setUpClass()
        if os.path.isdir(Test.tempDir):
            shutil.rmtree(Test.tempDir)        
        os.makedirs(Test.tempDir)            
    
    def testSql(self):
        # NOte: to mix with other data you may use other constructor e.g. lamba : {'sql' : SqlRoot()}        
        pv = init(self.tempDir, SqlRoot)         
        pv.exe(TestSqlTn1())
        pv.exe(TestSqlTn2())
        def fetchall(pvl):
            return pvl.root.dbconn.execute(u"select * from test").fetchall()
            
        self.assertEquals(fetchall(pv), [(u'abc',)])
        pv.shutdown()
        
        pv2 = init(Test.tempDir, SqlRoot)
        self.assertEquals(fetchall(pv2), [(u'abc',)])
        pv2.makeSnapshot()
        pv2.shutdown()
        
        pv3 = init(Test.tempDir, SqlRoot)
        self.assertEquals(fetchall(pv3), [(u'abc',)])
