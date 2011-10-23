'''
Tests.

@author Petr Gladkikh
'''
import unittest
import glob, os
from pv.core import PSys, Log
from .FsLock import VoidLock
from .util import NUMERALS
import os.path

import warnings
warnings.simplefilter('default') # re-enable DeprecationWarning in python 2.7

class Tn1:
    def __call__(self, root):
        if 'tick' in root:
            root['tick'] += 1
        else:
            root['tick'] = 0


class Tn2:
    def __init__(self, name, txnId):
        self.name = name
        self.id = txnId
        
    def __call__(self, root):
        if self.id in root:
            root[self.id] = root[self.id].swapcase() 
        else:
            root[self.id] = self.name


def clearState(dirName):
    for fn in glob.glob(os.path.join(dirName, u"*.log")):
        os.remove(fn)
    for fn in glob.glob(os.path.join(dirName, u"*.snapshot")):
        os.remove(fn)


class Test(unittest.TestCase):
    
    tempDir = u"./testData"
    
    @classmethod
    def setUpClass(cls):
        super(Test, cls).setUpClass()
        if not os.path.isdir(Test.tempDir):
            os.makedirs(Test.tempDir)
            
    def setUp(self):
        unittest.TestCase.setUp(self)
        clearState(Test.tempDir)
     
    def testSnapshot(self):
        "See issue #1 on github."        
        psys = PSys(Log(Test.tempDir, VoidLock()), dict)
        psys.exe(Tn1())         # tick = 0
        psys.exe(Tn1())         # 1
        psys.makeSnapshot()
        psys.exe(Tn1())         # 2
        self.assertEquals(psys.root[u'tick'], 2)
        psys.log.close()
        
        psys = PSys(Log(Test.tempDir, VoidLock()), dict)
        self.assertEquals(psys.root[u'tick'], 2)
        
    def testMultipleSnapshots(self):
        "See issue #1 on github."        
        psys = PSys(Log(Test.tempDir, VoidLock()), dict)
        for _ in range(7):
            psys.exe(Tn1())         # tick = 0
            psys.exe(Tn1())         # 1
            psys.makeSnapshot()
        psys.exe(Tn1())         # 2
        self.assertEquals(psys.root[u'tick'], 14)        
        psys.log.close()
        
        psys = PSys(Log(Test.tempDir, VoidLock()), dict)
        self.assertEquals(psys.root[u'tick'], 14)

    def testFilenamePattern(self):
        namePattern = Log.reSplitFileName
        self.assertTrue(namePattern.match(NUMERALS[:Log.idNumBase] + u".karma"))
        self.assertFalse(namePattern.match(NUMERALS))
        
    def testGetPieces(self):
        def newLog(): return Log(u"z", VoidLock())
        self.assertEqual(newLog().getPieces([]),
                         (0, None, []))
        self.assertEqual(newLog().getPieces([u".log", u".snapshot", u"I am Corvax"]),
                         (0, None, []))        
        # If no snapshot then assuming that we starting from transaction #1 and use all logs.
        self.assertEqual(newLog().getPieces([u"0020.log", u"01.log", u"12.bordeaux", u"2.log", u"30.log"]),
                         (0, None, [u"z/01.log", u"z/2.log", u"z/0020.log", u"z/30.log"]))
        # snapshot and no logs        
        self.assertEqual(newLog().getPieces([u"0020.snapshot"]),
                         (20L, u"z/0020.snapshot", []))
        # multiple snapshots
        self.assertEqual(newLog().getPieces([u"003.snapshot", u"0020.snapshot", u"01.log", u"0014.snapshot", u"2.log", u"0021.log", u"30.log"]),
                         (20L, u"z/0020.snapshot", [u"z/0021.log", u"z/30.log"]))
        # expected log-snapshot relation
        self.assertEqual(newLog().getPieces([u"20.snapshot", u"19.log", u"20.log", u"21.log"]),
                         (20L, u"z/20.snapshot", [u"z/21.log"]))


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testSnapshot']
    unittest.main()
