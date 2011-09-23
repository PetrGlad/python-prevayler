'''
Tests.

@author Petr Gladkikh
'''
import unittest
import glob, os
from pv.core import PSys, Log
from util import NUMERALS

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
    for fn in glob.glob(dirName + "/*.log"):
        os.remove(fn)
    for fn in glob.glob(dirName + "/*.snapshot"):
        os.remove(fn)


class Test(unittest.TestCase):
    
    tempDir = "../../testData"
    
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
        psys = PSys(Log(Test.tempDir), dict)
        psys.exe(Tn1())         # tick = 0
        psys.exe(Tn1())         # 1
        psys.makeSnapshot()
        psys.exe(Tn1())         # 2
        self.assertEquals(psys.root['tick'], 2)        
        psys.log.close()
        
        psys = PSys(Log(Test.tempDir), dict)        
        self.assertEquals(psys.root['tick'], 2)
        
    def testMultipleSnapshots(self):
        "See issue #1 on github."        
        psys = PSys(Log(Test.tempDir), dict)
        for _ in range(7):
            psys.exe(Tn1())         # tick = 0
            psys.exe(Tn1())         # 1
            psys.makeSnapshot()
        psys.exe(Tn1())         # 2
        self.assertEquals(psys.root['tick'], 14)        
        psys.log.close()
        
        psys = PSys(Log(Test.tempDir), dict)        
        self.assertEquals(psys.root['tick'], 14)
        
        
    def testFilenamePattern(self):
        namePattern = Log.reSplitFileName        
        self.assertTrue(namePattern.match(NUMERALS[:Log.idNumBase] + ".karma"))
        self.assertFalse(namePattern.match(NUMERALS))
        
    def testGetPieces(self):
        self.assertEqual(Log("z").getPieces([]), 
                         (0, None, []))
        self.assertEqual(Log("z").getPieces([".log", ".snapshot", "I am Corvax"]), 
                         (0, None, []))        
        # If no snapshot then assuming that we starting from transaction #1 and use all logs.
        self.assertEqual(Log("z").getPieces(["0020.log", "01.log", "12.bordeaux", "2.log", "30.log"]),
                         (0, None, ["z/01.log", "z/2.log", "z/0020.log", "z/30.log"]))
        # snapshot and no logs        
        self.assertEqual(Log("z").getPieces(["0020.snapshot"]),
                         (20L, "z/0020.snapshot", []))
        # multiple snapshots
        self.assertEqual(Log("z").getPieces(["003.snapshot", "0020.snapshot", "01.log", "0014.snapshot", "2.log", "0021.log", "30.log"]),
                         (20L, "z/0020.snapshot", ["z/0021.log", "z/30.log"]))
        # expected log-snapshot relation
        self.assertEqual(Log("z").getPieces(["20.snapshot", "19.log", "20.log", "21.log"]),
                         (20L, "z/20.snapshot", ["z/21.log"]))


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testSnapshot']
    unittest.main()
