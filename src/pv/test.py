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
         
    def testEndOfUniverse(self):
        "See issue #3 on github."
        class TestLog(Log):
            def findPieces(self):
                (serialId, snapshot, logList) = Log.findPieces(self)
                return (serialId + pow(2, 96) - 1, snapshot, logList)
        log = TestLog(Test.tempDir)
        psys = PSys(log, dict)
        count = 10000
        for _ in range(count):        
            psys.exe(Tn1())
        self.assertEquals(psys.root['tick'], count - 1)        
        psys.log.close()        
        psys = PSys(Log(Test.tempDir), dict)        
        self.assertEquals(psys.root['tick'], count - 1)
        
    def testFilenamePattern(self):
        namePattern = Log.reSplitFileName        
        self.assertTrue(namePattern.match(NUMERALS + ".karma"))
        self.assertFalse(namePattern.match(NUMERALS))
                        

if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testSnapshot']
    unittest.main()
