'''
@author: petr
'''
import unittest
import glob, os
from pv.core import PSys, Log

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
                        

if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testSnapshot']
    unittest.main()
