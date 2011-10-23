'''
Created on Oct 23, 2011

@author: petr
'''

import fcntl, os # for FsLock

# TODO Move this class to own file?        
class FsLock:
    def __init__(self, dirName, lockName):
        assert os.name == 'posix'
        self.dirName = dirName
        self.lockName = lockName
        self.lockFile = None        
        
    def acquire(self):
        assert self.lockFile is None
        lockFile = open(os.path.join(self.dirName, self.lockName), 'a+')
        fcntl.flock(lockFile, fcntl.LOCK_EX | fcntl.LOCK_NB)
        self.lockFile = lockFile
        
    def release(self):
        self.lockFile.close()
        self.lockFile = None


class VoidLock:
    "Stub for systems where FsLock is not available"
    def acquire(self): pass
    def release(self): pass    


def newLock(dataDir, lockFile):
    if os.name != 'posix':
        # TODO Use logger for warnings?
        print (u"Locks are not supported on this os, expected 'posix'." 
               + " Implement lock support for '" + os.name + "' or configure prevayler without locks.")
        return VoidLock()
    else:
        return FsLock(dataDir, lockFile)
    
    
import unittest, errno
class Test(unittest.TestCase):
    tempDir = u"./testData"
    lockName = u"xyzzy"
    
    @classmethod
    def setUpClass(cls):
        super(Test, cls).setUpClass()
        if not os.path.isdir(Test.tempDir):
            os.makedirs(Test.tempDir)
    
    def testFsLock(self):        
        a = FsLock(self.tempDir, self.lockName)
        b = FsLock(self.tempDir, self.lockName)
        a.acquire()
        try:
            b.acquire()
            self.fail(u'Exception expected')
        except IOError, e:
            self.assertEqual(e.errno, errno.EAGAIN)
        a.release()
        b.acquire()
