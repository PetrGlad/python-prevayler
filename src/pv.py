"""
    Implementation of "object prevalence" in Python.
    
    Author Petr Gladkikh
"""
import cPickle as pickle
import traceback
import sys
import os
import threading
import os.path
import re


class Log(object):
    """
    Support for snapshots and log segmentation.
    
    TODO Put sentry values between transaction records to detect partial writes.        
    """
    
    LOG_SUFFIX = "log"
    SNAPSHOT_SUFFIX = "snapshot"
    
    reSplitFileName = re.compile('(\d+)\.(\w+)')
    
    def __init__(self, dataDir):
        self.serialId = 0
        self.dataDir = dataDir
        self.logFile = None        
    
    def makeLogFileName(self, serialId):
        return os.path.join(self.dataDir, ("%010u." + self.LOG_SUFFIX) % serialId)
    
    def makeSnapshotName(self, serialId):
        return os.path.join(self.dataDir, ("%010u." + self.SNAPSHOT_SUFFIX) % serialId)
    
    def findPieces(self):
        allFiles = os.listdir(self.dataDir)
        allFiles.sort()
        snapshot = None
        logList = []
        serialId = 0
        for fName in allFiles:
            m = self.reSplitFileName.match(fName)
            if m:
                thisId = int(m.group(1))
                suffix = m.group(2)
                if suffix == self.SNAPSHOT_SUFFIX:
                    logList = [] # no purge implemented yet
                    snapshot = os.path.join(self.dataDir, fName)
                    serialId = thisId
                elif suffix == self.LOG_SUFFIX:
                    if snapshot is None:
                        serialId = thisId
                    logList.append(os.path.join(self.dataDir, fName))
        self.logFileName = self.makeLogFileName(serialId + 1) 
        self.logFile = open(self.logFileName, 'ab')
        return (serialId, snapshot, logList)
    
    def loadInitState(self, initStateConstructor):        
        (self.serialId, snapshot, logList) = self.findPieces()        
        if snapshot:
            initialState = pickle.load(open(snapshot, 'rb'))
        else:
            initialState = initStateConstructor()
        def replayLog():
            for logFileName in logList:
                log = open(logFileName, 'rb')
                try:                    
                    try:
                        while True:
                            yield pickle.load(log)
                    except EOFError:
                        pass
                finally:
                   log.close()
        return (initialState, replayLog)
    
    def put(self, value):
        self.serialId += 1
        pickle.dump(value, self.logFile, pickle.HIGHEST_PROTOCOL)
        
    def putSnapshot(self, root):
        # TODO refine error handling 
        snapshotFile = open(self.makeSnapshotName(self.serialId), 'ab')        
        pickle.dump(root, snapshotFile, pickle.HIGHEST_PROTOCOL)
        snapshotFile.close()
        
    def close(self):
        self.logFile.close()


class PSys(object):
    """
    Persistent ("prevalent") system. 
    """
    
    def __init__(self, log, rootConstructor):
        self.log = log
        self.lock = threading.Lock()
        self.tnCount = 0 # for debugging
        self.load(log, rootConstructor)        
        
    def load(self, log, rootConstructor):
        try:
            self.lock.acquire()
            (self.root, replayLog) = self.log.loadInitState(rootConstructor)
            try:
                iter = replayLog()
                for tn in iter:
                    tn(self.root)
                    self.tnCount += 1
            except StopIteration: pass                
        finally:
            self.lock.release()
            
    def makeSnapshot(self):
        try:
            self.lock.acquire()
            self.log.putSnapshot(self.root)
        finally:
            self.lock.release()
    
    def exe(self, tn):
        """
        Execute transaction.
        """
        try:
            self.lock.acquire()
            tn(self.root)
            self.log.put(tn)
            self.tnCount += 1
        finally:
            self.lock.release()


# ---------------------------------------------------------
# Test code


class Tn1:
    def __call__(self, root):
        if 'tick' in root:
            root['tick'] += 1
        else:
            root['tick'] = 0


class Tn2:
    def __init__(self, name, id):
        self.name = name
        self.id = id
        
    def __call__(self, root):
        if self.id in root:
            root[self.id] = root[self.id].swapcase() 
        else:
            root[self.id] = self.name


if __name__ == "__main__":
    print "----hi----"
    import time
    t1 = time.time()    
    psys = PSys(Log('../dat'), dict)
    print "Load time" , time.time() - t1
    print 'Transactions count', psys.tnCount
    print "----loaded----"    
    print "\nPerforming transactions ..."
    t1 = time.time()
    psys.exe(Tn1())
    k = 0
    for n in xrange(1000):
        for name in ['Jane', 'Joos', 'Joomla', 'Kritter']:                                
            psys.exe(Tn2(name, k))
            k += 1
    print "Transactions time" , time.time() - t1
    print 'Transactions count', psys.tnCount
    psys.makeSnapshot()
    psys.log.close()
    print "-----final state----"
    print '\n'.join([`key`+'->'+`v` for key, v in psys.root.iteritems()])    
    print "----bye----\n"
