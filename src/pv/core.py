"""
    Implementation of "object prevalence" in Python.
    
    Author Petr Gladkikh
"""
import cPickle as pickle
# import traceback, sys, os
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

    def logRotate(self, serialId):
        self.close()
        self.logFileName = self.makeLogFileName(serialId + 1)
        self.logFile = open(self.logFileName, 'ab')

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
        self.logRotate(serialId)
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
        self.logRotate(self.serialId)
        
    def close(self):        
        if self.logFile is not None:                        
            self.logFile.close()
            self.logFile = None


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
                itr = replayLog()
                for tn in itr:
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
