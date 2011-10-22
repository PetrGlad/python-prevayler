"""
    Implementation of "object prevalence" in Python.
    
    @author Petr Gladkikh
"""
import cPickle as pickle
# import traceback, sys, os
import threading
import os.path
import re
from pv.util import baseN, NUMERALS

class Sentry:
    """Log record boundary that is used to detect partial writes.
    It is not part of API"""
    # TODO Put sentry values between transaction records to detect partial writes.
    def __init__(self, serialId):
        self.serialId = serialId;


class Log(object):
    """
    Support for snapshots and log segmentation.
    """
    
    LOG_SUFFIX = "log"
    SNAPSHOT_SUFFIX = "snapshot"    
    PICKLE_PROTOCOL = 0    # ASCII
    
    idNumBase = 10
    reSplitFileName = re.compile('([' + NUMERALS[:idNumBase] + ']+)\.(\w+)')
    
    def __init__(self, dataDir):
        self.serialId = 0
        self.dataDir = dataDir
        self.logFile = None
        
    def formatFileName(self, serialId, suffix):
        # File name is padded for easier sorting
        return baseN(serialId, self.idNumBase).rjust(10, '0') + '.' + suffix
    
    def makeLogFileName(self, serialId):
        return os.path.join(self.dataDir,
                            self.formatFileName(serialId, self.LOG_SUFFIX))
    
    def makeSnapshotName(self, serialId):
        return os.path.join(self.dataDir,
                            self.formatFileName(serialId, self.SNAPSHOT_SUFFIX))

    def logRotate(self, serialId):
        self.close()
        self.logFileName = self.makeLogFileName(serialId + 1)
        self.logFile = open(self.logFileName, 'ab')
        
    def getIndexedList(self, dirName, allFiles, suffix):
        matched = [self.reSplitFileName.match(f) for f in allFiles if f.endswith(suffix)] 
        return sorted([(long(m.group(1), self.idNumBase), os.path.join(dirName, m.group())) 
                       for m in matched if m is not None],
                      key=lambda x: x[0])

    def getPieces(self, allFiles):
        try:
            serialId, snapshot = self.getIndexedList(self.dataDir, allFiles, self.SNAPSHOT_SUFFIX)[-1]            
        except IndexError:
            serialId, snapshot = 0, None
        logList = [x for serial, x in self.getIndexedList(self.dataDir, allFiles, self.LOG_SUFFIX) 
                   if snapshot is None or serial > serialId]            
        return (serialId, snapshot, logList)
    
    def loadInitState(self, initStateConstructor):
        (self.serialId, snapshot, logList) = self.getPieces(os.listdir(self.dataDir))
        self.logRotate(self.serialId)
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
                            tx = pickle.load(log)
                            assert callable(tx)
                            centry = pickle.load(log)                            
                            newSerialId = self.serialId + 1
                            if newSerialId != centry.serialId:
                                raise Exception("Unexpected transaction id in log."
                                                + " Log's transaction #" + centry.serialId 
                                                + ", expected transaction #" + newSerialId 
                                                + ", log filename " + logFileName)
                            else:
                                self.serialId = newSerialId
                            yield tx
                    except EOFError:
                        pass
                finally:
                    log.close()
        return (initialState, replayLog)
    
    def dumpVal(self, value):
        pickle.dump(value, self.logFile, Log.PICKLE_PROTOCOL)
    
    def put(self, value):
        self.serialId += 1
        self.dumpVal(value)
        self.dumpVal(Sentry(self.serialId))
        self.logFile.flush()
        
    def putSnapshot(self, root):
        # TODO refine error handling 
        snapshotFile = open(self.makeSnapshotName(self.serialId), 'ab')        
        pickle.dump(root, snapshotFile, Log.PICKLE_PROTOCOL)
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
                for tn in replayLog():
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
