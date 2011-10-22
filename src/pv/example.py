'''
Example code

@author: petr
'''
import pv.core
from pv.test import Tn1, Tn2
import os 

dataDir = "../../data"

if __name__ == "__main__":
    print "----hi----"
    if not os.path.isdir(dataDir):
        os.makedirs(dataDir)
    import time
    t1 = time.time()    
    
    psys = pv.core.init(dataDir, dict)
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
    print '\n'.join([`key` + '->' + `v` for key, v in psys.root.iteritems()])    
    print "----bye----\n"
