import cPickle

class C:
    def __init__(self, val):
        self.a = val
    class Inner:
        pass
    
logName = 'lab.pickle'    
f = file(logName, 'a')
cPickle.dump(C('first'), f, cPickle.HIGHEST_PROTOCOL)
cPickle.dump(C('second'), f, cPickle.HIGHEST_PROTOCOL)
f.close()

f = file(logName, 'r')
print cPickle.load(f).a
print cPickle.load(f).a
f.close()
