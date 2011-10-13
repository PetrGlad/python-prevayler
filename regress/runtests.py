import doctest

modules = ()
files = (
    'test_hybrid.txt',
    )

for m in modules:
        doctest.testmod(m)
for f in files:
        doctest.testfile(f)
