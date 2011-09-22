'''
@author Petr Gladkikh
'''

NUMERALS = "0123456789abcdefghijklmnopqrstuvwxyz"

def baseN(num, b, numerals=NUMERALS):
    """Integer to string with given base.
    http://code.activestate.com/recipes/65212/"""
    return ((num == 0) and numerals[0]) or (baseN(num // b, b, numerals).lstrip(numerals[0]) + numerals[num % b])


# Tests


import unittest
class Test(unittest.TestCase):
    def testBaseN(self):
        for base in range(2, 36):
            for n in range(base * base + 30):
                self.assertEquals(long(baseN(n, base), base), n)


if __name__ == "__main__":
    unittest.main()
    
