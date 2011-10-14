'''
Combines dictionary with in-memory SQLite3 database.
Thu Oct 13 16:36:11 EDT 2011
'''

from SqlRoot import SqlRoot

class Hybrid(dict):
	'''
	Usage example:

		psys = PSys(Log(), lambda : Hybrid())

	'''

	def __init__(self):
		dict.__init__(self)
		self.sql = SqlRoot()

	# Convenience.
	@property
	def dbconn(self):
		return self.sql.dbconn
