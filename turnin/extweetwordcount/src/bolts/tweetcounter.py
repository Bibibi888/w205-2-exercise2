from __future__ import absolute_import, print_function, unicode_literals
from collections import Counter
from streamparse.bolt import Bolt
import psycopg2, re, os


class TweetCounter(Bolt):

	def _run_query(self, query):
		if self._cnx is None or self._cnx.closed != 0:
			self.log("Initializing connection")
			
			if self._cnx_password is None:
				cnx_string = "host='{HOST}' dbname='{DATABASE}' user='{USER}'".format(HOST=self._cnx_host, DATABASE=self._cnx_db, USER=self._cnx_user)
			else:
				cnx_string = "host='{HOST}' dbname='{DATABASE}' user='{USER}' password='{PASSWORD}'".format(HOST=self._cnx_host, DATABASE=self._cnx_db, USER=self._cnx_user, PASSWORD=self._cnx_password)
			self._cnx = psycopg2.connect(cnx_string)
			self._cur = self._cnx.cursor()
		self._cur.execute(query)
		self.log("Sent query")
		self._cnx.commit()
		self.log("Committed query")
	

	def _roll_back_query(self):
		self._cnx.rollback()

	# First we check if we can insert. If we get a conflict, we roll back and update instead.
	# Note that psql supports ON CONFLICT statements, but that's not valid in version 8.x, which
	# is what I'm using for development work.
	def _insert_update_word(self, word):
		try:
			# Insert first
			query_template = 'INSERT INTO tweetwordcount (word, word_count) VALUES (\'{WORD}\', 0)'
			query = query_template.format(WORD=word)
			self._run_query(query)
			self.log(query)
		except:
			# Roll back and update with wordcount+1 if insert fails
			self._roll_back_query()
			self._run_query('UPDATE tweetwordcount SET word_count = word_count + 1 WHERE word = \'{WORD}\''.format(WORD=word))
			self.log('UPDATE tweetwordcount SET word_count = word_count + 1 WHERE word = \'{WORD}\''.format(WORD=word))

	def initialize(self, conf, ctx):
		self.log("============================INITIALIED============================")
		self.counts = Counter()
		self._cnx = None # Database connection
		self._cnx_user = os.environ['PSQL_USER']
		self._cnx_host = os.environ['PSQL_HOST']
		self._cnx_db = 'tcount'
		self._cnx_password = None

		# What if the user is using trust auth? Allow that case.
		try: self._cnx_password = os.environ['PSQL_PASSWORD'] if os.environ['PSQL_PASSWORD'] else None
		except: pass

	def process(self, tup):
		try:
			self.log("Tweet bolt, TUP: " + str(tup))
			word = tup.values[0]
			# Increment the local count
			self.counts[word] += 1
			self.emit([word, self.counts[word]])
			# Log the count - just to see the topology running
			self.log('%s: %d' % (word, self.counts[word]))

			# Store word in the database / update the word count
			if word:		
				self._insert_update_word(re.sub("'", '', word))
		except IndexError:
			pass
