import sys
import psycopg2

def run_query(query):
	cnx_string = "host='localhost' dbname='tcount' user='__tcount'"
	cnx = psycopg2.connect(cnx_string)
	cur = cnx.cursor()
	cur.execute(query)
	result = cur.fetchall()
	cur.close()
	cnx.close()
	return result

if len(sys.argv) == 1:
	query = 'SELECT word, word_count FROM tweetwordcount ORDER BY word'
	word_counts = run_query(query)

	word_pairs = []
	
	# It might seem a bit odd to use the method below to produce what are clearly
	# tuples in the w205 EX2 PDF, but if I simple do str(some_tuple), we'll have extra
	# quote marks, which I do not want.
	for wordset in word_counts:
		word_pairs.append("(" + wordset[0] + ", " + str(wordset[1]) + ")")

	print(', '.join(word_pairs))

else:
	search_word = sys.argv[1].lower()

	query = 'SELECT word_count FROM tweetwordcount WHERE word=\'{WORD}\' LIMIT 1'
	word_count = 0

	try:
		word_count = run_query(query.format(WORD=search_word))
	except: pass

	print('Total number of occurrences of of "{WORD}": {COUNT}'.format(WORD=search_word, COUNT=word_count[0][0]))
