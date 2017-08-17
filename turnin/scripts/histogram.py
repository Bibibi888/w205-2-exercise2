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

try:
	start = sys.argv[1].split(',')[0]
	end = sys.argv[1].split(',')[1]

        query = 'SELECT word, word_count FROM tweetwordcount WHERE word_count BETWEEN {START} AND {END} ORDER BY word_count DESC'
        word_counts = run_query(query.format(START=start, END=end))

        for wordset in word_counts:
                print(wordset[0] + ": " + str(wordset[1]))
except:
	print("Invalid parameters provided: " + str(sys.argv[1:]))
