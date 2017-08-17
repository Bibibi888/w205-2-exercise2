from __future__ import absolute_import, print_function, unicode_literals
import re
from streamparse.bolt import Bolt
from nltk.corpus import *
import nltk, re

def ascii_string(s):
	return all(ord(c) < 128 for c in s)

def contains_word(s):
	return re.search('[a-z]+', s) is not None

def not_url_or_encoded(s):
	return (not s.startswith("http")) and (re.match(r'&amp', s) is None) and (s[0].isalpha())

class ParseTweet(Bolt):
	def process(self, tup):

		self.log("Processing tuple " + str(tup))

		# Load the list of stopwords
		try: stopwords_english = set(stopwords.words('english'))
		except LookupError:
			nltk.download("stopwords")
			stopwords_english = set(stopwords.words('english'))		

		tweet = tup.values[0] # extract the tweet

		# Split common contractions for a more accurate word count
		# u -> you, im -> i am, you're -> you are, it's -> it is
		tweet = re.sub("'", '', tweet)
		tweet = re.sub('im', 'i am', tweet)
		tweet = re.sub('youre', 'you are', tweet)
		tweet = re.sub('its', 'it is', tweet)

		# Split the tweet into major chunks, remove invalid chunks
		t_words = tweet.split()
		t_words = filter(lambda x : not_url_or_encoded(x), t_words)

		# Split each chunk into true words
		# e.g. 'is...not' becomes ['is', 'not']
		words = []
		for word in t_words: 
			words += re.findall(r'[\w\'^-]+', word)


		# Change u to you. We do it here and not earlier for
		# a variety of reasons, mostly because 'u' is now
		# isolated
		words = map(lambda x : 'you' if x == 'u' else x, words)

		# Generate a valid word list
		valid_words = []
		for word in words:
			# Check if it's an ascii word first
			if not ascii_string(word): continue	
			# Filter hashtags
			if word.startswith("#"): continue
			# Filter the user mentions
			if word.startswith("@"): continue
			# Filter out retweet tags
			if word.startswith("rt"): continue
			# Filter out the urls
			if word.startswith("http"): continue
			# Removing encoded "s
			if word.startswith('&amp'): continue
			# Strip leading and lagging punctuations
			aword = word.strip("\"?><,'.:;)")
			# Is it possibly a word? Checking only for alpha characters, by the way
			if not contains_word(word): continue
			# Is the word a stopword?
			if aword in stopwords_english: continue
			# We can either replace a mispelled word, if mispelled
			# with the proper guess... but this is a high risk path.
			# We can also check to see if the word is an English word...
			# And toss out the word if it's not! But that's somewhat
			# silly- why not just store all words at that point?
                        # now check if the word contains only ascii
                        if len(aword) > 0 and ascii_string(word):
                                valid_words.append([aword])

		# Log and emit
		self.log('Valid words for %s: %s' % (tweet, str(valid_words)))
		if not valid_words: return
		# Emit all the words
		self.emit_many(valid_words)
		# tuple acknowledgment is handled automatically.
