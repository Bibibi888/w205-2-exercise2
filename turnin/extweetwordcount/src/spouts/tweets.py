# -*- coding: utf-8 -*-

from __future__ import absolute_import, print_function, unicode_literals
import itertools, tweepy
from streamparse.spout import Spout
import json, time, os
from Queue import Queue
from requests.packages.urllib3.exceptions import ProtocolError
import threading


'''
Our twitter listener

Sets a spout context for itself. It'll pass new tweets to the spout context
upon each new tweet.

Please note that it needs to clean up the tweet a bit to prevent encoding-related
errors.
'''
class StormStreamListener(tweepy.StreamListener):
	def on_status(self, status):
		self._spout_context.add_item(''.join(i for i in status.text if ord(i) < 128).encode('ascii').lower())	
		return True

	def on_disconnect(self, notice):
		self._spout_context.log("Disconnect: " + str(notice))
		return False

	def __init__(self, context):
		super(self.__class__, self).__init__()
		self._spout_context = context
		

class Tweets(Spout):

	'''
	Twitter-related code below
	'''

	def _get_auth(self):
		consumer_key = os.environ['TWITTER_CONSUMER_KEY']
		consumer_secret = os.environ['TWITTER_CONSUMER_SECRET']
		access_token = os.environ['TWITTER_ACCESS_TOKEN']
		access_token_secret = os.environ['TWITTER_ACCESS_TOKEN_SECRET'] 
	
		auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
		auth.set_access_token(access_token, access_token_secret)
	
		return auth

	# Is our stream still active?
	def _update_stream_status(self):
		try:
			return self._stream.running
		except: 
			return False

	# Check if the stream needs to be restarted. If it does, restart.
	def _try_start_stream(self):
		stream_running = self._update_stream_status()
		if not stream_running:
			try:
				self._stream = tweepy.Stream(auth = self._get_auth(), listener=StormStreamListener(self))	
				self._stream.filter(languages=["en"], stall_warnings=True, track=["a", "the", "i", "you", "u"], async=True)
			except ProtocolError: pass	
		
	# Allows the listener to safely push data to our spout object
	def add_item(self, item):
		# Note: There's a bug which causes crashes when setting a queue's maxsize
		# Due to this bug, we opt to manually check the approximate queue size
		# instead of allowing the queue to handle itself.
		if self._queue.qsize() > 10000:
			self.log("Queue full, tossing tweet")
		else:
			self.log("Added: " + item.encode('utf-8'))
			self._queue.put(item, block=False)

	'''
	Below is strictly spout code
	'''

	# Init a queue object, the stream object
	def initialize(self, stormconf, context):		
		self._queue = Queue()
		self._try_start_stream()

	def next_tuple(self):
		self._try_start_stream() #Check if our twitter stream is still valid. If not, restart stream.
		sentence = ''
		
		# Get the next tweet. We do not handle blank tweets in the spout- that's in a bolt!
		try: sentence = self._queue.get(block=False)
		except: pass

		self.log("Sending: " + sentence)
		self.emit([sentence])

	def ack(self, tup_id):
		pass

	def fail(self, tup_id):
		pass
