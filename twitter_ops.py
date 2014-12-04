"""
Author: Tigran Hakobyan (txh7358@rit.edu)
Version: 1.0
Date: 11/02/2014

Twitter program that inherits from the MapReduce framework.
Takes an attribute name and value and returns the total
number of tweets where attr=attr_value. The input file is in
.json format and is a collection of tweets.
"""

from map_reduce import *

import json

"""
Twitter class.
"""
class Twitter(MapReduceInterface):

	"""
	Files: the files that the Twitter program should run on.
	Attr:  attribute of a tweet. E.g. 'lang'
	Attr: the value of the attribute. E.g. 'en'
	"""
	def __init__(self, files, attr, attr_value):
		self.attr = attr
		self.attr_value = attr_value
		self.files = files
		mapper = self.mapper
		reducer = self.reducer
		MapReduceInterface.__init__(self,  mapper, reducer, files)

	"""
	The map function for twitter program.
	Takes a chunk .json file and returns a list of tweet ids where
	attr=attr_value.
	"""
	def mapper(self, json_file):
		json_data = json_file.read()
		results = []
		json_list = []

		json_list = json.loads(json_data)
		for tweet in json_list:
			if not self.attr in tweet:
				pass
			else:
				if tweet[self.attr] == self.attr_value:
					results.append((tweet['id'], 1))	
		return results

	"""
	The reduce function for twitter program.
	"""
	def reducer(self, mapping):
		total_sum = 0
		for entry in mapping:
			total_sum +=  sum(entry[1])
		return total_sum

