"""
The MIT License

Copyright (c) Tigran Hakobyan. http://tiggreen.me

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.

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
	files: the files that the Twitter program should run on.
	attr:  attribute of a tweet. E.g. 'lang'
	attr value: the value of the attribute. E.g. 'en'
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
	def reducer(self, key_values_list):
		total_sum = 0
		for entry in key_values_list:
			total_sum +=  sum(entry[1])
		return total_sum

