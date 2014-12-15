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
Date: 11/29/2014

WordCount program that inherits from the MapReduceInterface framework.
Counts the number of occurances of each word in the files.
"""

from map_reduce import *

"""
WordCount class.
"""
class WordCount(MapReduceInterface):

	"""
	files: the files that the WordCount program should run on.
	mapper: the user defined map function.
	reducer: the user defined reduce function.
	"""
	def __init__(self, files):
		self.files = files
		mapper = self.mapper
		reducer = self.reducer
		MapReduceInterface.__init__(self,  mapper, reducer, files)

	"""
	The map function for WordCount program.
	file_chunk is the same format file as the initial input files.
	Produce a (key, value) pair for each word in the text.
	"""
	def mapper(self, file_chunk):
		text = file_chunk.read()
		results = []
		text = text.split()
		for w in text:
			results.append((w,1))
		return results

	"""
	Gets a list of [('a', 1), ('b', [1, 1, 1, 1]), ...]
	and returns [(a, total_freq), (b, total_freq), ...].
	"""
	def reducer(self, key_values_list):
		result = []
		for entry in key_values_list:
			key = entry[0]
			sum_of_values = sum(entry[1])
			result.append((key,sum_of_values))
		return result
