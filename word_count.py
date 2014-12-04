"""
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
	num_processes: the number of processes given by user.
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
	def reducer(self, map_result_list):
		result = []
		for entry in map_result_list:
			key = entry[0]
			sum_of_values = sum(entry[1])
			result.append((key,sum_of_values))
		return result
