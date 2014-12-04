"""
Author: Tigran Hakobyan (txh7358@rit.edu)
Version: 1.0
Date: 11/02/2014

Numeric operations program that inherits from the MapReduceInterface framework.
Takes a .csv files and a column number and returns the min, max, avg
values of the given column.
"""

from map_reduce import *

import csv

"""
Numeric class.
"""
class Numeric(MapReduceInterface):

	"""
	Files: the files that the Numeric program should run on.
	col_number:  the column number or the field number, start from 1.
	Number of processes: the number of processes given by user.
	"""
	def __init__(self, files, col_number):
		self.col_number = col_number
		self.files = files
		mapper = self.mapper
		reducer = self.reducer
		MapReduceInterface.__init__(self,  mapper, reducer, files)
		
	"""
	The map function for numeric program.
	Takes a chunk csv file and 
	returns the (min, max, avg) values of the file.
	"""
	def mapper(self, csv_chunk):
		lst = self.load(csv_chunk)
		elems = [int(i) for i in lst]
		avg = sum(elems) / len(elems)
		return [(1, (min(elems), max(elems), avg))]

	"""
	The reduce function for numeric program.
	Doesn't do much for now. It just passes the map result
	to the next step so it can be merged.
	"""
	def reducer(self, mapping):
		mins = 0
		maxs  = 0
		avgs = 0
		ln = 1
		for entry in mapping:
			key = entry[0]
			# list of tuples
			tple_list = entry[1]
			ln = len(tple_list)
			for tple in tple_list:
				mins = tple[0]
				maxs = tple[1]
				avgs = tple[2]

		return (mins, maxs, avgs // ln)

	"""
	Load a .csv file and append all the values
	of the user given column. In short, loads the whole
	column into a new list. The column is the column given by user.
	"""
	def load(self, csv_file):
		reader = csv.reader(csv_file)
		all_rows = []
		for row in reader:
			all_rows.append(row[self.col_number-1])
		return all_rows
