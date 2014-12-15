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
	files: the files that the Numeric program should run on.
	col_number:  the column number or the field number, start from 1.
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
	The reduce function for the numeric program.
	"""
	def reducer(self, key_values_list):
		mins = 0
		maxs  = 0
		avgs = 0
		ln = 1
		for entry in key_values_list:
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
