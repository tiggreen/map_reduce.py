"""
Author: Tigran Hakobyan (txh7358@rit.edu)
Version: 1.0
Date: 11/30/2014

Multi-processed MapReduce framework for a limited main memory.
Designed to work in a single machine and uses file system to store all
intermediate calcaluation values. Map and Reduce functions must be defined by a user.
Uses Python's multiprocessing Pool library for concurrency support. 

Provides the following functionalities:

- Low main memory consumption. 
- Multi-processed.
- Multiple file support
- Built-in performance configuration 
- Different file format support (txt, csv, json)
- Scalable and easy to use

Usage:
Please see usage section of README file.

TODO:
- Write about that Map and Reduce args format is fixed. User has to follow the conventions.
- Handle all the exceptions. Gently.
- Intermediate step? Shuffling and Sorting?
- Write a cleanup function that cleans all temp files.
"""

import sys
import os
import subprocess
import logging
import ntpath
import pickle
import string
import csv
import json
import multiprocessing
import multiprocessing.pool
from multiprocessing import Pool

from collections import defaultdict

"""
Class MapReduce.
The core class that's used by framework. 
"""

class MapReduce:
	
	"""
	Return the name of the given file.   
	"""
	@classmethod
	def get_filename(cls, f):
		return ntpath.basename(f)

	"""
	Return the file extension of the given file.  
	"""
	@classmethod
	def get_file_extension(cls, filename):
		return os.path.splitext(filename)[1][1:]

	"""
	m: the map function defined by user.
	r: the reduce function defined by user.
	f: the file that the MapReduce object should run on.
	np: the number of processes that should run on a file.
	"""
	def __init__(self, m, r, f, np):
		self.mapper = m
		self.reducer = r
		self.file = f
		self.num_processes = np
		self.file_ext = MapReduce.get_file_extension(self.file)
		# partition the files based on it extension.
		self.partition(self.file_ext)
		# run the master. Mater takes care of the rest.

		self.run_master()

		

	"""
	Open a #part-filename-chunkindex file and 
	run the map() method on the chunk. Writes the 
	result of the map into a temporary file named
	#map-filename-chunkindex. The map result is a
	serialized stream.
	"""
	def apply_map(self, i):

		filename = MapReduce.get_filename(self.file)
		chunk_file = open("#part-%s-%s" % (filename, i))

		logging.info('Started the map phase for ' + filename + ' chunk #' + str(i) + '.') 

		# call the user defined mapper on the chunk.
		map_result = self.mapper(chunk_file)

		# close the chunk file and remove it.
		chunk_file.close()
		os.remove("#part-%s-%s" % (filename, i))

		"""
		Marshal (serialize) the map result and
		write it to a temporary file.
		"""

		fl = open("#map-%s-%s" % (filename, i), "wb+")
		logging.info('Writing the map results for ' + filename + ' chunk #' + str(i) + '.')
		pickle.dump(map_result, fl)
		fl.close()


	"""
	Open a #map-filename-chunkindex file and 
	run the reduce() method on the file. Writes the 
	result of the map into a temporary file named
	#reduce-filename-chunkindex. The reduce result is a
	serialized stream.
	"""
	def apply_reduce(self, i):

		fl = open("#inter-shuffled-%s" % i, "rb")
		logging.info('Started the intermediate phase for #inter-shuffled #' + str(i) + '.') 

		# load the shuffled map results.
		map_shuffled_result = pickle.load(fl)
		fl.close()
		os.remove("#inter-shuffled-%s" % i)

		# call the reducer on the shuffled map result.
		reduce_result = self.reducer(map_shuffled_result)

		fl = open("#reduce-%s" % i, "wb+")
		logging.info('Writing the reduce results for #inter-shuffled #' + str(i) + '.') 
		pickle.dump(reduce_result, fl)
		fl.close()

		# filename = MapReduce.get_filename(self.file)
		# fl = open("#map-%s-%s" % (filename, i), "rb")
		# logging.info('Started the reduce phase for ' + filename + ' chunk #' + str(i) + '.') 

		# # load the map result.
		# map_result = pickle.load(fl)
		# fl.close()
		# os.remove("#map-%s-%s" % (filename, i))

		# # call the reducer on the map result.
		# reduce_result = self.reducer(map_result)

		# fl = open("#reduce-%s-%s" % (filename, i), "wb+")
		# logging.info('Writing the reduce results for ' + filename + ' chunk #' + str(i) + '.') 
		# pickle.dump(reduce_result, fl)
		# fl.close()

	"""
	Apply user specified intermediate step. if any.
	The goal of this step is to do shuffling and sorting
	the values by key before passing it to reducer.
	"""
	def apply_intermediate(self, i):

		filename = MapReduce.get_filename(self.file)
		fl = open("#map-%s-%s" % (filename, i), "rb")

		# load the map result.
		map_result = pickle.load(fl)
		fl.close()
		os.remove("#map-%s-%s" % (filename, i))

		# start the sorting and grouping stage.
		# {'n': [1], 'm': [1], 't': [1, 1, 1]})
		dic = defaultdict(list)
		for key, value in map_result:
			dic[key].append(value)

		fl = open("#inter-%s-%s" % (filename, i), "wb+")
		logging.info('Writing the intermediate results for ' + filename + ' chunk #' + str(i) + '.') 
		pickle.dump(dic, fl)
		fl.close()


	"""
	Partition the given file into multiple chunks. All the partition 
	methods support stream parsing which means that you do not need to
	hold the whole JSON, CSV or TXT representation in textual form in memory. 
	The number of chunks is equal to the number of processes. Based on the
	file extension the splitting will be handled accordingly. 
	For a .txt file the word boundaries are considered.
	By default it assumes that the file format is 'txt'.
	"""
	def partition(self, ext):
		if ext == 'csv':
			self.__partition_csv()
		elif ext == 'json':
			self.__partition_json()
		else:
			self.__partition_text()

	"""
	Partition a txt file into smaller chunks.
	"""
	def __partition_text(self):


		# return the size of the file in bytes.
		file_size = os.path.getsize(self.file)
		filename = MapReduce.get_filename(self.file)

		chunk = file_size // self.num_processes + 1
		logging.info('Partitioning ' + MapReduce.get_filename(self.file) + \
			 ' into ' + str(self.num_processes) + ' chunks.')

		# open the file
		fl = open(self.file)
		# read the whole file
		buff = fl.read()
		# close the file
		fl.close()

		fl = open("#part-%s-%s" % (filename, 0), "w+")
		fl.write(str(0) + "\n")
		# each char is one byte 
		num_bytes = 0
		chunk_num = 1

		for ch in buff:
			fl.write(ch)
			num_bytes += 1
			"""
			Make sure we never split a word into parts so we can gently
			handle this case. If the number of bytes is greater than the size of 
			a chunk then create a new part file. 
			"""
			if (ch in string.whitespace) and (num_bytes > chunk * chunk_num):
				fl.close()
				chunk_num += 1
				fl = open("#part-%s-%s" % (filename, chunk_num-1), "w+")
				fl.write(str(num_bytes) + "\n")

		fl.close()

	
	"""
	Partitions a CSV file into multiple chunks. Performs stream partitioning
	which means the whole csv representation is never loaded into memory.

	delimiter: by default it's ','. 
	temp_output_name: a %s-style template for the numbered temp output files.
	"""
	def __partition_csv(self, delimiter=',',temp_output_name='#part-%s-%s'):

		logging.info('Partitioning ' + MapReduce.get_filename(self.file) + \
			 ' into ' + str(self.num_processes) + ' chunks.')

		filename = MapReduce.get_filename(self.file)

		# counting the number of lines of the csv files.
		num_lines = sum(1 for line in open(self.file))
		row_limit = num_lines // self.num_processes
		reader = csv.reader(open(self.file), delimiter=delimiter)
		# skipping the header
		next(reader)

		fl = open("#part-%s-%s" % (filename, 0), "w+", newline='')
		writer = csv.writer(fl, delimiter=delimiter)

		current_limit = row_limit
		current_part = 1
		for i, row in enumerate(reader):
			if i + 1 > current_limit:
				current_part += 1
				current_limit = row_limit * current_part
				fl = open("#part-%s-%s" % (filename, current_part-1), "w+", newline='')
				writer = csv.writer(fl, delimiter=delimiter)
			writer.writerow(row)

	"""
	Partition a json file into smaller chunks.
	TODO: Think of a better way to achieve this.
	"""
	def __partition_json(self):
		filename = MapReduce.get_filename(self.file)
		logging.info('Partitioning ' + filename + \
			 ' into ' + str(self.num_processes) + ' chunks.')

		# load the json file into a list.
		tweet_list = self.__load(self.file)
		chunks = self.split_list(tweet_list, self.num_processes)
		# the number of chunks is equal to the number of processes.
		j = 0
		for ch in chunks:
			fl = open("#part-%s-%s" % (filename, j), "w+")
			json.dump(ch, fl)
			fl.close()
			j+=1


	"""
	Load a file into a json object list.
	Must be changed. Consider ijson external module
	to do a better json parsing.
	"""
	def __load(self, json_file):
		tweet_list = []
		for tweet in open(json_file):
			tweet_list.append(json.loads(tweet))
		return tweet_list

	"""
	Split a list into n chunks.
	"""
	def split_list(self, lst, n):
		start = 0
		for i in range(n):
		    stop = start + len(lst[i::n])
		    yield lst[start:stop]
		    start = stop


	"""
	Plays the role of the master node in our framework.
	Creates a pool of worker processes and assigns mappers and 
	reducers for each chunk of the data. The number of created chunks 
	is equal to #number-of-processes. The total number of processes in the 
	program will be #number-of-processes * #number-of-processes.
	For each chunk there is one reducer.
	"""
	def run_master(self):

		logging.info('Creating a pool of ' + str(self.num_processes) + ' subprocess workers.')
		# create a pool of processes.
		pool = Pool(processes=self.num_processes,)

		# apply map on the chunks in parallel.
		regions = pool.map(self.apply_map, range(0, self.num_processes))

		# do the intermediate step on each chunks in parallel.
		inters = pool.map(self.apply_intermediate, range(0, self.num_processes))

		# must be an intermediate step

		# reduce the map results in parallel.
		#reduced_parts = pool.map(self.apply_reduce, range(0, self.num_processes))

		"""
		Call user defined post processing to get the final file?
		"""


"""
Class MapReduceInterface.

The main class interface that directly talks to the user define class.

This class takes user input files, mapper and reducer 
functions and creates MapReduce job on each single file. 
Please note that MapReduce class object takes only one file 
as a field. All under the hood operations are hidden from the 
user and handled by the framework itself. In the beginning, 
the class creates a pool of processes such that one process is
assigned to one file. Then, for each file one MapReduce object is 
created where the number of processes is equal to the total number of files. 
All MapReduce jobs for different files are run in parallel. 
"""
class MapReduceInterface(MapReduce):

	"""
	The framework supports these 3 file formats yet. 
	"""
	supported_file_types = ('txt', 'json', 'csv')


	"""
	mapper: the map function defined by user.
	reducer: the reduce function defined by user.
	files: the files that the framework should run on.
	"""
	def __init__(self, mapper, reducer, files):

		logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', 
			filename='map_reduce.log',level=logging.INFO)

		mapper = self.mapper
		reducer = self.reducer
		self.__map_and_reduce_are_fine()

		self.files = files
		self.num_processes = self.__set_num_processes()

		# make sure that the user input is valid.
		self.__files_not_empty()
		self.__the_same_format_files()
		self.__is_file_format_supported()
		self.__check_file_names()

		logging.info('All user input validations are passed. We are set to go!')

	"""
	Check that at least one file is given as input.
	"""
	def __files_not_empty(self):
		if self.files == []:
			self.__cleanup()
			raise MapReduceError("At least one file must be given as input.")

	"""
	Map and Reduce fields must be functions.
	"""
	def __map_and_reduce_are_fine(self):
		if not (hasattr(self.mapper, '__call__') and hasattr(self.reducer, '__call__')):
			self.__cleanup()
			raise MapReduceError("Mapper and Reducer must be functions.")


	"""
	All input file extensions must be supported by the framework.
	"""
	def __is_file_format_supported(self):
		for f in self.files:
			ext = MapReduce.get_file_extension(f)
			if ext not in MapReduceInterface.supported_file_types:
				self.__cleanup()
				raise MapReduceError("Currently framework supports txt, json and csv files only.")

	"""
	All input files must have the same file extension.. 
	"""
	def __the_same_format_files(self):
		file_exts = []
		file_exts.append(MapReduce.get_file_extension(self.files[0]))
		if len(self.files) > 1:
			for i in range(1, len(self.files)):
				ext = MapReduce.get_file_extension(self.files[i])
				if ext not in file_exts:
					self.__cleanup()
					raise MapReduceError("All input files must have the same file extension.")
		

	"""
	We can't have two files with the same name as a framework input.
	Check and make sure that the input is valid.
	"""
	def __check_file_names(self):
		all_file_names = {}
		for f in self.files:
			key = MapReduce.get_filename(f)
			if key not in all_file_names:
				all_file_names[key] = True
			else:
				"""
				If we have a duplicate filename then
				raise an exceotion and exit the program.
				"""
				self.__cleanup()
				raise MapReduceError("All files must have unique names.")

	"""
	Based on the number of files this method sets 
	the number of processes used in the framework.
	If the number of files is less than 3 then we
	assign num_processes = 3 (the best threshold we found). 
	"""
	def __set_num_processes(self):
		if len(self.files) < 3:
			return 3
		return len(self.files)

	"""
	Clean all temporary files created during the program
	execution if any.
	"""
	def __cleanup(self):
		# all temp files start with #.
		subprocess.call(["/bin/bash", "rm \#*", "shell=False"])

	"""
	Finalize the program execution. E.g. print outputs,
	generate log files, etc..
	"""
	def __finalize_program(self):
		logging.info('Finalizing the program execution. Cleaning up.')


	"""
	The main method that runs the whole framwork (program).
	Must be run by user.
	"""
	def run_program(self):

		logging.info('Running the framework...')

		"""
		Create a pool of processes. The number of processes
		is equal to the number of files. One process takes care of one file.
		"""
		pool = MyMRPool(len(self.files))

		logging.info('The initial number of running processes is ' + str(len(self.files)) + '.')


		"""
		Apply call_map_reduce on all files in parallel. All files 
		will be partitioned/mapped/shuffled individually.
		"""
		apply_map_reduces = pool.map(self.call_map_reduce, self.files)


		self.shuffle()

		"""
		At this point we have bunch of inter-shuffled files.
		We can reduce them in parallel.	
		"""
		reduces = pool.map(self.apply_reduce, range(0, self.num_processes))

		"""
		At this point we have bunch of reduce files so we can 
		merge all reduce files into one final file.	
		"""
		self.merge_reduce_results()

		self.__finalize_program()

		logging.info('The program is successfully finished.')


	"""
	Explain
	"""
	def shuffle(self):
		dic = defaultdict(list)
		for filepath in self.files:
			filename = MapReduce.get_filename(filepath)
			for i in range(0, self.num_processes):
				fl = open("#inter-%s-%s" % (filename, i), "rb")
				chunk_dic = pickle.load(fl)
				for key in chunk_dic.keys():
					if key in dic:
						dic[key] += chunk_dic[key]
					else:
						dic[key] = list(chunk_dic[key])
				fl.close()
				os.remove("#inter-%s-%s" % (filename, i))

		to_reduce_chunks = self.split_list(list(dic.items()), self.num_processes)
		# the number of chunks is equal to the number of processes.
		j = 0
		for ch in to_reduce_chunks:
			fl = open("#inter-shuffled-%s" % j, "wb+")
			pickle.dump(ch, fl)
			fl.close()
			j+=1

	"""
	Explain
	"""
	def call_map_reduce(self, input_file):
		logging.info('Creating a map_reduce job for ' + MapReduce.get_filename(input_file) \
		 + ' file. Mapper and reducer are given by user.')
		MapReduce.__init__(self,  self.mapper, self.reducer, input_file, self.num_processes)



	"""
	Merge all reduce outputs into one final file.
	"""
	def merge_reduce_results(self):

		logging.info('Merging all reduce results into a final file.') 

		# This function must be changed to work for all file types.
		final_output = open("map_reduce_output.txt", "w+")
		for i in range(0, self.num_processes):
			fl = open("#reduce-%s" % i, "rb")
			temp_data = pickle.load(fl)
			final_output.write(str(temp_data)+"\n")
			fl.close()
			os.remove("#reduce-%s" % i)

		logging.info('Merging is done. Generating the output map_reduce_output.txt file.') 


"""
Class NoDaemonProcess.

The multiprocessing Pool class creates daemonic processes that are not allowed 
to have children which means that one process can't create sub-processes
(worker processes) which we need to do in our MapReduce. 
The solution is to create a class that inherits from multiprocessing.Processes 
and override the _get_daemon method to return False.

Otherwide the exception below is thrown:
AssertionError: daemonic processes are not allowed to have children
"""
class NoDaemonProcess(multiprocessing.Process):

	# make 'daemon' attribute always return False
	def _get_daemon(self):
		return False

	def _set_daemon(self, value):
		pass

	daemon = property(_get_daemon, _set_daemon)


"""
Class MyMRPool.

MyMRPool is a class that can create a non-daemonic pool of processes. 
We sub-class multiprocessing.pool.Pool instead of multiprocessing.Pool
because the latter is only a wrapper function, not a proper class.
"""
class MyMRPool(multiprocessing.pool.Pool):
	Process = NoDaemonProcess


"""
Class MapReduceError.

Create our own exception class.
"""
class MapReduceError(Exception):

    def __init__(self, message, errors={}):
    	# call the base class constructor
        super(MapReduceError, self).__init__(message)

        """
        This way we can pass dict of error messages to the 
        second parameter and access it later wit e.errors. 
        """
        self.errors = errors



