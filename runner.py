"""
Author: Tigran Hakobyan (txh7358@rit.edu)
Version: 1.0
Date: 11/29/2014

The runner class for all 3 MR prototypes.
This module is run by a user and based on a file format
the runner call the corresponding program.

.txt --> WordCount
.json --> Twitter
.txt --> Numeric 
"""

import os
import time
import resource

# importing all three modules.
from word_count import *
from twitter import *
from numeric import *

import time


"""
Return the name of the given file.
"""
def get_filename(filepath):
	return ntpath.basename(filepath)

"""
Return the extension of the given file.
"""
def get_file_extension(filename):
	return os.path.splitext(filename)[1][1:]


if __name__ == '__main__':
	
	# Fixing the start time.
	start_time = time.time()

	# input_files = sys.argv[1:-2]
	# attr = sys.argv[-2]
	# attr_value = sys.argv[-1]
	# mr_job = Twitter(input_files, attr, attr_value)

	# input_files = sys.argv[1:]
	# mr_job = WordCount(input_files)

	input_files = sys.argv[1:-1]
	column_number = int(sys.argv[-1])
	mr_job = Numeric(input_files, column_number)

	# # The first input file decides the format of the file.
	# filename = get_filename(sys.argv[1])

	# # Getting the file format.
	# ext = get_file_extension(filename).lower()

	# if ext == "txt":
	# 	input_files = sys.argv[1:]
	# 	mr_job = WordCount(input_files)
	# elif ext == "json":
	# 	input_files = sys.argv[1:-2]
	# 	attr = sys.argv[-2]
	# 	attr_value = sys.argv[-1]
	# 	mr_job = Twitter(input_files, attr, attr_value)
	# elif ext == "csv":
	# 	input_files = sys.argv[1:-1]
	# 	column_number = int(sys.argv[-1])
	# 	mr_job = Numeric(input_files, column_number)
	# else:
	# 	pass

	# run the program.
	mr_job.run_program()


	print("The execution time is {0} ".format(time.time() - start_time))
