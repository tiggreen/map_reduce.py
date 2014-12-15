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

The runner class for all 3 MR prototypes.
This module is run by a user and based on a file format it
calls the right program.

JUST IMPLEMENTED FOR TESTING PURPOSES. 

.txt --> WordCount
.json --> Twitter
.txt --> Numeric 
"""

import os
import time
import resource

# importing all three modules.
from word_count import *
from twitter_ops import *
from numeric_ops import *

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

	if (len(sys.argv) < 2):
		print("Please take a look at the usage of the framework.")
		exit(2)
	
	# Fixing the start time.
	start_time = time.time()

	# The first input file decides the format of the file.
	filename = get_filename(sys.argv[1])

	# Getting the file format.
	ext = get_file_extension(filename).lower()

	if ext == "txt":
		input_files = sys.argv[1:]
		mr_job = WordCount(input_files)
	elif ext == "json":
		input_files = sys.argv[1:-2]
		attr = sys.argv[-2]
		attr_value = sys.argv[-1]
		mr_job = Twitter(input_files, attr, attr_value)
	elif ext == "csv":
		input_files = sys.argv[1:-1]
		column_number = int(sys.argv[-1])
		mr_job = Numeric(input_files, column_number)
	else:
		pass

	# run the program.
	mr_job.run_program()


	print("The execution time is {0} ".format(time.time() - start_time))
