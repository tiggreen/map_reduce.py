map_reduce.py
=======================

## Synopsis

Multi-processed MapReduce framework for a limited main memory. Runs in
a single machine and uses the file system to store all intermediate calculations. 
Based on Python’s multiprocessing Pool module (Python 3.4).
Framework provides the following functionalities:
		
1. Low main memory consumption
2. Multi-processed
3. Multiple file support
4. Built-in performance configuration.
5. Different file format support (txt, csv, json) 
6. Scalable and easy to use 
					
Users can easily create MapReduce jobs using this framework by only defining map and reduce functions. All the intermediate steps are handled by the framework. Number of running processes in the framework is based on the number of input files. If the number of files is less than three, then the framework assigns 3 processes for each file by default. The number of partitioned
chunk files for each file is equal to the number of files. 

<ul>
<li>Purely implemented in Python.</li>
<li>All the partition methods support stream parsing which means that the framework doesn’t.
need to hold the whole TXT, JSON or CSV representation in textual form in memory.</li>
<li>Uses Python’s multiprocessing Pool module for the best concurrency support.</li>
<li>All the intermediate results are stored in physical files avoiding the main memory
consumption.</li>
<li>Uses Python’s pickle module for serializing and de-serializing Python objects to a byte
stream.</li>
</ul>

## Motivation

There are many MapReduce implementations in different programming languages. One of the most popular implementation is the Hadoop which hides all the details of concurrent programming from the users making it super easy to create and run MapReduce jobs. Even though there are many available MR frameworks nowadays, only few of them are focused on running MapReduce jobs in a limited memory space. Most existing MR frameworks are designed to run on large number of clusters powered with very high CPUs providing a great performance on huge datasets.<br>
**The goal and the main objective of this project is to propose and implement a MapReduce framework that can work in a single machine with a limited main memory**. 


## Installation

**Step 1:** <br>
Get the zip of the project from [here](https://github.com/tiggreen/map_reduce.py/zipball/master).

**Step 2:** <br>
Once you have the source files you can start creating and running your MapReduce jobs. It's super simple. 

Import the framework module.<br>

```python 
from map_reduce import *
````

 Create a new class that defines your **mapper** and **reducer** functions.

> Mapper function must take a file and return a list of ```(key, value)``` pairs. Each ```(key, value)``` must be a tuple.

> Reducer takes a list of ```(key, [values])``` pairs. All values are already grouped by key in the framework. Reducer returns a list of ```(key, value)``` pairs.

**Step 3:** <br>
Once you created your class, make it to extend **MapReduceInterface** class.

```python
class YourMRClass(MapReduceInterface):
	def __init__(self, files):
		self.files = files
		mapper = self.mapper
		reducer = self.reducer
		MapReduceInterface.__init__(self,  mapper, reducer, files)
```
	
**Step 4:** <br>

Create an object of your class and run ```run_program()``` method on it.

```python
your_mr_class_obj = YourMRClass(input_files)
your_mr_class_obj.run_program()
```

**Step 5:** <br>

Now relax! The framework will take care of the rest and the output will be generated to
```map_reduce_output.txt``` file.



The example below is a full implementation of a class the uses the framework
to run a MapReduce job. This class finds the number of occurances of each word in all files. 

```python

# You have to import the framework first.
from map_reduce import *

# Define your class that inherits from MapReduceInterface.
# This means that your class object is a MapReduce job.
class WordCount(MapReduceInterface):

	def __init__(self, files):
		self.files = files
		mapper = self.mapper
		reducer = self.reducer
		# Calling the super class constructor.
		MapReduceInterface.__init__(self,  mapper, reducer, files)

	"""
	Gets a file chunk. 
	Returns [(key, value)].
	"""
	def mapper(self, file_chunk):
		text = file_chunk.read()
		results = []
		text = text.split()
		for w in text:
			results.append((w,1))
		return results


	"""
	Gets [(word1, [counts]),  (word2, [counts2]), ...). 
	Returns [(word1, total_freq), (b, total_freq), ...].
	"""
	def reducer(self, key_values_list):
		result = []
		for entry in key_values_list:
			key = entry[0]
			sum_of_values = sum(entry[1])
			result.append((key,sum_of_values))
		return result

```

All the framework execution logs are written to ```map_reduce.log``` file.

## API Reference

### ```MapReduceInterface``` class extends MapReduce.

| Instance Variables   | Methods                   |
| ---------------------| --------------------------|
| mapper               | __files_not_empty         |
| reducer         	   | __map_and_reduce_are_fine |
| files                | __is_file_format_supported|
| num_processes        | __the_same_format_files   |
|         			   | __check_file_names        |
|                      | __set_num_processes       |
|         			   | __cleanup                 |
|                      | __finalize_program        |
|         			   | run_program               |
|                      | shuffle                   |
|         			   | run_program               |
|                      | call_map_reduce           |
|         			   | merge_reduce_results      |

### ```MapReduce``` class

| Instance Variables   | Methods           |
| ---------------------| ------------------|
| mapper               | get_filename      |
| reducer         	   | get_file_extension|
| file                 | apply_map         |
| num_processes        | apply_reduce      |
| file_ext        	   | apply_intermediate|
|                      | partition         |
|         			   | __partition_text  |
|                      | __partition_csv   |
|         			   | __partition_json  |
|                      | __load            |
|         			   | split_list        |
|                      | run_master        |


### ```NoDaemonProcess``` class extends multiprocessing.Process. 

| Instance Variables   | Methods           |
| ---------------------| ------------------|
|          			   | _get_daemon       |
|          	           | _set_daemon       |


### ```MyMRPool``` class extends multiprocessing.pool.Pool.

```python
"""
Class MyMRPool.

MyMRPool is a class that can create a non-daemonic pool of processes. 
We sub-class multiprocessing.pool.Pool instead of multiprocessing.Pool
because the latter is only a wrapper function, not a proper class.
"""
class MyMRPool(multiprocessing.pool.Pool):
	Process = NoDaemonProcess
```

### ```MapReduceError``` class extends Exception. 

| Instance Variables   | Methods           |
| ---------------------| ------------------|
| message       	   | __init__          |
| errors         	   |                   |

- 

## Tests

TODO:
Write unit tests for the framework.

## Contributors

Tigran Hakobyan
https://twitter.com/tiggreen

## License

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