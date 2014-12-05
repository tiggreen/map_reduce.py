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
4. Built-in performance configuration
5. Different file format support (txt, csv, json) 
6. Scalable and easy to use 
					
Users can easily create MapReduce jobs using this framework by only defining map and reduce functions. All the intermediate steps are handled by the framework.

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
Get the zip of the project from [here](http://tiggreen.github.io/map_reduce.py/zipball/master).

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

	
**Step 4:** <br>

The last step is to create an object of your class and run ```python run_program()``` method. 

**Step 5:** <br>

Now relax! The framework will take care of the rest and the output will be generated to
```map_reduce_output.txt``` file.



The below example shows how one can create a MapReduce job using our framework.
This class finds the number of occurances of each word in all files. 

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

Depending on the size of the project, if it is small and simple enough the reference docs can be added to the README. For medium size to larger projects it is important to at least provide a link to where the API reference docs live.

Small API for classes and functions?

## Tests

Describe and show how to run the tests with code examples.

## Contributors

Let people know how they can dive into the project, include important links to things like issue trackers, irc, twitter accounts if applicable.

## License

A short snippet describing the license (MIT, Apache, etc.)
