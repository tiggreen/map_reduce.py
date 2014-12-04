MapReduceMultiprocessed
=======================

## Synopsis

At the top of the file there should be a short introduction and/ or overview that explains **what** the project is. This description should match descriptions added for package managers (Gemspec, package.json, etc.)

Multi-processed MapReduce simulator for a limited main memory.
Uses physical files to store intermediate map, reduce calculations. 
Based on Python’s multiprocessing Pool module (Python 3.4).
Framework that provides the following functionalities:
		
1. Low main memory consumption
2. Multi-processed
3. Multiple file support
4. Built-in performance configuration
5. Different file format support (txt, csv, json) 
6. Scalable and easy to use 
					
Users should be able to easily create MapReduce jobs using our framework by only defining map and reduce functions. All the intermediate steps are handled by our framework. The results are stored in the file system of the running machine in this way consuming less main memory (RAM).

## Code Example

<ul>
<li>Purely implemented in Python.</li>
• All the partition methods support stream parsing which means that the framework doesn’t
need to hold the whole TXT, JSON or CSV representation in textual form in memory.
• Uses Python’s multiprocessing Pool module for the best concurrency support.
• All the intermediate results are stored in physical files avoiding the main memory
consumption.
• Uses Python’s pickle module for serializing and de-serializing Python objects to a byte
stream.
<ul>

## Motivation

A short description of the motivation behind the creation and maintenance of the project. This should explain **why** the project exists.

## Installation

Provide code examples and explanations of how to get the project.

## API Reference

Depending on the size of the project, if it is small and simple enough the reference docs can be added to the README. For medium size to larger projects it is important to at least provide a link to where the API reference docs live.

## Tests

Describe and show how to run the tests with code examples.

## Contributors

Let people know how they can dive into the project, include important links to things like issue trackers, irc, twitter accounts if applicable.

## License

A short snippet describing the license (MIT, Apache, etc.)
