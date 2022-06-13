# Nasuni Coding Exercise

## Overview

Please complete the implementation for `TextSearcher` in the agreed upon language. The provided starter files contain outlines to produce a TextSearcher that can be used to search the sample data files.

```go
// Instantiate a new TextSearcher from the file
searcher, err := text.NewSearcher("some_file.txt")

// Use the TextSearcher that is returned to search for matches with their context
results :=searcher.Search("fox", 2)
```

```python
searcher = TextSearcher()
searcher.load("some_file.txt")
results = searcher.search("fox", 2)
```

In addition to finding matches the Search function should return the given number of words (2, in the example above) as context around the matched word.

For example given a file containing,
> The quick brown fox jumped over the lazy dog.

running `searcher.Search("fox", 2)` would return.
> quick brown fox jumped over


## Details

To complete this exercise you new to make all of the tests pass in the respective language. You should not alter the tests to pass but you may add addition files and functions you see require to make the tests pass.

### go
You can run the tests via `go test` from the main directory as, `go test ./...`

### python
We have provided a `Pipfile` for usage with `pipenv`, but any recent version of python and pytest should be sufficient for running the tests.

### Files

**Readme.md**
This file, containing instructions on how to complete the exercise.

**go/**
Stub and tests for a go implementation.

**python/**
Stub and tests for a python implementation.

**files/Siddhartha.txt**
A larger text file which is used by some of the tests to validate your solution.

**files/Siddhartha.opening.txt**
A smaller text file which is used by some of the tests to validate your solution.


### Requirements / Assumptions / Etc
* A word consists of any sequence of the characters a-z, A-Z, 0-9, apostrophe, and dash. Therefore, “animal’s” is a single word, as are “1844” and “xxxxx10x”.
* Any non-word character is regarded as punctuation and will be ignored by the search (but included in the results).
* Search should be case-insensitive, so "fox" and "Fox" should return the same set of results.
* You don't have to worry about matching plural words: "river" does not have to find occurrences of "rivers".
* Strings should be returned in document order.
* Strings should be returned exactly as they appear in the document, including original punctuation and capitalization.
* The exception to the previous rule is white space. Strings should be return with extraneous whitespace removed.
* Overlapping hits require no special treatment; each should be returned as a separate hit.
* If no hits are found it should return an empty array.
* It is safe to assume that the contents of the underlying file can be held in memory.
* The assumption is that a single file with be read into the `TextSearcher` once and then the searched multiple times. Your implementation should be most efficient at searching.
