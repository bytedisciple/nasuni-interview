package main

import (
	"fmt"
	"nasuni/takehome/go-text-searcher/text"
	"os"
)

//
// Sample main program to demostrate how the TextSeatcher is called
//

func main() {
	// Assume the first arg is a file name
	args := os.Args[1:]

	// Instantiate a new TextSearcher with the file
	ts, _ := text.NewSearcher(args[0])

	// Use the TextSearcher to return all the matches with their context
	for i, match := range ts.Search("lacked", 4) {
		fmt.Printf("%d: %s\n", i, match)
	}


}
