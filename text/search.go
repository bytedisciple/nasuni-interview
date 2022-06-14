package text

import (
	"bufio"
	"bytes"
	"fmt"
	"math"
	"os"
	"strings"
	"sync"
)

var chunkSize = 4*1024 //4KB is OS word size

// wordCharactersList holds array of bytes for the characters a-zA-Z0-9'-
var wordCharactersList = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'-")
var punctuationList = []byte("\".,!?(){}[]") //Potentially incomplete list

// TextSearcher structure
// This implementation reads the chunk in 4kb chunks and does the search in parallel.
// It creates a 'fencepost chunk' in between all 4kb chunks of a variable size depending on context size.
// The fencepost chunks solve the situation where a match is found across two separate chunks.
type TextSearcher struct {
	maxWordSizeInBytes int
	fileBuffers [][]byte 		// 4KB chunks of file
}

// NewSearcher returns a TextSearcher from the given file
func NewSearcher(filePath string) (*TextSearcher, error) {

	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}

	defer file.Close()  //Don't forget to close the file

	fileInfo, err := file.Stat()
	if err!= nil{
		return nil, err
	}

	// Convert to float 64 so we get remainder in following line
	fileSize := float64(fileInfo.Size())

	// Round up to get the last buffer that is partially filled
	numberOfFileChunks := int(math.Ceil(fileSize/ float64(chunkSize)))
	fileBuffers := make([][]byte, numberOfFileChunks)

	r := bufio.NewReader(file)
	for i := 0; i < numberOfFileChunks; i++ {
		buf := make([]byte,  int(math.Ceil(float64(chunkSize))))
		n, err := r.Read(buf)
		if err != nil{
			return nil, err
		}
		buf = buf[:n]
		fileBuffers[i] = buf
	}

	fileBuffersWithFencePosts := make([][]byte, (numberOfFileChunks*2)-1)

	for i, _ := range fileBuffersWithFencePosts {
		if i % 2 == 0{
			fileBuffersWithFencePosts[i] = fileBuffers[(i/2)]
		}
	}

	return &TextSearcher{
		fileBuffers: fileBuffersWithFencePosts,
		maxWordSizeInBytes: 20,
	}, nil
}

// Search searches the file loaded when NewSearcher was called for the given word and
// returns a list of matches surrounded by the given number of context words
func (ts *TextSearcher) Search(word string, context int) []string {

	// Intelligently select the size of the fencepost chunk
	// Assuming a generous 20 characters per word average, multipied by `context`
	// 	should give you at least `context` words on either side
	sizeEitherSideFencepost := context * ts.maxWordSizeInBytes

	// Actually put the fencepost's data in place. 20*context bytes from the end of one chunk and
	//	20*context bytes from the beginning of the next.
	for i := range ts.fileBuffers {
		if i % 2 != 0 {
			firstHalfPost := ts.fileBuffers[i-1]
			secondHalfPost := ts.fileBuffers[i+1]
			ts.fileBuffers[i] = append(
				firstHalfPost[len(firstHalfPost)-sizeEitherSideFencepost:],
				secondHalfPost[0:sizeEitherSideFencepost]...)
		}
	}

	var wg sync.WaitGroup

	matches := make([][]string, len(ts.fileBuffers))

	for fileBufferIndex, _ := range ts.fileBuffers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			buffer := ts.fileBuffers[i]
			wordIndex := -1
			skip := 0	//We trim the local scope buffer as we search. Remember offset into full chunk for reading full

			//Find many matches in a given buffer
			for{
				// Convert
				wordIndex = bytes.Index(bytes.ToLower(buffer), []byte(strings.ToLower(word)))
				if wordIndex == -1 {
					break
				} else {

					// Check if the match is just a substring or an exact match
					exactMatch, err := ts.isExactWord(i, word, wordIndex+skip)
					if err != nil {
						break
					}
					// Move the cursor forward even, if it wasnt an exact match
					if exactMatch == false {
						skip += wordIndex+len(word)
						buffer = buffer[wordIndex+len(word):]
						continue
					}

					// Default context strings to empty string in case context=0
					prevWords := ""
					nextWords := ""
					if context > 0 {
						prevWords = ts.findPrevWords(i, wordIndex+skip, context)
						nextWords = ts.findNextWords(i, wordIndex+skip + len(word), context)
					}

					match := fmt.Sprintf("%v%v%v",
						prevWords,
						//Regrab the search term from the buffer to restore capitalization
						string(ts.fileBuffers[i][wordIndex+skip:wordIndex+skip+len(word)]),
						nextWords)

					matches[i] = append(matches[i], match)

					// Move the 'cursor' forward. If not done bytes.Index will find the same match
					skip += wordIndex+len(word)
					buffer = buffer[wordIndex+len(word):]
				}
			}
		}(fileBufferIndex)
	}

	wg.Wait()

	// @TODO potentially duplicates from fencepost chunks
	flatMatches := make([]string, 0)
	for _, match := range matches {
		flatMatches = append(flatMatches, match...)
	}
	return flatMatches
}

// findNextWords gets the next N words after a given index in a buffer
func (ts TextSearcher) findNextWords(bufferNum int, bufferIndex int, numWords int) string{
	return ts.findContext(
		removeReturnCharacters(ts.fileBuffers[bufferNum][bufferIndex:]),
		false, bufferNum, bufferIndex, numWords)
}

// findPrevWords gets the previous N words before a given index in a buffer
func (ts TextSearcher) findPrevWords(bufferNum int, bufferIndex int, numWords int) string {
	return ts.findContext(
		removeReturnCharacters(ts.fileBuffers[bufferNum][:bufferIndex]),
		true, bufferNum, bufferIndex, numWords)
}

// findContext gets N words from the left or right of our search term.
func (ts TextSearcher) findContext(bufferAsideWord []byte, prev bool, bufferNum int, bufferIndex int, numWords int) string  {
	bufferAsSplitBytes := bytes.Fields(bufferAsideWord) //Like split(" ") but handled many spaces in a row
	numWords = minInt(len(bufferAsSplitBytes), numWords)

	var wordsInBytes [][]byte

	if prev {
		wordsInBytes = bufferAsSplitBytes[len(bufferAsSplitBytes)-numWords:]
	} else {
		wordsInBytes = bufferAsSplitBytes[0:numWords]
	}

	// Nothing left to read in the buffer. We are at the end or beginning of the file.
	if len(wordsInBytes) == 0 {
		return ""
	}

	// Our search term may be padded with some punctuation. If unhandled, it will be treated as a word.
	//	recurse, but move the pointer and trim the buffer of the punctuation.
	for _, b := range punctuationList {
		if bytes.Equal(wordsInBytes[0], []byte{b}){
			if prev{
				return ts.findContext(removeReturnCharacters(ts.fileBuffers[bufferNum][:bufferIndex-1]), prev, bufferNum, bufferIndex-1, numWords) + string(b)
			} else {
				return string(b) + ts.findContext(removeReturnCharacters(ts.fileBuffers[bufferNum][bufferIndex+1:]), prev, bufferNum, bufferIndex+1, numWords)
			}
		}
	}

	// Convert to []string to later strings.Join
	words := make([]string, numWords)
	for i, wordInBytes := range wordsInBytes {
		words[i] = string(wordInBytes)
	}

	if prev {
		return strings.Join(words, " ") + " "
	} else {
		return " " + strings.Join(words, " ")
	}
}

// isExactWord checks if the match is a substring of a word or a stand-alone word
// throws an error if the outer bound check on the word would result in OutOfBounds error
func (ts TextSearcher) isExactWord(bufferNum int, word string, index int) (bool, error) {

	// If the match starts at the beginning of the file, only check to the right of the word
	if bufferNum == 0 && index == 0 {
		if bytes.ContainsRune(wordCharactersList, rune(ts.fileBuffers[bufferNum][len(word)])){
			return false, nil
		} else {
			return true, nil
		}
	} else
	// If the match is contains the last char in the file, only check to the left of the word
	if bufferNum == len(ts.fileBuffers) && index+len(word) == int(chunkSize) {
		if bytes.ContainsRune(wordCharactersList, rune(ts.fileBuffers[bufferNum][len(word)])){
			return false, nil
		} else {
			return true, nil
		}
	}

	// We will throw an error if we are in middle chunks and can't read to the left or right.
	//	Fenceposts will handle this.
	if index-1 < 0 || index+1 >  int(math.Ceil(float64(chunkSize))){
		return false, fmt.Errorf("out of bounds of OS_WORD_SIZE buffer")
	}

	byteBeforeWord := ts.fileBuffers[bufferNum][index-1]
	byteAfterWord := ts.fileBuffers[bufferNum][index + len(word)]

	//If the byte before or after the word is a word-character a-zA-Z0-9'-
	if bytes.ContainsRune(wordCharactersList, rune(byteBeforeWord)) ||
		bytes.ContainsRune(wordCharactersList, rune(byteAfterWord)) {
		return false, nil
	}

	return true, nil
}

// removeReturnCharacters removes new line characters and carriage returns which cause lots of chaos
func removeReturnCharacters(buf []byte) []byte{
	buf = bytes.ReplaceAll(buf, []byte("\r\n"), []byte(" "))
	buf = bytes.ReplaceAll(buf, []byte("\n"), []byte(" "))
	return buf
}

//minInt is a simple helper to compare ints since math package only has Min function for floats
func minInt(x,y int) int{
	if x < y {
		return x
	} else {
		return y
	}
}