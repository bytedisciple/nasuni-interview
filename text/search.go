package text

import (
	"bufio"
	"bytes"
	"fmt"
	"math"
	"os"
	"strings"
)

var OS_WORD_SIZE = 4.*1024 //4KB is OS word size

// WORD_CHARACTERS_LIST holds array of bytes for the characters a-zA-Z0-9'-
var WORD_CHARACTERS_LIST = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'-")

// TextSearcher structure
// TODO: add in objects/data structures required by the Search function
type TextSearcher struct {
	fileBuffers [][]byte 		// 4KB chunks of file
	orderedMatches [][]string	// Matches as they exist in each chunk
}

// NewSearcher returns a TextSearcher from the given file
// TODO: Load/process the file so that the Search function work
func NewSearcher(filePath string) (*TextSearcher, error) {

	// Credit to https://medium.com/swlh/processing-16gb-file-in-seconds-go-lang-3982c235dfa2
	// for an efficient file reading scheme using buffer readers

	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}

	// UPDATE: close after checking error
	defer file.Close()  //Do not forget to close the file

	fileInfo, err := file.Stat()
	if err!= nil{
		return nil, err
	}

	// Convert to float 64 so we get remainder in following line
	fileSize := float64(fileInfo.Size())
	// Round up to get the last buffer that is partially filled

	OS_WORD_SIZE = fileSize

	numberOfFileChunks := int(math.Ceil(fileSize/OS_WORD_SIZE))
	fileBuffers := make([][]byte, numberOfFileChunks)

	r := bufio.NewReader(file)
	for i := 0; i < numberOfFileChunks; i++ {
		buf := make([]byte,  int(math.Ceil(OS_WORD_SIZE))) //the chunk size
		n, err := r.Read(buf) //loading chunk into buffer
		if err != nil{
			return nil, err
		}
		buf = buf[:n]
		fileBuffers[i] = buf
	}

	return &TextSearcher{
		fileBuffers: fileBuffers,
	}, nil
}

// Search searches the file loaded when NewSearcher was called for the given word and
// returns a list of matches surrounded by the given number of context words
// TODO: Implement this function to pass the tests in search_test.go
func (ts *TextSearcher) Search(word string, context int) []string {

	matches := make([]string, 0)

	for i, buffer := range ts.fileBuffers {

		wordIndex := -1
		skip := 0	//We trim the local scope buffer as we search. Remember offset into full chunk for reading full

		//Find many matches in a given buffer
		for{
			wordIndex = bytes.Index(bytes.ToLower(buffer), []byte(strings.ToLower(word)))
			if wordIndex == -1 {	// Found a match
				break
			} else {				// No match in this chunk

				exactMatch, err := ts.isExactWord(i, word, wordIndex+skip)
				if err != nil {
					break
				}
				if exactMatch == false {
					skip += wordIndex+len(word)
					buffer = buffer[wordIndex+len(word):]
					continue
				}
				prevWords := ts.findPrevWords(i, wordIndex+skip, context)
				nextWords := ts.findNextWords(i, wordIndex+skip, word, context)

				match := fmt.Sprintf("%v%v%v", prevWords, word, nextWords)
				//match := strings.Join(prevWords," ") + word + strings.Join(nextWords, " ")

				matches = append(matches, match)
				skip += wordIndex+len(word)
				buffer = buffer[wordIndex+len(word):]
			}
		}
		//break
	}

	return matches
}

// findNextWords gets the next N words after a given index in a buffer
func (ts TextSearcher) findNextWords(bufferNum int, bufferIndex int, word string, numWords int) string{

	words := make([]string, numWords)

	bufferAfterWord := removeReturnCharacters(ts.fileBuffers[bufferNum][bufferIndex+len(word):])
	wordsInBytes := bytes.Fields(bufferAfterWord)[0:numWords]

	if bytes.Equal(wordsInBytes[0], []byte("\"")){
		return "\"" + ts.findNextWords(bufferNum, bufferIndex + 1, word, numWords)
	}

	for i, wordInBytes := range wordsInBytes {
		words[i] = string(wordInBytes)
	}
	return " " + strings.Join(words, " ")
}

// findPrevWords gets the previous N words after a given index in a buffer
func (ts TextSearcher) findPrevWords(bufferNum int, bufferIndex int, numWords int) string {
	words := make([]string, numWords)

	bufferBeforeWord := removeReturnCharacters(ts.fileBuffers[bufferNum][:bufferIndex])

	bufferAsSplitBytes := bytes.Fields(bufferBeforeWord)
	wordsInBytes := bufferAsSplitBytes[len(bufferAsSplitBytes)-numWords:]



	if bytes.Equal(wordsInBytes[0], []byte("\"")){
		return ts.findPrevWords(bufferNum, bufferIndex-1, numWords) + "\""
	}

	for i, wordInBytes := range wordsInBytes {
		words[i] = string(wordInBytes)
	}

	//fmt.Printf("%v\n", strings.Join(words, " "))

	return strings.Join(words, " ") + " "
}

// isExactWord checks if the match is a substring of a word or a stand-alone word
// throws an error if the outer bound check on the word would result in OutOfBounds error
func (ts TextSearcher) isExactWord(bufferNum int, word string, index int) (bool, error) {
	if index-1 < 0 || index+1 >  int(math.Ceil(OS_WORD_SIZE)){
		return false, fmt.Errorf("out of bounds of OS_WORD_SIZE buffer")
	}

	byteBeforeWord := ts.fileBuffers[bufferNum][index-1]
	byteAfterWord := ts.fileBuffers[bufferNum][index + len(word)]

	//If the byte before or after the word is a word-character a-zA-Z0-9'-
	if bytes.ContainsRune(WORD_CHARACTERS_LIST, rune(byteBeforeWord)) ||
		bytes.ContainsRune(WORD_CHARACTERS_LIST, rune(byteAfterWord)) {
		return false, nil
	}

	return true, nil
}

// adjustIndex adjusted the index of the provided word in the situation where a non-whitespace non-character-word
// prepends the search team.
func (ts TextSearcher) adjustIndex(bufferNum int, word string, index int) (adjustedWord string, indexAdjustment int, err error) {
	adjustedWord = word
	indexAdjustment = 0

	offset := 1

	for{
		if index-offset < 0 {
			return word, index, fmt.Errorf("out of bounds of OS_WORD_SIZE buffer")
		}

		byteBeforeWord := ts.fileBuffers[bufferNum][index-offset]
		if bytes.ContainsRune(WORD_CHARACTERS_LIST, rune(byteBeforeWord)){
			// We have hit a new word
			return
		} else if rune(byteBeforeWord) == ' '{
			return
		} else {
			indexAdjustment--
			adjustedWord = string(byteBeforeWord) + adjustedWord
		}

		offset++
	}
}


func removeReturnCharacters(buf []byte) []byte{
	buf = bytes.ReplaceAll(buf, []byte("\r\n"), []byte(" "))
	buf = bytes.ReplaceAll(buf, []byte("\n"), []byte(" "))
	return buf
}