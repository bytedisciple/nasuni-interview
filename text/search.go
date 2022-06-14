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

var chunkSize = 4.*1024 //4KB is OS word size

// WordCharactersList holds array of bytes for the characters a-zA-Z0-9'-
var WordCharactersList = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'-")

// TextSearcher structure
// TODO: add in objects/data structures required by the Search function
type TextSearcher struct {
	maxWordSizeInBytes int
	fileBuffers [][]byte 		// 4KB chunks of file
}

// NewSearcher returns a TextSearcher from the given file
// TODO: Load/process the file so that the Search function work
func NewSearcher(filePath string) (*TextSearcher, error) {

	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}

	defer file.Close()  //Do not forget to close the file

	fileInfo, err := file.Stat()
	if err!= nil{
		return nil, err
	}

	// Convert to float 64 so we get remainder in following line
	fileSize := float64(fileInfo.Size())

	//chunkSize = fileSize

	// Round up to get the last buffer that is partially filled
	numberOfFileChunks := int(math.Ceil(fileSize/ chunkSize))
	fileBuffers := make([][]byte, numberOfFileChunks)

	r := bufio.NewReader(file)
	for i := 0; i < numberOfFileChunks; i++ {
		buf := make([]byte,  int(math.Ceil(chunkSize)))
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

	sizeEitherSideFencepost := context * ts.maxWordSizeInBytes

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

					prevWords := ""
					nextWords := ""

					if context > 0 {
						prevWords = ts.findPrevWords(i, wordIndex+skip, context)
						nextWords = ts.findNextWords(i, wordIndex+skip + len(word), context)
					}

					match := fmt.Sprintf("%v%v%v", prevWords,
						string(ts.fileBuffers[i][wordIndex+skip:wordIndex+skip +len(word)]),
						nextWords)

					matches[i] = append(matches[i], match)
					skip += wordIndex+len(word)
					buffer = buffer[wordIndex+len(word):]
				}
			}
		}(fileBufferIndex)
	}

	wg.Wait()

	//Flatten the [][]string into just a []string
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

// findPrevWords gets the previous N words after a given index in a buffer
func (ts TextSearcher) findPrevWords(bufferNum int, bufferIndex int, numWords int) string {
	return ts.findContext(
		removeReturnCharacters(ts.fileBuffers[bufferNum][:bufferIndex]),
		true, bufferNum, bufferIndex, numWords)
}

// findContext gets N words from the left or right of our search term.
func (ts TextSearcher) findContext(bufferBeforeWord []byte, prev bool, bufferNum int, bufferIndex int, numWords int) string  {
	bufferAsSplitBytes := bytes.Fields(bufferBeforeWord)
	numWords = minInt(len(bufferAsSplitBytes), numWords)

	var wordsInBytes [][]byte

	if prev {
		wordsInBytes = bufferAsSplitBytes[len(bufferAsSplitBytes)-numWords:]
	} else {
		wordsInBytes = bufferAsSplitBytes[0:numWords]
	}

	if len(wordsInBytes) == 0 {
		return ""
	}

	if prev {
		for _, b := range []byte("\".") {
			if bytes.Equal(wordsInBytes[0], []byte{b}){
				return ts.findContext(removeReturnCharacters(ts.fileBuffers[bufferNum][:bufferIndex-1]), prev, bufferNum, bufferIndex, numWords) + string(b)
			}
		}
	} else {
		for _, b := range []byte("\".") {
			if bytes.Equal(wordsInBytes[0], []byte{b}){
				return string(b) + ts.findContext(removeReturnCharacters(ts.fileBuffers[bufferNum][bufferIndex+1:]), prev, bufferNum, bufferIndex+1, numWords)
			}
		}
	}

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

	if (bufferNum == 0 && index == 0)  ||
		(bufferNum == len(ts.fileBuffers) && index+len(word) == int(chunkSize)){
		return true, nil
	}

	if index-1 < 0 || index+1 >  int(math.Ceil(chunkSize)){
		return false, fmt.Errorf("out of bounds of OS_WORD_SIZE buffer")
	}

	byteBeforeWord := ts.fileBuffers[bufferNum][index-1]
	byteAfterWord := ts.fileBuffers[bufferNum][index + len(word)]

	//If the byte before or after the word is a word-character a-zA-Z0-9'-
	if bytes.ContainsRune(WordCharactersList, rune(byteBeforeWord)) ||
		bytes.ContainsRune(WordCharactersList, rune(byteAfterWord)) {
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
		if bytes.ContainsRune(WordCharactersList, rune(byteBeforeWord)){
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