package text

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Unit tests for search: DO NOT MODIFY THIS FILE

func TestSearch(t *testing.T) {

	t.Run("No context and the word occurs once", func(t *testing.T) {
		ts, err := NewSearcher("../files/Siddhartha.opening.txt")
		assert.NoError(t, err)
		assert.ElementsMatch(t,
			[]string{"riverbank"},
			ts.Search("riverbank", 0),
		)
	})

	t.Run("No context and the multiple occurrences", func(t *testing.T) {
		ts, err := NewSearcher("../files/Siddhartha.opening.txt")
		assert.NoError(t, err)
		assert.ElementsMatch(t,
			[]string{"shade", "shade", "shade", "shade"},
			ts.Search("shade", 0),
		)
	})

	t.Run("No matches for word", func(t *testing.T) {
		ts, err := NewSearcher("../files/Siddhartha.opening.txt")
		assert.NoError(t, err)
		assert.ElementsMatch(t,
			[]string{},
			ts.Search("potato", 2),
		)
	})

	t.Run("Simple match with a little context", func(t *testing.T) {
		ts, err := NewSearcher("../files/Siddhartha.opening.txt")
		assert.NoError(t, err)
		assert.ElementsMatch(t,
			[]string{"to feel Atman in the"},
			ts.Search("Atman", 2),
		)
	})

	t.Run("Simple match with a more context", func(t *testing.T) {
		ts, err := NewSearcher("../files/Siddhartha.opening.txt")
		assert.NoError(t, err)
		assert.ElementsMatch(t,
			[]string{"In the mango grove, shade poured into his black eyes, when playing as a boy, when his"},
			ts.Search("black", 8),
		)
	})

	t.Run("Dashes match correctly", func(t *testing.T) {
		ts, err := NewSearcher("../files/Siddhartha.opening.txt")
		assert.NoError(t, err)
		assert.ElementsMatch(t,
			[]string{"shade of the Sal-wood forest, in the"},
			ts.Search("Sal-wood", 3),
		)
	})

	t.Run("Apostophes match correctly", func(t *testing.T) {
		ts, err := NewSearcher("../files/Siddhartha.txt")
		assert.NoError(t, err)
		assert.ElementsMatch(t,
			[]string{"the evening's ablution."},
			ts.Search("evening's", 1),
		)
	})

	t.Run("Quotes match correctly", func(t *testing.T) {
		ts, err := NewSearcher("../files/Siddhartha.txt")
		assert.NoError(t, err)
		assert.ElementsMatch(t,
			[]string{
				"Govinda: \"Early tomorrow",
				"Very early in",
				"and early in",
				"an early pre-birth",
				"sleeping. Early in",
			},
			ts.Search("Early", 1),
		)
	})

	t.Run("Numbers match correctly", func(t *testing.T) {
		ts, err := NewSearcher("../files/Siddhartha.txt")
		assert.NoError(t, err)
		assert.ElementsMatch(t,
			[]string{"within 90 days", "within 90 days"},
			ts.Search("90", 1),
		)
	})

	t.Run("Alphanumeric words match correctly", func(t *testing.T) {
		ts, err := NewSearcher("../files/Siddhartha.txt")
		assert.NoError(t, err)
		assert.ElementsMatch(t,
			[]string{"named 2500-8.txt or"},
			ts.Search("2500-8.txt", 1),
		)
	})

	t.Run("Matches at the beginning of the file", func(t *testing.T) {
		ts, err := NewSearcher("../files/Siddhartha.opening.txt")
		assert.NoError(t, err)
		assert.ElementsMatch(t,
			[]string{
				"In the shade of the house, in",
				"the boats, in the shade of the Sal-wood forest,",
				"Sal-wood forest, in the shade of the fig tree",
				"In the mango grove, shade poured into his black",
			},
			ts.Search("shade", 4),
		)
	})

	t.Run("Matches at the end of the file", func(t *testing.T) {
		ts, err := NewSearcher("../files/Siddhartha.opening.txt")
		assert.NoError(t, err)
		assert.ElementsMatch(t,
			[]string{
				"one with the universe.",
			},
			ts.Search("universe", 3),
		)
	})

	t.Run("Matches are case insensitive", func(t *testing.T) {
		ts, err := NewSearcher("../files/Siddhartha.opening.txt")
		assert.NoError(t, err)
		assert.Equal(t,
			7,
			len(ts.Search("in", 0)),
		)
		assert.Equal(t,
			7,
			len(ts.Search("In", 0)),
		)
		assert.Equal(t,
			7,
			len(ts.Search("IN", 0)),
		)
		assert.Equal(t,
			7,
			len(ts.Search("iN", 0)),
		)
	})

	t.Run("Multple searches on one file", func(t *testing.T) {
		ts, err := NewSearcher("../files/Siddhartha.opening.txt")
		assert.NoError(t, err)
		assert.ElementsMatch(t,
			[]string{
				"In the shade of the house, in",
				"the boats, in the shade of the Sal-wood forest,",
				"Sal-wood forest, in the shade of the fig tree",
				"In the mango grove, shade poured into his black",
			},
			ts.Search("shade", 4),
		)
		assert.ElementsMatch(t,
			[]string{
				"one with the universe.",
			},
			ts.Search("universe", 3),
		)
	})

	t.Run("Matches with overlapping results", func(t *testing.T) {
		ts, err := NewSearcher("../files/Siddhartha.opening.txt")
		assert.NoError(t, err)
		assert.ElementsMatch(t,
			[]string{
				"river when bathing, performing the sacred ablutions, the sacred offerings. In",
				"performing the sacred ablutions, the sacred offerings. In the mango grove,",
				"his mother sang, when the sacred offerings were made, when his",
			},
			ts.Search("sacred", 5),
		)
	})

}
