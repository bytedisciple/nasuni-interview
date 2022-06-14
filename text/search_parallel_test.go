package text

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

// Unit tests specifically for parallel search. Provided unit tests were incomplete given a parallelized solution.

func TestParallelSearch(t *testing.T) {
	t.Run("Match across chunks", func(t *testing.T) {
		ts, err := NewSearcher("../files/Siddhartha.txt")
		assert.NoError(t, err)
		assert.ElementsMatch(t,
			[]string{"and joy, he still lacked all joy in his",
				"strength and tenacity. They lacked nothing, there was nothing"},
			ts.Search("lacked", 4),
		)
	})

}
