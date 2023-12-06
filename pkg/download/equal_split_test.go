package download_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/replicate/pget/pkg/download"
)

type equalSplitTestCase struct {
	number   int64
	parts    int64
	expected []int64
}

var equalSplitTestCases = []equalSplitTestCase{
	{
		number:   5,
		parts:    1,
		expected: []int64{5},
	},
	{
		number:   5,
		parts:    5,
		expected: []int64{1, 1, 1, 1, 1},
	},
	{
		number:   32,
		parts:    3,
		expected: []int64{11, 11, 10},
	},
	{
		number:   32,
		parts:    5,
		expected: []int64{7, 7, 6, 6, 6},
	},
}

func TestEqualSplit(t *testing.T) {
	for _, testCase := range equalSplitTestCases {
		actual := download.EqualSplit(testCase.number, testCase.parts)
		assert.Equal(t, testCase.expected, actual)
	}
}
