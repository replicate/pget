package consistent_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/replicate/pget/pkg/consistent"
)

func TestHashingDoesNotChangeWhenZeroValueFieldsAreAdded(t *testing.T) {
	a, err := consistent.HashBucket(struct{}{}, 1024)
	require.NoError(t, err)
	b, err := consistent.HashBucket(struct{ I int }{}, 1024)
	require.NoError(t, err)

	assert.Equal(t, a, b)
}

func TestRetriesScatterBuckets(t *testing.T) {
	// This test is tricky! We want an example of hash keys which map to the
	// same bucket, but after one retry map to different buckets.
	//
	// These two keys happen to have this property for 10 buckets:
	strA := "abcdefg"
	strB := "1234567"
	a, err := consistent.HashBucket(strA, 10)
	require.NoError(t, err)
	b, err := consistent.HashBucket(strB, 10)
	require.NoError(t, err)

	// strA and strB to map to the same bucket
	require.Equal(t, a, b)

	aRetry, err := consistent.HashBucket(strA, 10, a)
	require.NoError(t, err)
	bRetry, err := consistent.HashBucket(strB, 10, b)
	require.NoError(t, err)

	// but after retry they map to different buckets
	assert.NotEqual(t, aRetry, bRetry)
}

func FuzzRetriesMostNotRepeatIndices(f *testing.F) {
	f.Add("test.replicate.delivery", 5)
	f.Add("test.replicate.delivery", 0)
	f.Fuzz(func(t *testing.T, key string, excessBuckets int) {
		if excessBuckets < 0 {
			t.Skip("invalid value")
		}
		attempts := 20
		buckets := attempts + excessBuckets
		if buckets < 0 {
			t.Skip("integer overflow")
		}
		previous := []int{}
		for i := 0; i < attempts; i++ {
			next, err := consistent.HashBucket(key, buckets, previous...)
			require.NoError(t, err)

			// we must be in range
			assert.Less(t, next, buckets)
			assert.GreaterOrEqual(t, next, 0)

			// we shouldn't repeat any previous value
			assert.NotContains(t, previous, next)

			previous = append(previous, next)
		}
	})
}
