// Package consistent implements consistent hashing for cache nodes.
package consistent

import (
	"fmt"
	"slices"

	"github.com/dgryski/go-jump"
	"github.com/mitchellh/hashstructure/v2"
)

type cacheKey struct {
	Key     any
	Attempt int
}

// HashBucket returns a bucket from [0,buckets). If you want to implement a
// retry, you can pass previousBuckets, which indicates buckets which must be
// avoided in the output. HashBucket will modify the previousBuckets slice by
// sorting it.
func HashBucket(key any, buckets int, previousBuckets ...int) (int, error) {
	if len(previousBuckets) >= buckets {
		return -1, fmt.Errorf("No more buckets left: %d buckets available but %d already attempted", buckets, previousBuckets)
	}
	// we set IgnoreZeroValue so that we can add fields to the hash key
	// later without breaking things.
	// note that it's not safe to share a HashOptions so we create a fresh one each time.
	hashopts := &hashstructure.HashOptions{IgnoreZeroValue: true}
	hash, err := hashstructure.Hash(cacheKey{Key: key, Attempt: len(previousBuckets)}, hashstructure.FormatV2, hashopts)
	if err != nil {
		return -1, fmt.Errorf("error calculating hash of key: %w", err)
	}

	// jump is an implementation of Google's Jump Consistent Hash.
	//
	// See http://arxiv.org/abs/1406.2294 for details.
	bucket := int(jump.Hash(hash, buckets-len(previousBuckets)))
	slices.Sort(previousBuckets)
	for _, prev := range previousBuckets {
		if bucket >= prev {
			bucket++
		}
	}
	return bucket, nil
}
