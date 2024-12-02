package consumer_test

import (
	"math/rand"
)

// generateTestContent generates a byte slice of a random size > 1KiB
func generateTestContent(size int64) []byte {
	content := make([]byte, size)
	// Generate random bytes and write them to the content slice
	for i := range content {
		content[i] = byte(rand.Intn(256))
	}
	return content

}
