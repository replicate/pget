//go:build slow
// +build slow

package download

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func BenchmarkDownload10G(b *testing.B) { benchmarkDownloadSingleFile(10*1024*1024*1024, b) }

func BenchmarkDownloadDollyTensors(b *testing.B) {
	bufferMode := makeBufferMode()

	for n := 0; n < b.N; n++ {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		url := "https://storage.googleapis.com/replicate-weights/dolly-v2-12b-fp16.tensors"
		_, _, err := bufferMode.fileToBuffer(ctx, Target{URL: url, TrueURL: url})
		assert.NoError(b, err)
	}
}
