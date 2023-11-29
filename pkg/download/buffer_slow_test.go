//go:build slow
// +build slow

package download_test

import (
	"testing"
)

func BenchmarkDownload10G(b *testing.B) {
	benchmarkDownloadSingleFile(defaultOpts, 10*1024*1024*1024, b)
}
func BenchmarkDownload10GH2(b *testing.B) {
	benchmarkDownloadSingleFile(http2Opts, 10*1024*1024*1024, b)
}

func BenchmarkDownloadDollyTensors(b *testing.B) {
	benchmarkDownloadURL(defaultOpts, "https://storage.googleapis.com/replicate-weights/dolly-v2-12b-fp16.tensors", b)
}
