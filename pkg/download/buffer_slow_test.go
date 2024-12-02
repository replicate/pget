//go:build slow
// +build slow

package download_test

import (
	"github.com/dustin/go-humanize"

	"testing"
)

func BenchmarkDownload10G(b *testing.B) {
	benchmarkDownloadSingleFile(defaultOpts, 10*humanize.GiByte, b)
}
func BenchmarkDownload10GH2(b *testing.B) {
	benchmarkDownloadSingleFile(http2Opts, 10*humanize.GiByte, b)
}

func BenchmarkDownloadDollyTensors(b *testing.B) {
	benchmarkDownloadURL(defaultOpts, "https://weights.replicate.delivery/default/dolly-v2-12b-fp16.tensors", b)
}
