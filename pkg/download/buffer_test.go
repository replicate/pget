package download_test

import (
	"context"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"

	"github.com/replicate/pget/pkg/client"
	"github.com/replicate/pget/pkg/download"
)

func init() {
	zerolog.SetGlobalLevel(zerolog.WarnLevel)
}

var defaultOpts = download.Options{Client: client.Options{}}
var http2Opts = download.Options{Client: client.Options{ForceHTTP2: true}}

func benchmarkDownloadURL(opts download.Options, url string, b *testing.B) {
	bufferMode := download.GetBufferMode(opts)

	for n := 0; n < b.N; n++ {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, _, err := bufferMode.Fetch(ctx, url)
		assert.NoError(b, err)
	}
}

func BenchmarkDownloadBertH1(b *testing.B) {
	benchmarkDownloadURL(defaultOpts, "https://storage.googleapis.com/replicate-weights/bert-base-uncased-hf-cache.tar", b)
}
func BenchmarkDownloadBertH2(b *testing.B) {
	benchmarkDownloadURL(http2Opts, "https://storage.googleapis.com/replicate-weights/bert-base-uncased-hf-cache.tar", b)
}
func BenchmarkDownloadLlama7bChatH1(b *testing.B) {
	benchmarkDownloadURL(defaultOpts, "https://storage.googleapis.com/replicate-weights/Llama-2-7b-Chat-GPTQ/gptq_model-4bit-32g.safetensors", b)
}
func BenchmarkDownloadLlama7bChatH2(b *testing.B) {
	benchmarkDownloadURL(http2Opts, "https://storage.googleapis.com/replicate-weights/Llama-2-7b-Chat-GPTQ/gptq_model-4bit-32g.safetensors", b)
}
