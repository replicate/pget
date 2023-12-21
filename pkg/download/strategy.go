package download

import (
	"context"
	"io"
	"net/http"
)

type Strategy interface {
	// Fetch retrieves the content from a given URL and returns it as an io.Reader along with the file size.
	// If an error occurs during the process, it returns nil for the reader, 0 for the fileSize, and the error itself.
	// This is the primary method that should be called to initiate a download of a file.
	Fetch(ctx context.Context, url string) (result io.Reader, fileSize int64, err error)

	// DoRequest executes an HTTP request with a specified range of bytes.
	// It returns the HTTP response and any error encountered during the request. It is intended that Fetch calls DoRequest
	// and that each chunk is downloaded with a call to DoRequest. DoRequest is exposed so that consistent-hashing can
	// utilize any strategy as a fall-back for chunk downloading.
	//
	// If the request fails to download or execute, an error is returned.
	//
	// The start and end parameters specify the byte range to request.
	DoRequest(req *http.Request, start, end int64) (*http.Response, error)
}
