package download

import (
	"context"
	"errors"
	"io"
	"net/http"
)

var ErrUnexpectedHTTPStatus = errors.New("unexpected http status")

type Strategy interface {
	// Fetch retrieves the content from a given URL and returns it as an io.Reader along with the file size.
	// If an error occurs during the process, it returns nil for the reader, 0 for the fileSize, and the error itself.
	// This is the primary method that should be called to initiate a download of a file.
	Fetch(ctx context.Context, url string) (result io.Reader, fileSize int64, contentType string, err error)

	// DoRequest sends an HTTP GET request with a specified range of bytes to the given URL using the provided context.
	// It returns the HTTP response and any error encountered during the request. It is intended that Fetch calls DoRequest
	// and that each chunk is downloaded with a call to DoRequest. DoRequest is exposed so that consistent-hashing can
	// utilize any strategy as a fall-back for chunk downloading.
	//
	// If the request fails to download or execute, an error is returned.
	//
	// The start and end parameters specify the byte range to request.
	// The trueURL parameter is the actual URL after any redirects.
	DoRequest(ctx context.Context, start, end int64, url string) (*http.Response, error)
}
