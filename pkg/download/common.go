package download

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/dustin/go-humanize"
	"github.com/replicate/pget/pkg/client"
)

const defaultChunkSize = 125 * humanize.MiByte

var (
	contentRangeRegexp = regexp.MustCompile(`^bytes .*/([0-9]+)$`)

	errMalformedRangeHeader = errors.New("malformed range header")
	errMissingRangeHeader   = errors.New("missing range header")
	errInvalidContentRange  = errors.New("invalid content range")
)

func resumeDownload(req *http.Request, buffer []byte, client client.HTTPClient, bytesReceived int64) (*http.Response, error) {
	var startByte int
	for {
		var n int
		if err := updateRangeRequestHeader(req, bytesReceived); err != nil {
			return nil, err
		}
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusPartialContent {
			return nil, fmt.Errorf("expected status code %d, got %d", http.StatusPartialContent, resp.StatusCode)
		}
		n, err = io.ReadFull(resp.Body, buffer[startByte:])
		if err == io.ErrUnexpectedEOF {
			startByte = n
			continue
		}
		return nil, err

	}
}

func updateRangeRequestHeader(req *http.Request, receivedBytes int64) error {
	rangeHeader := req.Header.Get("Range")
	if rangeHeader == "" {
		return errMissingRangeHeader
	}

	// Expected format: "bytes=start-end"
	if !strings.HasPrefix(rangeHeader, "bytes=") {
		return fmt.Errorf("%w: %s", errMalformedRangeHeader, rangeHeader)
	}

	rangeValues := strings.TrimPrefix(rangeHeader, "bytes=")
	parts := strings.Split(rangeValues, "-")
	if len(parts) != 2 {
		return fmt.Errorf("%w: %s", errMalformedRangeHeader, rangeHeader)
	}

	start, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return fmt.Errorf("%w: %s", errMalformedRangeHeader, rangeHeader)
	}

	end, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return fmt.Errorf("%w: %s", errMalformedRangeHeader, rangeHeader)
	}

	start = start + receivedBytes
	if start > end {
		return fmt.Errorf("%w: %s", errInvalidContentRange, rangeHeader)
	}

	newRangeHeader := fmt.Sprintf("bytes=%d-%d", start, end)
	req.Header.Set("Range", newRangeHeader)

	return nil
}
