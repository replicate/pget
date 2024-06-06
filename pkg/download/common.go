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
	"github.com/replicate/pget/pkg/logging"
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
	logger := logging.GetLogger()

	var resumeCount = 1
	var initialBytesReceived = bytesReceived

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
			bytesReceived = int64(n)
			startByte += n
			resumeCount++
			logger.Warn().
				Int("connection_interrupted_at_byte", n).
				Int("resume_count", resumeCount).
				Int64("total_bytes_received", initialBytesReceived+int64(startByte)).
				Msg("Resuming Chunk Download")
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
	newRangeHeader := fmt.Sprintf("bytes=%d-%d", start, end)

	if start > end {
		return fmt.Errorf("%w: %s", errInvalidContentRange, newRangeHeader)
	}

	req.Header.Set("Range", newRangeHeader)

	return nil
}
