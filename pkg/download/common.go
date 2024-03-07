package download

import (
	"fmt"
	"regexp"

	"github.com/dustin/go-humanize"
)

const defaultChunkSize = 125 * humanize.MiByte

var contentRangeRegexp = regexp.MustCompile(`^bytes .*/([0-9]+)$`)

type ErrContentLengthMismatch struct {
	contentLength   int64
	downloadedBytes int64
}

var _ error = ErrContentLengthMismatch{}

func (err ErrContentLengthMismatch) Error() string {
	return fmt.Sprintf("Downloaded %d bytes but Content-Length was %d", err.downloadedBytes, err.contentLength)
}
