package download

import (
	"regexp"

	"github.com/dustin/go-humanize"
)

const defaultChunkSize = 125 * humanize.MiByte

var contentRangeRegexp = regexp.MustCompile(`^bytes .*/([0-9]+)$`)
