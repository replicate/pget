package download

import (
	"github.com/dustin/go-humanize"
	"regexp"
)

const defaultMinChunkSize = 16 * humanize.MiByte
const defaultChunkSize = 125 * humanize.MiByte

var contentRangeRegexp = regexp.MustCompile(`^bytes .*/([0-9]+)$`)
