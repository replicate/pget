package multireader

import "errors"

var (
	ErrExceedsCapacity   = errors.New("multireader.BufferedReader: peek size exceeds capacity")
	ErrReaderMarkedReady = errors.New("multireader.BufferedReader: reader already marked Ready")
	ErrNegativeCount     = errors.New("multireader: negative count")
	ErrSizeAlreadySet    = errors.New("multireader.BufferedReader: size already set")
	ErrClosed            = errors.New("multireader.MultiBufferedReader: closed")
)
