package download

import (
	"context"
	"io"
)

type Strategy interface {
	Fetch(ctx context.Context, url string) (result io.Reader, fileSize int64, err error)
}
