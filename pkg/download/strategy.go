package download

import (
	"context"
	"fmt"
	"net/http"

	"github.com/replicate/pget/pkg/client"
)

var ErrDownloadNotStarted = fmt.Errorf("download not started")

type byteRange struct {
	start int64
	end   int64
}

type Segment struct {
	byteRange byteRange
	response  *http.Response
	request   *http.Request
	started   bool
}

func (s *Segment) Read(p []byte) (n int, err error) {
	if !s.started {
		return 0, ErrDownloadNotStarted
	}
	return s.response.Body.Read(p)
}

func (s *Segment) Close() error {
	if !s.started {
		return ErrDownloadNotStarted
	}
	return s.response.Body.Close()
}

func (s *Segment) ByteRange() (int64, int64) {
	return s.byteRange.start, s.byteRange.end
}

func (s *Segment) Request() *http.Request {
	return s.request
}

func (s *Segment) Response() *http.Response {
	if !s.started {
		return nil
	}
	return s.response
}

func (s *Segment) Started() bool {
	return s.started
}

func (s *Segment) RangeHeader() string {
	return fmt.Sprintf("bytes=%d-%d", s.byteRange.start, s.byteRange.end)
}

type File struct {
	URL          string
	FilePath     string
	ContentType  string
	Size         int64
	SegmentCount int
	Segments     []*Segment
}

// Strategy is a download strategy that splits the file into multiple Segments and downloads them concurrently
type Strategy struct {
	Client    *client.HTTPClient
	Options   Options
	Segmenter Segmenter
	Files     []*File
}

func (s *Strategy) InitializeDownload(ctx context.Context, url, filePath string) (*File, error) {
	req, err := http.NewRequest("HEAD", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := s.Client.Do(req)
	if err != nil {
		return nil, err
	}
	// TODO Status Code Handling
	segments, err := s.Segmenter.Segment(ctx, req, resp, s.Options.MaxChunks, s.Options.MinChunkSize)
	if err != nil {
		return nil, err
	}
	if resp.ContentLength == -1 {
		return nil, fmt.Errorf("could not determine file size")

	}
	// TODO: Additional Content-Type handling (e.g. extension based if not provider or application/octet-stream)
	file := &File{
		URL:          url,
		FilePath:     filePath,
		ContentType:  resp.Header.Get("Content-Type"),
		Size:         resp.ContentLength,
		SegmentCount: len(segments),
		Segments:     segments,
	}
	s.Files = append(s.Files, file)

	return file, nil
}

func (s *Strategy) StartSegmentDownload(ctx context.Context, segment *Segment) error {
	req := segment.Request()
	resp, err := s.Client.Do(req)
	if err != nil {
		return err
	}

	segment.started = true
	segment.response = resp

	return nil
}

type Segmenter interface {
	// Segment splits the file into multiple Segments for concurrent download. It is critical that the Segmenter returns a list of Segments that
	// have not been started. The Segmenter should also set the Range header on the request to the appropriate byte range on the segment request.
	Segment(ctx context.Context, req *http.Request, resp *http.Response, maxSegments int, minSegmentSize int64) (segments []*Segment, err error)
}

// The DefaultSegmenter splits the file into equal sized chunks. The chunk size is constraind by the maxSegments and minSegmentSize.
// If the file size is less than the minSegmentSize, the file will be a single segment.
type DefaultSegmenter struct {
}

func (s *DefaultSegmenter) Segment(ctx context.Context, req *http.Request, resp *http.Response, maxSegments int, minSegmentSize int64) ([]*Segment, error) {
	fileSize := resp.ContentLength
	segmentCount := maxSegments
	chunkSize := fileSize / int64(segmentCount)
	if chunkSize < minSegmentSize {
		segmentCount = int(fileSize / minSegmentSize)
		chunkSize = fileSize / int64(segmentCount)
	}
	segments := make([]*Segment, segmentCount)
	for i := 0; i < segmentCount; i++ {
		segmentReq, err := http.NewRequest("GET", req.URL.String(), nil)
		if err != nil {
			return nil, err
		}
		start := int64(i) * chunkSize
		end := start + chunkSize - 1
		segment := &Segment{
			byteRange: byteRange{
				start: start,
				end:   end,
			},
			request: segmentReq,
		}
		segmentReq.Header.Set("Range", segment.RangeHeader())
		segments[i] = segment
	}
	segments[segmentCount-1].byteRange.end = fileSize - 1
	return segments, nil
}
