package consumer_test

import (
	"archive/tar"
	"bytes"
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/replicate/pget/pkg/consumer"
)

const (
	file1Content     = "This is the content of file1."
	file2Content     = "This is the content of file2."
	file1Path        = "file1.txt"
	file2Path        = "file2.txt"
	fileSymLinkPath  = "link_to_file1.txt"
	fileHardLinkPath = "subdir/hard_link_to_file2.txt"
)

func createTarFileBytesBuffer() ([]byte, error) {
	// Create an in-memory representation of a tar file dynamically. This will be used to test the TarExtractor

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)

	// Create first file
	content1 := []byte(file1Content)
	hdr := &tar.Header{
		Name:    file1Path,
		Mode:    0600,
		Size:    int64(len(content1)),
		ModTime: time.Now(),
	}
	if err := tw.WriteHeader(hdr); err != nil {
		return nil, err
	}
	if _, err := tw.Write(content1); err != nil {
		return nil, err
	}

	// Create second file
	content2 := []byte(file2Content)
	hdr = &tar.Header{
		Name:    file2Path,
		Mode:    0600,
		Size:    int64(len(content2)),
		ModTime: time.Now(),
	}
	if err := tw.WriteHeader(hdr); err != nil {
		return nil, err
	}
	if _, err := tw.Write(content2); err != nil {
		return nil, err
	}

	// Create a symlink to file1
	hdr = &tar.Header{
		Name:     fileSymLinkPath,
		Mode:     0777,
		Size:     0,
		Linkname: file1Path,
		Typeflag: tar.TypeSymlink,
		ModTime:  time.Now(),
	}
	if err := tw.WriteHeader(hdr); err != nil {
		return nil, err
	}

	// Create a subdirectory or path for the hardlink
	hdr = &tar.Header{
		Name:     "subdir/",
		Mode:     0755,
		Typeflag: tar.TypeDir,
		ModTime:  time.Now(),
	}
	if err := tw.WriteHeader(hdr); err != nil {
		return nil, err
	}

	// Create a hardlink to file2 in the subdirectory
	hdr = &tar.Header{
		Name:     fileHardLinkPath,
		Mode:     0600,
		Size:     0,
		Linkname: file2Path,
		Typeflag: tar.TypeLink,
		ModTime:  time.Now(),
	}
	if err := tw.WriteHeader(hdr); err != nil {
		return nil, err
	}

	// Close the tar writer to flush the data
	if err := tw.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func TestTarExtractor_Consume(t *testing.T) {
	r := require.New(t)

	tarFileBytes, err := createTarFileBytesBuffer()
	r.NoError(err)

	// Create a reader from the tar file bytes
	reader := bytes.NewReader(tarFileBytes)

	// Create a temporary directory to extract the tar file
	tmpDir, err := os.MkdirTemp("", "tarExtractorTest-")
	r.NoError(err)

	t.Cleanup(func() { os.RemoveAll(tmpDir) })

	tarConsumer := consumer.TarExtractor{}
	targetDir := path.Join(tmpDir, "extract")
	r.NoError(tarConsumer.Consume(reader, targetDir, int64(len(tarFileBytes))))

	// Check if the extraction was successful
	checkTarExtraction(t, targetDir)

	// Test with incorrect expectedBytes
	_, _ = reader.Seek(0, 0)
	targetDir = path.Join(tmpDir, "extract-fail")
	r.Error(tarConsumer.Consume(reader, targetDir, int64(len(tarFileBytes)-1)))
}

func checkTarExtraction(t *testing.T, targetDir string) {
	r := require.New(t)

	// Verify that file1.txt is correctly extracted
	fqFile1Path := path.Join(targetDir, file1Path)
	content, err := os.ReadFile(fqFile1Path)
	r.NoError(err)
	r.Equal(file1Content, string(content))

	// Verify that file2.txt is correctly extracted
	fqFile2Path := path.Join(targetDir, file2Path)
	content, err = os.ReadFile(fqFile2Path)
	r.NoError(err)
	r.Equal(file2Content, string(content))

	// Verify that link_to_file1.txt is a symlink pointing to file1.txt
	linkToFile1Path := path.Join(targetDir, fileSymLinkPath)
	linkTarget, err := os.Readlink(linkToFile1Path)
	r.NoError(err)
	r.Equal(file1Path, linkTarget)
	r.Equal(os.ModeSymlink, os.ModeSymlink&os.ModeType)

	// Verify that subdir/hard_link_to_file2.txt is a hard link to file2.txt
	hardLinkToFile2Path := path.Join(targetDir, fileHardLinkPath)
	hardLinkStat, err := os.Stat(hardLinkToFile2Path)
	r.NoError(err)
	file2Stat, err := os.Stat(fqFile2Path)
	r.NoError(err)

	if !os.SameFile(hardLinkStat, file2Stat) {
		t.Errorf("hard link does not match file2.txt")
	}
}
