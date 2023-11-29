package extract

import (
	"archive/tar"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/replicate/pget/pkg/logging"
)

func ExtractTarFile(reader io.Reader, destDir string) error {
	startTime := time.Now()
	tarReader := tar.NewReader(reader)
	logger := logging.GetLogger()

	logger.Debug().
		Str("extractor", "tar").
		Str("status", "starting").
		Msg("Extract")
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		target := filepath.Join(destDir, header.Name)
		targetDir := filepath.Dir(target)
		if err := os.MkdirAll(targetDir, 0755); err != nil {
			return err
		}

		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, os.FileMode(header.Mode)); err != nil {
				return err
			}
		case tar.TypeReg:
			targetFile, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY, os.FileMode(header.Mode))
			if err != nil {
				return err
			}
			if _, err := io.Copy(targetFile, tarReader); err != nil {
				targetFile.Close()
				return err
			}
			targetFile.Close()
		case tar.TypeSymlink:
			if err := os.Symlink(header.Linkname, target); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported file type for %s, typeflag %s", header.Name, string(header.Typeflag))
		}
	}
	elapsed := time.Since(startTime).Seconds()
	logger.Debug().
		Str("extractor", "tar").
		Float64("elapsed_time", elapsed).
		Str("status", "complete").
		Msg("Extract")
	return nil
}
