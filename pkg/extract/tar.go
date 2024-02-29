package extract

import (
	"archive/tar"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/replicate/pget/pkg/logging"
)

type link struct {
	linkType byte
	oldName  string
	newName  string
}

func TarFile(reader io.Reader, destDir string, overwrite bool) error {
	var links []*link

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

		if err := guardAgainstZipSlip(header, destDir); err != nil {
			return err
		}

		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, cleanFileMode(os.FileMode(header.Mode))); err != nil {
				return err
			}
		case tar.TypeReg:
			openFlags := os.O_CREATE | os.O_WRONLY
			if overwrite {
				openFlags |= os.O_TRUNC
			}
			targetFile, err := os.OpenFile(target, openFlags, cleanFileMode(os.FileMode(header.Mode)))
			if err != nil {
				return err
			}
			if _, err := io.Copy(targetFile, tarReader); err != nil {
				targetFile.Close()
				return err
			}
			if err := targetFile.Close(); err != nil {
				return fmt.Errorf("error closing file %s: %w", target, err)
			}
		case tar.TypeSymlink, tar.TypeLink:
			// Defer creation of
			links = append(links, &link{linkType: header.Typeflag, oldName: header.Linkname, newName: target})
		default:
			return fmt.Errorf("unsupported file type for %s, typeflag %s", header.Name, string(header.Typeflag))
		}
	}

	if err := createLinks(links, destDir, overwrite); err != nil {
		return fmt.Errorf("error creating links: %w", err)
	}

	elapsed := time.Since(startTime).Seconds()
	logger.Debug().
		Str("extractor", "tar").
		Float64("elapsed_time", elapsed).
		Str("status", "complete").
		Msg("Extract")
	return nil
}

func createLinks(links []*link, destDir string, overwrite bool) error {
	for _, link := range links {
		targetDir := filepath.Dir(link.newName)
		if err := os.MkdirAll(targetDir, 0755); err != nil {
			return err
		}
		switch link.linkType {
		case tar.TypeLink:
			oldPath := filepath.Join(destDir, link.oldName)
			if err := createHardLink(oldPath, link.newName, overwrite); err != nil {
				return fmt.Errorf("error creating hard link from %s to %s: %w", oldPath, link.newName, err)
			}
		case tar.TypeSymlink:
			if err := createSymlink(link.oldName, link.newName, overwrite); err != nil {
				return fmt.Errorf("error creating symlink from %s to %s: %w", link.oldName, link.newName, err)
			}
		default:
			return fmt.Errorf("unsupported link type %s", string(link.linkType))
		}
	}
	return nil
}

func createHardLink(oldName, newName string, overwrite bool) error {
	if overwrite {
		err := os.Remove(newName)
		if err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("error removing existing file: %w", err)
		}
	}
	return os.Link(oldName, newName)
}

func createSymlink(oldName, newName string, overwrite bool) error {
	if overwrite {
		err := os.Remove(newName)
		if err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("error removing existing symlink/file: %w", err)
		}
	}
	return os.Symlink(oldName, newName)
}

func guardAgainstZipSlip(header *tar.Header, destDir string) error {
	if header.Name == "" {
		return fmt.Errorf("tar file contains entry with empty name")
	}
	target, err := filepath.Abs(filepath.Join(destDir, header.Name))
	if err != nil {
		return fmt.Errorf("error getting absolute path of destDir %s: %w", header.Name, err)
	}
	filePath, err := filepath.Abs(target)
	if err != nil {
		return fmt.Errorf("error getting absolute path of %s: %w", target, err)
	}
	if !strings.HasPrefix(filePath, destDir) {
		return fmt.Errorf("archive (tar) file contains file (%s) outside of target directory: %s", filePath, target)
	}
	return nil
}

func cleanFileMode(mode os.FileMode) os.FileMode {
	mask := os.ModeSticky | os.ModeSetuid | os.ModeSetgid
	return mode &^ mask
}
