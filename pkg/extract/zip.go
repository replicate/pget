package extract

import (
	"archive/zip"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"

	"github.com/replicate/pget/pkg/logging"
)

// ZipFile extracts a zip file to the given destination path.
func ZipFile(reader io.ReaderAt, destPath string, size int64, overwrite bool) error {
	logger := logging.GetLogger()
	err := os.MkdirAll(destPath, 0755)
	if err != nil {
		return fmt.Errorf("error creating destination directory: %w", err)
	}

	logger.Debug().
		Str("extractor", "zip").
		Str("status", "starting").
		Bool("overwrite", overwrite).
		Str("destDir", destPath).
		Msg("Extract")
	zipReader, err := zip.NewReader(reader, size)
	if err != nil {
		return fmt.Errorf("error creating zip reader: %w", err)
	}

	for _, file := range zipReader.File {
		err := handleFileFromZip(file, destPath, overwrite)
		if err != nil {
			return fmt.Errorf("error extracting file: %w", err)
		}
	}
	return nil
}

func handleFileFromZip(file *zip.File, outputDir string, overwrite bool) error {
	if file.FileInfo().IsDir() {
		return extractDir(file, outputDir)
	} else if file.FileInfo().Mode().IsRegular() {
		return extractFile(file, outputDir, overwrite)
	} else {
		return fmt.Errorf("unsupported file type (not dir or regular): %s (%d)", file.Name, file.FileInfo().Mode().Type())
	}

}

func extractDir(file *zip.File, outputDir string) error {
	logger := logging.GetLogger()
	target := path.Join(outputDir, file.Name)
	// Strip setuid/setgid/sticky bits
	perms := file.Mode().Perm() &^ os.ModeSetuid &^ os.ModeSetgid &^ os.ModeSticky
	logger.Debug().Str("target", target).Str("perms", fmt.Sprintf("%o", perms)).Msg("Unzip: directory")
	info, err := os.Stat(target)
	if err == nil && !info.IsDir() {
		return fmt.Errorf("error creating directory: %s already exists and is not a directory", target)
	}
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("error creating directory: %w", err)
	}
	if os.IsNotExist(err) {
		err := os.MkdirAll(target, perms)
		if err != nil {
			return fmt.Errorf("error creating directory: %w", err)
		}
	} else {
		err := os.Chmod(target, perms)
		if err != nil {
			return fmt.Errorf("error changing directory permissions: %w", err)
		}
	}
	return nil
}

func extractFile(file *zip.File, outputDir string, overwrite bool) error {
	logger := logging.GetLogger()
	target := path.Join(outputDir, file.Name)
	targetDir := filepath.Dir(target)
	err := os.MkdirAll(targetDir, 0755)
	if err != nil {
		return fmt.Errorf("error creating directory: %w", err)
	}

	// Open the file inside the zip archive
	zipFile, err := file.Open()
	if err != nil {
		return fmt.Errorf("error opening file: %w", err)
	}
	defer zipFile.Close()

	// Create the file on the filesystem
	openFlags := os.O_WRONLY | os.O_CREATE
	if overwrite {
		openFlags |= os.O_TRUNC
	}
	// Strip setuid/gid/sticky bits.
	perms := file.Mode().Perm() &^ os.ModeSetuid &^ os.ModeSetgid &^ os.ModeSticky
	logger.Debug().Str("target", target).Str("perms", fmt.Sprintf("%o", perms)).Msg("Unzip: file")
	out, err := os.OpenFile(target, openFlags, perms)
	if err != nil {
		return fmt.Errorf("error creating file: %w", err)
	}
	defer out.Close()

	// Copy the file contents
	_, err = io.Copy(out, zipFile)
	if err != nil {
		return fmt.Errorf("error copying file: %w", err)
	}
	return nil
}
