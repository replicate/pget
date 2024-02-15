package extract

import (
	"archive/zip"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

// ZipFile extracts a zip file to the given destination path.
func ZipFile(reader io.ReaderAt, destPath string, size int64) error {
	err := os.MkdirAll(destPath, 0755)
	if err != nil {
		return fmt.Errorf("error creating destination directory: %w", err)
	}

	zipReader, err := zip.NewReader(reader, size)
	if err != nil {
		return fmt.Errorf("error creating zip reader: %w", err)
	}

	for _, file := range zipReader.File {
		err := handleFileFromZip(file, destPath)
		if err != nil {
			return fmt.Errorf("error extracting file: %w", err)
		}
	}
	return nil
}

func handleFileFromZip(file *zip.File, outputDir string) error {
	target := outputDir + file.Name
	targetDir := filepath.Dir(target)
	if file.FileInfo().IsDir() {
		return extractDir(file, targetDir)
	} else if file.FileInfo().Mode().IsRegular() {
		return extractFile(file, targetDir)
	} else {
		return fmt.Errorf("unsupported file type (not dir or regular): %s (%d)", file.Name, file.FileInfo().Mode().Type())
	}

}

func extractDir(file *zip.File, outputDir string) error {
	target := outputDir + file.Name
	err := os.MkdirAll(target, file.Mode().Perm())
	if err != nil {
		return fmt.Errorf("error creating directory: %w", err)
	}
	return applyPermissions(target, file.Mode().Perm())
}

func extractFile(file *zip.File, outputDir string) error {
	target := outputDir + file.Name
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
	out, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, file.Mode())
	if err != nil {
		return fmt.Errorf("error creating file: %w", err)
	}
	defer out.Close()

	// Copy the file contents
	_, err = io.Copy(out, zipFile)
	if err != nil {
		return fmt.Errorf("error copying file: %w", err)
	}
	return applyPermissions(target, file.Mode().Perm())
}

func applyPermissions(filepath string, fileMode fs.FileMode) error {
	// Do not apply setuid/gid/sticky bits.
	perms := fileMode &^ os.ModeSetuid &^ os.ModeSetgid &^ os.ModeSticky
	return os.Chmod(filepath, perms)
}
