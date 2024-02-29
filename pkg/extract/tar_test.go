package extract

import (
	"archive/tar"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateLinks(t *testing.T) {
	tests := []struct {
		name                  string
		links                 []*link
		expectedError         bool
		overwrite             bool
		createFileToOverwrite bool
	}{
		{
			name:  "EmptyLink",
			links: []*link{},
		},
		{
			name:  "ValidHardLink",
			links: []*link{{tar.TypeLink, "", "testLinkHard"}},
		},
		{
			name:  "ValidSymlink",
			links: []*link{{tar.TypeSymlink, "", "testLinkSym"}},
		},
		{
			name:          "InvalidLinkType",
			links:         []*link{{'!', "", "x"}},
			expectedError: true,
		},
		{
			name: "ValidMultipleLinks",
			links: []*link{
				{tar.TypeLink, "", "testLinkHard"},
				{tar.TypeSymlink, "", "testLinkSym"},
			},
		},
		{
			name:                  "HardLink_OverwriteEnabled_File Exists",
			links:                 []*link{{tar.TypeLink, "", "testLinkHard"}},
			overwrite:             true,
			createFileToOverwrite: true,
		},
		{
			name:                  "HardLink_OverwriteDisabled_FileExists",
			links:                 []*link{{tar.TypeLink, "", "testLinkHard"}},
			createFileToOverwrite: true,
			expectedError:         true,
		},
		{
			name:      "HardLink_OverwriteEnabled_FileDoesNotExist",
			links:     []*link{{tar.TypeLink, "", "testLinkHard"}},
			overwrite: true,
		},
		{
			name:                  "SymLink_OverwriteEnabled_FileExists",
			links:                 []*link{{tar.TypeSymlink, "", "testLinkSym"}},
			overwrite:             true,
			createFileToOverwrite: true,
		},
		{
			name:                  "SymLink_OverwriteDisabled_FileExists",
			links:                 []*link{{tar.TypeSymlink, "", "testLinkSym"}},
			createFileToOverwrite: true,
			expectedError:         true,
		},
		{
			name:      "SymLink_OverwriteEnabled_FileDoesNotExist",
			links:     []*link{{tar.TypeSymlink, "", "testLinkSym"}},
			overwrite: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup
			destDir, err := os.MkdirTemp("./", tt.name)
			if err != nil {
				t.Fatal(err)
			}
			// Cleanup
			defer os.RemoveAll(destDir)

			// For hardlink and symlink, create dummy files
			for _, link := range tt.links {
				if link.linkType == tar.TypeLink || link.linkType == tar.TypeSymlink {
					testFile, err := os.CreateTemp(destDir, "test-")
					if tt.createFileToOverwrite {
						_, err = os.Create(filepath.Join(destDir, link.newName))
					}
					if err != nil {
						t.Fatalf("Test failed, could not create test file: %v", err)
					}
					_ = testFile.Close()
					link.oldName = filepath.Base(testFile.Name())
					link.newName = filepath.Join(destDir, link.newName)
				}
			}

			err = createLinks(tt.links, destDir, tt.overwrite)

			// Validation
			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)

				for _, link := range tt.links {
					oldPath := filepath.Join(destDir, link.oldName)
					if link.linkType == tar.TypeSymlink {
						assertSymlinkTarget(t, oldPath, link.newName)
					} else if link.linkType == tar.TypeLink {
						assertHardLinkTarget(t, oldPath, link.newName)
					} else {
						t.Fatal("Invalid link type")
					}
				}
			}

		})
	}
}

func assertHardLinkTarget(t *testing.T, oldName, newName string) {
	fileStat, err := os.Stat(oldName)
	if !assert.NoError(t, err) {
		t.Fatal("Test failed, could not stat test-created file", err)
	}
	linkStat, err := os.Lstat(newName)
	if !assert.NoError(t, err) {
		t.Fatalf("Test failed, could not stat link %s: %v", newName, err)
	}
	targetStat, err := os.Stat(newName)
	if !assert.NoError(t, err) {
		t.Fatalf("Test failed, could not stat link %s: %v", newName, err)
	}
	assert.True(t, linkStat.Mode()&os.ModeSymlink == 0)
	assert.Equal(t, fileStat.Sys().(*syscall.Stat_t).Ino, targetStat.Sys().(*syscall.Stat_t).Ino)
}

func assertSymlinkTarget(t *testing.T, oldName, newName string) {
	fileStat, err := os.Stat(oldName)
	if !assert.NoError(t, err) {
		t.Fatal("Test failed, could not stat test-created file", err)
	}
	linkStat, err := os.Lstat(newName)
	if !assert.NoError(t, err) {
		t.Fatalf("Test failed, could not stat link %s: %v", newName, err)
	}
	assert.True(t, linkStat.Mode()&os.ModeSymlink != 0)
	// os.Stat follows symlinks
	realTarget, err := os.Stat(newName)
	if !assert.NoError(t, err) {
		t.Fatalf("Test failed, could not stat link %s: %v", newName, err)
	}
	assert.Equal(t, fileStat.Sys().(*syscall.Stat_t).Ino,
		realTarget.Sys().(*syscall.Stat_t).Ino)
}

func TestGuardAgainstZipSlip(t *testing.T) {
	tests := []struct {
		description   string
		header        *tar.Header
		destDir       string
		expectedError string
	}{
		{
			description: "valid file path within directory",
			header: &tar.Header{
				Name: "valid_file",
			},
			destDir:       "/tmp/valid_dir",
			expectedError: "",
		},
		{
			description: "file path outside directory",
			header: &tar.Header{
				Name: "../invalid_file",
			},
			destDir:       "/tmp/valid_dir",
			expectedError: "archive (tar) file contains file (/tmp/invalid_file) outside of target directory: ",
		},
		{
			description: "directory traversal with invalid file",
			header: &tar.Header{
				Name: "./../../tmp/invalid_dir/invalid_file",
			},
			destDir:       "/tmp/valid_dir",
			expectedError: "archive (tar) file contains file (/tmp/invalid_dir/invalid_file) outside of target directory: ",
		},
		{
			description: "Empty header name",
			header: &tar.Header{
				Name: "",
			},
			destDir:       "/tmp",
			expectedError: "tar file contains entry with empty name",
		},
	}

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			err := guardAgainstZipSlip(test.header, test.destDir)
			if test.expectedError != "" {
				if assert.Error(t, err) {
					assert.Contains(t, err.Error(), test.expectedError)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
func TestCleanFileMode(t *testing.T) {
	testCases := []struct {
		name     string
		input    os.FileMode
		expected os.FileMode
	}{
		{
			name:     "TestWithoutStickyBit",
			input:    0755,
			expected: 0755,
		},
		{
			name:     "TestWithStickyBit",
			input:    os.ModeSticky | 0755,
			expected: 0755,
		},
		{
			name:     "TestWithoutSetuidBit",
			input:    0600,
			expected: 0600,
		},
		{
			name:     "TestWithSetuidBit",
			input:    os.ModeSetuid | 0600,
			expected: 0600,
		},
		{
			name:     "TestWithoutSetgidBit",
			input:    0777,
			expected: 0777,
		},
		{
			name:     "TestWithSetgidBit",
			input:    os.ModeSetgid | 0777,
			expected: 0777,
		},
		{
			name:     "TestWithAllBits",
			input:    os.ModeSticky | os.ModeSetuid | os.ModeSetgid | 0777,
			expected: 0777,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := cleanFileMode(tc.input)
			if result != tc.expected {
				t.Errorf("cleanFileMode() = %v, want %v", result, tc.expected)
			}
		})
	}
}
