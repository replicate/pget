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
		name          string
		links         []*link
		expectedError bool
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
					if err != nil {
						t.Fatalf("Test failed, could not create test file: %v", err)
					}
					_ = testFile.Close()
					link.oldName = filepath.Base(testFile.Name())
					link.newName = filepath.Join(destDir, link.newName)
				}
			}

			err = createLinks(tt.links, destDir)

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
