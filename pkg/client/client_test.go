package client_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/replicate/pget/pkg/client"
)

func TestGetSchemeHostKey(t *testing.T) {
	expected := "http://example.com"
	actual, err := client.GetSchemeHostKey("http://example.com/foo/bar;baz/quux?animal=giraffe")

	assert.NoError(t, err)
	assert.Equal(t, expected, actual)
}
