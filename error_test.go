package drill

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	statusCode = 200
	method     = "GET"
	uri        = "http://any-url:8081/"
	message    = "some-message for sending back"
)

func TestNewError(t *testing.T) {
	err := newError(statusCode, method, uri, message)
	assert.Equal(t, statusCode, err.ErrorCode)
	assert.Equal(t, method, err.Method)
	assert.Equal(t, uri, err.URI)
	assert.Equal(t, message, err.Message)

	assert.Equal(t, fmt.Sprintf(msgErrorFmt, method, uri, statusCode, message), err.Error())
}
