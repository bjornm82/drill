package drill

import "fmt"

const (
	msgErrorFmt = "client: (%s: %s) failed with error code %d and message %s"
)

func newError(errorCode int, method, uri, message string) ResourceError {
	return ResourceError{
		ErrorCode: errorCode,
		Method:    method,
		URI:       uri,
		Message:   message,
	}
}

// ResourceError is being fired from all API calls when an error code is received.
type ResourceError struct {
	ErrorCode int    `json:"error_code"`
	Method    string `json:"method,omitempty"`
	URI       string `json:"uri,omitempty"`
	Message   string `json:"message,omitempty"`
}

func (err ResourceError) Error() string {
	return fmt.Sprintf(msgErrorFmt,
		err.Method, err.URI, err.ErrorCode, err.Message)
}
