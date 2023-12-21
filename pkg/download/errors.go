package download

import (
	"fmt"
)

type HttpStatusError struct {
	StatusCode int
}

func ErrUnexpectedHTTPStatus(statusCode int) error {
	return HttpStatusError{StatusCode: statusCode}
}

var _ error = &HttpStatusError{}

func (c HttpStatusError) Error() string {
	return fmt.Sprintf("Status code %d", c.StatusCode)
}
