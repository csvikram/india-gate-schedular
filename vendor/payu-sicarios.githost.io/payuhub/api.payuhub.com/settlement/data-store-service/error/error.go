package error

/*
   @author: tausif
   This file has implementation of error handling of API's in this project.
*/

import (
	"encoding/json"
)

// APIError represents the error response body for the API's.
type APIError struct {
	ErrType string      `json:"type"`
	ErrDesc interface{} `json:"description"`
	ErrInfo string      `json:"more_info,omitempty"`
}

// New represents a new error invoked wherever it is used.
// It returns pointer to APIError.
func New(errType string, errDesc interface{}, errInfo string) *APIError {
	return &APIError{errType, errDesc, errInfo}
}

// Error is standard interface and APIError struct is resolver attached to it.
// Below is our own implementation and it can be different for different repo.
// It returns string which is basically a json marshaled from APIError struct.
func (apiErr *APIError) Error() string {
	result, _ := json.Marshal(apiErr)
	return string(result)
}
