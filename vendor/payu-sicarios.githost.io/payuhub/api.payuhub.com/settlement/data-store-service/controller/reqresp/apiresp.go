package reqresp

/*
   @author: tausif
*/

// Response used for apiControllers responses
type Response struct {
	APIResponse    interface{}
	HTTPStatusCode int
}

// TextResponse contains string field.
type TextResponse struct {
	Message    string `json:"message,omitempty"`
	CipherText string `json:"cipherText,omitempty"`
	Location   string `json:"location,omitempty"`
}

// PrepareResponse prepares final response body
func PrepareResponse(apiResponse interface{}, httpStatusCode int) *Response {
	return &Response{
		APIResponse:    apiResponse,
		HTTPStatusCode: httpStatusCode,
	}
}
