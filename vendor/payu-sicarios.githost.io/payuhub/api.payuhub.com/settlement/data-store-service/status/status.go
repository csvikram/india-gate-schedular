package status

/*
   @author: tausif
   This file has HTTP code description.
*/

// HTTP statusCode
const (
	OK        = 200 // RFC 7231, 6.3.1
	Created   = 201 // RFC 7231, 6.3.2
	Accepted  = 202 // RFC 7231, 6.3.3
	NoContent = 204 // RFC 7231, 6.3.5

	BadRequest          = 400 // RFC 7231, 6.5.1
	Unauthorized        = 401 // RFC 7235, 3.1
	Forbidden           = 403 // RFC 7231, 6.5.3
	NotFound            = 404 // RFC 7231, 6.5.4
	UnProcessableEntity = 422 // RFC 4918, 11.2
	TooManyRequests     = 429 // RFC 6585, 4

	InternalServerError = 500 // RFC 7231, 6.6.1
	BadGateway          = 502 // RFC 7231, 6.6.3
	ServiceUnavailable  = 503 // RFC 7231, 6.6.4
)

var statusDescMap = map[int]string{

	OK: "OK",

	Created: "Created",

	Accepted: "The request is accepted for processing, but processing has not completed.",

	NoContent: "No Content",

	BadRequest: "The request is invalid. The request method or content-type may be incorrect",

	Unauthorized: "Authentication is required to fulfill the request." +
		" The credentials provided were either incorrect or were not provided",

	Forbidden: "The request cannot be fulfilled. Additional user privileges may be needed",

	NotFound: "The requested resource could not be found",

	UnProcessableEntity: "No configuration could be returned as the specified merchant does not exist",

	TooManyRequests: "Request count of the user exceeds specified limit",

	InternalServerError: "Internal Server Error",

	BadGateway: "Bad Gateway",

	ServiceUnavailable: "Service Unavailable",
}

// Desc returns a text for the HTTP status code. It returns the empty
// string if the code is unknown.
func Desc(code int) string {
	return statusDescMap[code]
}
