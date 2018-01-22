package error

/*
   @author: tausif
*/

// This file declares all the error type to be returned in the API response.
const (
	APIRequestError = "api_request_error" // Represents bad API request or invalid API request param value.

	APIAuthenticationError = "api_authentication_error" // Represents unauthenticated or unauthorized API request.

	APINetworkError = "api_network_error" // Represents error occured while calling our sister services.

	APIServiceError = "api_service_error" // Represents if one of the third party service being used is down.

	APIProcessingError = "api_processing_error" // Represents any unknown processing error in our system.

)
