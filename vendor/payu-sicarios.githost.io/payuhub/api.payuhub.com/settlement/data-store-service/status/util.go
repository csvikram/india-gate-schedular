package status

/*
   @author: tausif
*/

// Is4xx check is statusCode is of 4xx series.
// It takes a int,
// It returns bool, true if statusCode is of 4xx series else false
func Is4xx(statusCode int) bool {

	if statusCode/100 == 4 {
		return true
	}
	return false
}

// Is5xx check is statusCode is of 4xx series.
// It takes a int,
// It returns bool, true if statusCode is of 5xx series else false
func Is5xx(statusCode int) bool {

	if statusCode/100 == 5 {
		return true
	}
	return false
}
