package stringutil

/*
   @author: vikram
*/

import "strings"

// IsEmpty check whether a string is empty.
// It takes string.
// It returns bool, return true if string is empty else false
func IsEmpty(str string) bool {
	if strings.TrimSpace(str) == "" {
		return true
	}
	return false
}

// IsNotEmpty check whether a string is not empty.
// It takes string.
// It returns bool, return true if string is not empty else false
func IsNotEmpty(str string) bool {
	return !IsEmpty(str)
}
