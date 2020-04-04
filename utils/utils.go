package utils

import "log"

// CheckErr ...logs fatal err
func CheckErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
