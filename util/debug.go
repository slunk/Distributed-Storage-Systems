package util

import (
	"log"
)

var debug = false

func SetDebug(shouldDebug bool) {
	debug = shouldDebug
}

func P_out(s string, args ...interface{}) {
	if !debug {
		return
	}
	log.Printf(s, args...)
}

func P_err(s string, args ...interface{}) {
	log.Printf(s, args...)
}