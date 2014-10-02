package test

import (
	"testing"
)

func assert(t *testing.T, cond bool) {
	if !cond {
		t.FailNow()
	}
}

func assertWithMsg(t *testing.T, cond bool, msg string) {
	if !cond {
		t.Logf(msg)
		t.FailNow()
	}
}

func assertFalse(t *testing.T, cond bool) {
	assert(t, !cond)
}

func assertFalseWithMsg(t *testing.T, cond bool, msg string) {
	assertWithMsg(t, !cond, msg)
}