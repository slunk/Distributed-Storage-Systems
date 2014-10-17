//
package util

import (
	"time"
)

type Vid struct {
	Sid, Index int
}

type Node struct {
	Name  string
	Nid   uint64
	Vid   Vid
	Dirty bool
	Kids  map[string]Vid
	Data  []uint8
}

func ParseTime(s string) (time.Time, bool) {
	timeFormats := []string{"2006-1-2 15:04:05", "2006-1-2 15:04", "2006-1-2", "1-2-2006 15:04:05",
		"1-2-2006 15:04", "1-6-2006", "2006/1/2 15:04:05", "2006/1/2 15:04", "2006/1/2",
		"1/2/2006 15:04:05", "1/2/2006 15:04", "1/2/2006"}
	loc, _ := time.LoadLocation("Local")

	for _, v := range timeFormats {
		if tm, terr := time.ParseInLocation(v, s, loc); terr == nil {
			return tm, false
		}
	}
	return time.Time{}, true
}
