package test

import (
	"io/ioutil"
	"testing"
	"dss/util"
)

func TestSingleChunk(t *testing.T) {
	chunker := util.DefaultChunker()
	chunks := chunker.Chunks(make([]byte, 10))
	if len(chunks) != 1 {
		t.Fail()
	}
}

func TestChunks(t *testing.T) {
	expectedChunkSizes := []int{7141, 6482, 7778, 8192, 7500, 8192, 2101, 7995, 4888, 8192, 8192, 8111}
	if buffer, err := ioutil.ReadFile("input/testfile"); err == nil {
		chunker := util.DefaultChunker()
		chunks := chunker.Chunks(buffer)
		if len(chunks) != 12 {
			t.Logf("Expected 12 chunks.")
			t.FailNow()
		}
		for i, expected := range expectedChunkSizes {
			if len(chunks[i]) != expected {
				t.Logf("Expected chunk %d to be %d bytes long.", i, expected)
				t.FailNow()
			}
		}
		if len(chunks[0]) != 7141 {
			t.Fail()
		}
	} else {
		t.Logf("Couldn't read input file.")
		t.Fail()
	}
}