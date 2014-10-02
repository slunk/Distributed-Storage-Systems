package test

import (
	"io/ioutil"
	"testing"
	"dss/util"
)

func TestSingleChunk(t *testing.T) {
	chunker := util.DefaultChunker()
	chunks := chunker.Chunks(make([]byte, 10))
	assert(t, len(chunks) == 1)
}

func TestChunks(t *testing.T) {
	expectedChunkSizes := []int{7141, 6482, 7778, 8192, 7500, 8192, 2101, 7995, 4888, 8192, 8192, 8111}
	if buffer, err := ioutil.ReadFile("input/testfile"); err == nil {
		chunker := util.DefaultChunker()
		chunks := chunker.Chunks(buffer)
		assertWithMsg(t, len(chunks) == 12, "Expected 12 chunks.")
		for i, expected := range expectedChunkSizes {
			assertWithMsg(t, len(chunks[i]) == expected, 
				"Expected chunk " + string(i) + " to be " + string(expected) + " bytes long.")
		}
	} else {
		t.Logf("Couldn't read input file.")
		t.Fail()
	}
}