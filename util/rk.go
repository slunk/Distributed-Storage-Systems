package util

import (
)

type RkChunker struct {
    hash_len uint64
    prime uint64
    min_chunk uint64
    target_chunk uint64
    max_chunk uint64
    b_n uint64
    saved [256]uint64
}

func DefaultChunker() *RkChunker {
    return (&RkChunker{
        hash_len: 32,
        prime: 31,
        min_chunk: 2048,
        target_chunk: 4096,
        max_chunk: 8192,
    }).Init()
}

func (chunker *RkChunker) Init() *RkChunker {
    chunker.b_n = 1
    for i := uint64(0); i < chunker.hash_len - 1; i++ {
        chunker.b_n *= chunker.prime
    }
    for i := uint64(0); i < 256; i++ {
        chunker.saved[i] = i * chunker.b_n
    }
    return chunker
}

// Computes a single Rabin-Karp chunk from the front of the input buffer
// Returns the first chunk and the unchunked remainder of the buffer
func (chunker *RkChunker) chunk(buffer []byte) ([]byte, []byte) {
    off := uint64(0)
    hash := uint64(0)
    length := uint64(len(buffer))
    if chunker.hash_len >= length {
        return buffer, buffer[len(buffer):]
    }
    for ; off < chunker.hash_len; off++ {
        hash = hash * chunker.prime + uint64(buffer[off])
    }
    for off < length {
        hash = (hash - chunker.saved[buffer[off - chunker.hash_len]]) * chunker.prime + uint64(buffer[off])
        off++
        if ((off >= chunker.min_chunk) && ((hash % chunker.target_chunk) == 1)) || (off >= chunker.max_chunk) {
            return buffer[:off], buffer[off:]
        }
    }
    return buffer[:off], buffer[off:]
}

// Computes and returns all Rabin-Karp chunks of the input buffer
func (chunker *RkChunker) Chunks(buffer []byte) [][]byte {
    chunks := make([][]byte, 0, 20)
    curr_chunk := []byte(nil)
    tmp := buffer
    for i := 0; len(tmp) != 0; i++ {
        curr_chunk, tmp = chunker.chunk(tmp)
        if i >= cap(chunks) {
            tmp := chunks
            chunks = make([][]byte, i, 2 * cap(chunks))
            copy(chunks, tmp)
        } else {
            chunks = chunks[:i+1]
        }
        chunks[i] = curr_chunk
    }
    return chunks
}

//func main() {
//    chunker := DefaultChunker()
//    if buffer, err := ioutil.ReadFile("testfile"); err == nil {
//        chunks := chunker.Chunks(buffer)
//        offset := 0
//        for idx, element := range chunks {
//            fmt.Println("Chunk: ", idx, " Offset: ", offset, " Length: ", len(element))
//            offset += len(element)
//        }
        //fmt.Println(chunks)
//    }
//}
