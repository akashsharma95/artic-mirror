package storage

import (
	"bytes"
	"io"
	"sync"
)

type Buffer struct {
	buf  *bytes.Buffer
	size int64
	mu   sync.Mutex
}

func NewBuffer() *Buffer {
	return &Buffer{
		buf: bytes.NewBuffer(nil),
	}
}

func (b *Buffer) Write(p []byte) (n int, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	n, err = b.buf.Write(p)
	b.size += int64(n)
	return
}

func (b *Buffer) Size() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.size
}

func (b *Buffer) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf.Reset()
	b.size = 0
}

func (b *Buffer) Reader() io.Reader {
	b.mu.Lock()
	defer b.mu.Unlock()
	return bytes.NewReader(b.buf.Bytes())
}
