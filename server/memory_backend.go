package server

import (
	"io"
	"log"
	"sync"
)

type MemoryOperation struct {
	Kind   string
	Offset int64
	Length int
}

type MemoryBackend struct {
	ExportName string

	memory     []byte
	operations []MemoryOperation
	lock       sync.Mutex
}

func NewMemoryBackend(exportName string, size int64) *MemoryBackend {
	return &MemoryBackend{
		ExportName: exportName,
		memory:     make([]byte, size),
	}
}

func (b *MemoryBackend) ReadAt(p []byte, off int64) (n int, err error) {
	b.lock.Lock()
	defer b.lock.Unlock()

	b.recordOperationLocked("read", off, len(p))

	if off >= int64(len(b.memory)) {
		return 0, io.EOF
	}

	n = copy(p, b.memory[off:])
	if n < len(p) {
		return n, io.EOF
	}

	return n, nil
}

func (b *MemoryBackend) WriteAt(p []byte, off int64) (n int, err error) {
	b.lock.Lock()
	defer b.lock.Unlock()

	b.recordOperationLocked("write", off, len(p))

	if off >= int64(len(b.memory)) {
		return 0, io.EOF
	}

	n = copy(b.memory[off:], p)
	if n < len(p) {
		return n, io.ErrShortWrite
	}

	return n, nil
}

func (b *MemoryBackend) Size() (int64, error) {
	return int64(len(b.memory)), nil
}

func (b *MemoryBackend) Sync() error {
	b.lock.Lock()
	defer b.lock.Unlock()

	b.recordOperationLocked("sync", 0, 0)

	return nil
}

func (b *MemoryBackend) Operations() []MemoryOperation {
	b.lock.Lock()
	defer b.lock.Unlock()

	operations := make([]MemoryOperation, len(b.operations))
	copy(operations, b.operations)

	return operations
}

func (b *MemoryBackend) recordOperationLocked(kind string, offset int64, length int) {
	b.operations = append(b.operations, MemoryOperation{
		Kind:   kind,
		Offset: offset,
		Length: length,
	})

	log.Printf("export=%s op=%s offset=%d length=%d", b.ExportName, kind, offset, length)
}
