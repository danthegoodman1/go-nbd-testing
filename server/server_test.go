package server

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	nbdbackend "github.com/pojntfx/go-nbd/pkg/backend"
	"github.com/pojntfx/go-nbd/pkg/protocol"
)

func TestHandleUsesDifferentBackendsPerExport(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()

	var (
		backendsMu sync.Mutex
		backends   = map[string]*MemoryBackend{}
		handlers   sync.WaitGroup
		serverErrs = make(chan error, 8)
		stopServer = make(chan struct{})
	)

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				select {
				case <-stopServer:
					return
				default:
					serverErrs <- err
					return
				}
			}

			handlers.Add(1)
			go func(conn net.Conn) {
				defer handlers.Done()
				defer conn.Close()

				err := Handle(conn, func(info ConnInfo) (nbdbackend.Backend, error) {
					backend := NewMemoryBackend(info.ExportName, 64)

					backendsMu.Lock()
					backends[info.ExportName] = backend
					backendsMu.Unlock()

					return backend, nil
				}, &Options{
					ExportDescription: "test export",
					SupportsMultiConn: true,
				})
				if err != nil && !errors.Is(err, net.ErrClosed) {
					serverErrs <- err
				}
			}(conn)
		}
	}()

	type clientResult struct {
		exportName string
		payload    []byte
		readBack   []byte
		err        error
	}

	ready := make(chan struct{}, 2)
	startWrites := make(chan struct{})
	results := make(chan clientResult, 2)

	runClient := func(exportName string, payload []byte) {
		client, err := dialTestClient(listener.Addr().String(), exportName)
		if err != nil {
			results <- clientResult{exportName: exportName, err: err}
			return
		}
		defer client.Close()

		ready <- struct{}{}
		<-startWrites

		// Both clients write the same range concurrently so shared-backend routing
		// would collapse them onto one final value.
		if err := client.WriteAt(payload, 8); err != nil {
			results <- clientResult{exportName: exportName, err: err}
			return
		}

		readBack, err := client.ReadAt(len(payload), 8)
		if err != nil {
			results <- clientResult{exportName: exportName, err: err}
			return
		}

		if err := client.Disconnect(); err != nil {
			results <- clientResult{exportName: exportName, err: err}
			return
		}

		results <- clientResult{
			exportName: exportName,
			payload:    payload,
			readBack:   readBack,
		}
	}

	go runClient("tenant-a", []byte("AAAA"))
	go runClient("tenant-b", []byte("BBBB"))

	for range 2 {
		<-ready
	}
	close(startWrites)

	resultA := <-results
	resultB := <-results

	close(stopServer)
	_ = listener.Close()
	handlers.Wait()

	for _, result := range []clientResult{resultA, resultB} {
		if result.err != nil {
			t.Fatalf("client %s: %v", result.exportName, result.err)
		}

		if !bytes.Equal(result.payload, result.readBack) {
			t.Fatalf("client %s read back %q, want %q", result.exportName, result.readBack, result.payload)
		}
	}

	backendsMu.Lock()
	backendA := backends["tenant-a"]
	backendB := backends["tenant-b"]
	backendsMu.Unlock()

	if backendA == nil || backendB == nil {
		t.Fatalf("expected backends for both exports, got %v", mapsKeys(backends))
	}

	if backendA.ExportName != "tenant-a" || backendB.ExportName != "tenant-b" {
		t.Fatalf("unexpected backend export names: %q %q", backendA.ExportName, backendB.ExportName)
	}

	for _, backend := range []*MemoryBackend{backendA, backendB} {
		operations := backend.Operations()
		if !hasOperation(operations, "write", 8, 4) {
			t.Fatalf("expected write operation for %s, got %#v", backend.ExportName, operations)
		}

		if !hasOperation(operations, "read", 8, 4) {
			t.Fatalf("expected read operation for %s, got %#v", backend.ExportName, operations)
		}

		if !hasOperation(operations, "sync", 0, 0) {
			t.Fatalf("expected sync operation for %s, got %#v", backend.ExportName, operations)
		}
	}

	select {
	case err := <-serverErrs:
		t.Fatalf("server error: %v", err)
	default:
	}
}

type testClient struct {
	conn       net.Conn
	nextHandle uint64
}

func dialTestClient(addr string, exportName string) (*testClient, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("error in net.Dial: %w", err)
	}

	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("error in SetDeadline: %w", err)
	}

	if err := negotiateTestClient(conn, exportName); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("error in negotiateTestClient: %w", err)
	}

	return &testClient{
		conn:       conn,
		nextHandle: 1,
	}, nil
}

func (c *testClient) WriteAt(payload []byte, offset uint64) error {
	handle := c.handle()

	if err := binary.Write(c.conn, binary.BigEndian, protocol.TransmissionRequestHeader{
		RequestMagic: protocol.TRANSMISSION_MAGIC_REQUEST,
		Type:         protocol.TRANSMISSION_TYPE_REQUEST_WRITE,
		Handle:       handle,
		Offset:       offset,
		Length:       uint32(len(payload)),
	}); err != nil {
		return fmt.Errorf("error in writeWriteRequestHeader: %w", err)
	}

	if _, err := c.conn.Write(payload); err != nil {
		return fmt.Errorf("error in writeWriteRequestPayload: %w", err)
	}

	if err := readTransmissionReply(c.conn, handle); err != nil {
		return fmt.Errorf("error in readWriteReply: %w", err)
	}

	return nil
}

func (c *testClient) ReadAt(length int, offset uint64) ([]byte, error) {
	handle := c.handle()

	if err := binary.Write(c.conn, binary.BigEndian, protocol.TransmissionRequestHeader{
		RequestMagic: protocol.TRANSMISSION_MAGIC_REQUEST,
		Type:         protocol.TRANSMISSION_TYPE_REQUEST_READ,
		Handle:       handle,
		Offset:       offset,
		Length:       uint32(length),
	}); err != nil {
		return nil, fmt.Errorf("error in writeReadRequestHeader: %w", err)
	}

	if err := readTransmissionReply(c.conn, handle); err != nil {
		return nil, fmt.Errorf("error in readReadReplyHeader: %w", err)
	}

	payload := make([]byte, length)
	if _, err := io.ReadFull(c.conn, payload); err != nil {
		return nil, fmt.Errorf("error in readReadReplyPayload: %w", err)
	}

	return payload, nil
}

func (c *testClient) Disconnect() error {
	handle := c.handle()

	if err := binary.Write(c.conn, binary.BigEndian, protocol.TransmissionRequestHeader{
		RequestMagic: protocol.TRANSMISSION_MAGIC_REQUEST,
		Type:         protocol.TRANSMISSION_TYPE_REQUEST_DISC,
		Handle:       handle,
	}); err != nil {
		return fmt.Errorf("error in writeDisconnectRequestHeader: %w", err)
	}

	return nil
}

func (c *testClient) Close() error {
	return c.conn.Close()
}

func (c *testClient) handle() uint64 {
	handle := c.nextHandle
	c.nextHandle++

	return handle
}

func negotiateTestClient(conn net.Conn, exportName string) error {
	var header protocol.NegotiationNewstyleHeader
	if err := binary.Read(conn, binary.BigEndian, &header); err != nil {
		return fmt.Errorf("error in readNegotiationHeader: %w", err)
	}

	if header.OldstyleMagic != protocol.NEGOTIATION_MAGIC_OLDSTYLE || header.OptionMagic != protocol.NEGOTIATION_MAGIC_OPTION {
		return ErrInvalidMagic
	}

	if _, err := conn.Write(make([]byte, 4)); err != nil {
		return fmt.Errorf("error in writeClientFlags: %w", err)
	}

	exportNameRaw := []byte(exportName)
	optionLength := uint32(4 + len(exportNameRaw) + 2)

	if err := binary.Write(conn, binary.BigEndian, protocol.NegotiationOptionHeader{
		OptionMagic: protocol.NEGOTIATION_MAGIC_OPTION,
		ID:          protocol.NEGOTIATION_ID_OPTION_GO,
		Length:      optionLength,
	}); err != nil {
		return fmt.Errorf("error in writeGoOptionHeader: %w", err)
	}

	if err := binary.Write(conn, binary.BigEndian, uint32(len(exportNameRaw))); err != nil {
		return fmt.Errorf("error in writeExportNameLength: %w", err)
	}

	if _, err := conn.Write(exportNameRaw); err != nil {
		return fmt.Errorf("error in writeExportName: %w", err)
	}

	if err := binary.Write(conn, binary.BigEndian, uint16(0)); err != nil {
		return fmt.Errorf("error in writeInformationRequestCount: %w", err)
	}

	for {
		var replyHeader protocol.NegotiationReplyHeader
		if err := binary.Read(conn, binary.BigEndian, &replyHeader); err != nil {
			return fmt.Errorf("error in readNegotiationReplyHeader: %w", err)
		}

		if replyHeader.ReplyMagic != protocol.NEGOTIATION_MAGIC_REPLY {
			return ErrInvalidMagic
		}

		switch replyHeader.Type {
		case protocol.NEGOTIATION_TYPE_REPLY_INFO:
			payload := make([]byte, replyHeader.Length)
			if _, err := io.ReadFull(conn, payload); err != nil {
				return fmt.Errorf("error in readNegotiationReplyPayload: %w", err)
			}

			var infoType uint16
			if err := binary.Read(bytes.NewReader(payload), binary.BigEndian, &infoType); err != nil {
				return fmt.Errorf("error in readNegotiationInfoType: %w", err)
			}
		case protocol.NEGOTIATION_TYPE_REPLY_ACK:
			return nil
		case protocol.NEGOTIATION_TYPE_REPLY_ERR_UNKNOWN:
			return ErrUnknownExport
		default:
			return errors.New("unexpected negotiation reply")
		}
	}
}

func readTransmissionReply(conn net.Conn, wantHandle uint64) error {
	var replyHeader protocol.TransmissionReplyHeader
	if err := binary.Read(conn, binary.BigEndian, &replyHeader); err != nil {
		return fmt.Errorf("error in readTransmissionReplyHeader: %w", err)
	}

	if replyHeader.ReplyMagic != protocol.TRANSMISSION_MAGIC_REPLY {
		return ErrInvalidMagic
	}

	if replyHeader.Handle != wantHandle {
		return errors.New("unexpected reply handle")
	}

	if replyHeader.Error != 0 {
		return errors.New("unexpected transmission error")
	}

	return nil
}

func mapsKeys[K comparable, V any](m map[K]V) []K {
	keys := make([]K, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}

	return keys
}

func hasOperation(operations []MemoryOperation, kind string, offset int64, length int) bool {
	for _, operation := range operations {
		if operation.Kind == kind && operation.Offset == offset && operation.Length == length {
			return true
		}
	}

	return false
}
