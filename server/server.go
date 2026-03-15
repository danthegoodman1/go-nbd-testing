package server

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"

	nbdbackend "github.com/pojntfx/go-nbd/pkg/backend"
	"github.com/pojntfx/go-nbd/pkg/protocol"
)

var (
	ErrInvalidMagic           = errors.New("invalid magic")
	ErrInvalidBlocksize       = errors.New("invalid blocksize")
	ErrBackendFactoryRequired = errors.New("backend factory required")
	ErrNilBackend             = errors.New("backend factory returned nil backend")
	ErrUnknownExport          = errors.New("unknown export")
)

const (
	defaultMaximumRequestSize = 32 * 1024 * 1024
)

type ConnInfo struct {
	Conn       net.Conn
	LocalAddr  net.Addr
	RemoteAddr net.Addr
	ExportName string
}

type BackendFactory func(info ConnInfo) (nbdbackend.Backend, error)

type Options struct {
	ReadOnly bool

	MinimumBlockSize   uint32
	PreferredBlockSize uint32
	MaximumBlockSize   uint32

	MaximumRequestSize int
	SupportsMultiConn  bool
	ExportDescription  string
}

func Handle(conn net.Conn, backendFactory BackendFactory, options *Options) (err error) {
	if backendFactory == nil {
		return ErrBackendFactoryRequired
	}

	options = normalizeOptions(options)

	var (
		activeBackend nbdbackend.Backend
		activeInfo    ConnInfo
	)

	defer func() {
		if activeBackend == nil {
			return
		}

		err = errors.Join(err, closeBackend(activeBackend))
	}()

	if err := writeNewstyleHeader(conn); err != nil {
		return fmt.Errorf("error in writeNewstyleHeader: %w", err)
	}

	if _, err := io.CopyN(io.Discard, conn, 4); err != nil {
		return fmt.Errorf("error in discardClientFlags: %w", err)
	}

negotiation:
	for {
		var optionHeader protocol.NegotiationOptionHeader
		if err := binary.Read(conn, binary.BigEndian, &optionHeader); err != nil {
			return fmt.Errorf("error in readNegotiationOptionHeader: %w", err)
		}

		if optionHeader.OptionMagic != protocol.NEGOTIATION_MAGIC_OPTION {
			return ErrInvalidMagic
		}

		switch optionHeader.ID {
		case protocol.NEGOTIATION_ID_OPTION_INFO, protocol.NEGOTIATION_ID_OPTION_GO:
			info, err := readConnInfo(conn)
			if err != nil {
				return fmt.Errorf("error in readConnInfo: %w", err)
			}

			requestCount, err := readInformationRequestCount(conn)
			if err != nil {
				return fmt.Errorf("error in readInformationRequestCount: %w", err)
			}

			if err := discardInformationRequests(conn, requestCount); err != nil {
				return fmt.Errorf("error in discardInformationRequests: %w", err)
			}

			backend, err := resolveBackend(activeBackend, activeInfo, info, backendFactory)
			if err != nil {
				if errors.Is(err, ErrUnknownExport) {
					if err := writeNegotiationReplyHeader(conn, optionHeader.ID, protocol.NEGOTIATION_TYPE_REPLY_ERR_UNKNOWN, 0); err != nil {
						return fmt.Errorf("error in writeUnknownExportReply: %w", err)
					}

					continue
				}

				return fmt.Errorf("error in resolveBackend: %w", err)
			}

			if activeBackend != backend {
				if activeBackend != nil {
					if err := closeBackend(activeBackend); err != nil {
						return fmt.Errorf("error in closeBackend: %w", err)
					}
				}

				activeBackend = backend
			}
			activeInfo = info

			size, err := activeBackend.Size()
			if err != nil {
				return fmt.Errorf("error in backend.Size: %w", err)
			}

			if err := writeExportInfoReplies(conn, optionHeader.ID, activeInfo.ExportName, options.ExportDescription, size, options); err != nil {
				return fmt.Errorf("error in writeExportInfoReplies: %w", err)
			}

			if optionHeader.ID == protocol.NEGOTIATION_ID_OPTION_GO {
				break negotiation
			}
		case protocol.NEGOTIATION_ID_OPTION_ABORT:
			if err := discardOptionPayload(conn, optionHeader.Length); err != nil {
				return fmt.Errorf("error in discardOptionPayload: %w", err)
			}

			if err := writeNegotiationReplyHeader(conn, optionHeader.ID, protocol.NEGOTIATION_TYPE_REPLY_ACK, 0); err != nil {
				return fmt.Errorf("error in writeAbortReply: %w", err)
			}

			return nil
		default:
			if err := discardOptionPayload(conn, optionHeader.Length); err != nil {
				return fmt.Errorf("error in discardOptionPayload: %w", err)
			}

			if err := writeNegotiationReplyHeader(conn, optionHeader.ID, protocol.NEGOTIATION_TYPE_REPLY_ERR_UNSUPPORTED, 0); err != nil {
				return fmt.Errorf("error in writeUnsupportedReply: %w", err)
			}
		}
	}

	if activeBackend == nil {
		return ErrUnknownExport
	}

	if err := serveTransmission(conn, activeBackend, options); err != nil {
		return fmt.Errorf("error in serveTransmission: %w", err)
	}

	return nil
}

func normalizeOptions(options *Options) *Options {
	if options == nil {
		options = &Options{
			SupportsMultiConn: true,
		}
	}

	if options.MinimumBlockSize == 0 {
		options.MinimumBlockSize = 1
	}

	if options.PreferredBlockSize == 0 {
		options.PreferredBlockSize = 4096
	}

	if options.MaximumBlockSize == 0 {
		options.MaximumBlockSize = defaultMaximumRequestSize
	}

	if options.MaximumRequestSize == 0 {
		options.MaximumRequestSize = defaultMaximumRequestSize
	}

	return options
}

func readConnInfo(conn net.Conn) (ConnInfo, error) {
	var exportNameLength uint32
	if err := binary.Read(conn, binary.BigEndian, &exportNameLength); err != nil {
		return ConnInfo{}, fmt.Errorf("error in readExportNameLength: %w", err)
	}

	exportName := make([]byte, exportNameLength)
	if _, err := io.ReadFull(conn, exportName); err != nil {
		return ConnInfo{}, fmt.Errorf("error in readExportName: %w", err)
	}

	return ConnInfo{
		Conn:       conn,
		LocalAddr:  conn.LocalAddr(),
		RemoteAddr: conn.RemoteAddr(),
		ExportName: string(exportName),
	}, nil
}

func readInformationRequestCount(conn net.Conn) (uint16, error) {
	var requestCount uint16
	if err := binary.Read(conn, binary.BigEndian, &requestCount); err != nil {
		return 0, fmt.Errorf("error in readInformationRequestCountValue: %w", err)
	}

	return requestCount, nil
}

func discardInformationRequests(conn net.Conn, requestCount uint16) error {
	if requestCount == 0 {
		return nil
	}

	_, err := io.CopyN(io.Discard, conn, 2*int64(requestCount))

	if err != nil {
		return fmt.Errorf("error in discardInformationRequestValues: %w", err)
	}

	return nil
}

func resolveBackend(activeBackend nbdbackend.Backend, activeInfo ConnInfo, requestedInfo ConnInfo, backendFactory BackendFactory) (nbdbackend.Backend, error) {
	if activeBackend != nil && activeInfo.ExportName == requestedInfo.ExportName {
		return activeBackend, nil
	}

	backend, err := backendFactory(requestedInfo)
	if err != nil {
		return nil, fmt.Errorf("error in backendFactory: %w", err)
	}

	if backend == nil {
		return nil, ErrNilBackend
	}

	return backend, nil
}

func writeNewstyleHeader(conn net.Conn) error {
	return binary.Write(conn, binary.BigEndian, protocol.NegotiationNewstyleHeader{
		OldstyleMagic:  protocol.NEGOTIATION_MAGIC_OLDSTYLE,
		OptionMagic:    protocol.NEGOTIATION_MAGIC_OPTION,
		HandshakeFlags: protocol.NEGOTIATION_HANDSHAKE_FLAG_FIXED_NEWSTYLE,
	})
}

func writeExportInfoReplies(conn net.Conn, optionID uint32, exportName string, description string, size int64, options *Options) error {
	transmissionFlags := uint16(0)
	if options.SupportsMultiConn {
		transmissionFlags = protocol.NEGOTIATION_REPLY_FLAGS_HAS_FLAGS | protocol.NEGOTIATION_REPLY_FLAGS_CAN_MULTI_CONN
	}

	if err := writeInfoReply(conn, optionID, protocol.NegotiationReplyInfo{
		Type:              protocol.NEGOTIATION_TYPE_INFO_EXPORT,
		Size:              uint64(size),
		TransmissionFlags: transmissionFlags,
	}); err != nil {
		return err
	}

	nameReply := &bytes.Buffer{}
	if err := binary.Write(nameReply, binary.BigEndian, protocol.NegotiationReplyNameHeader{
		Type: protocol.NEGOTIATION_TYPE_INFO_NAME,
	}); err != nil {
		return err
	}

	if _, err := nameReply.Write([]byte(exportName)); err != nil {
		return err
	}

	if err := writeNegotiationReply(conn, optionID, protocol.NEGOTIATION_TYPE_REPLY_INFO, nameReply.Bytes()); err != nil {
		return err
	}

	descriptionReply := &bytes.Buffer{}
	if err := binary.Write(descriptionReply, binary.BigEndian, protocol.NegotiationReplyDescriptionHeader{
		Type: protocol.NEGOTIATION_TYPE_INFO_DESCRIPTION,
	}); err != nil {
		return err
	}

	if _, err := descriptionReply.Write([]byte(description)); err != nil {
		return err
	}

	if err := writeNegotiationReply(conn, optionID, protocol.NEGOTIATION_TYPE_REPLY_INFO, descriptionReply.Bytes()); err != nil {
		return err
	}

	if err := writeInfoReply(conn, optionID, protocol.NegotiationReplyBlockSize{
		Type:               protocol.NEGOTIATION_TYPE_INFO_BLOCKSIZE,
		MinimumBlockSize:   options.MinimumBlockSize,
		PreferredBlockSize: options.PreferredBlockSize,
		MaximumBlockSize:   options.MaximumBlockSize,
	}); err != nil {
		return err
	}

	return writeNegotiationReplyHeader(conn, optionID, protocol.NEGOTIATION_TYPE_REPLY_ACK, 0)
}

func writeInfoReply(conn net.Conn, optionID uint32, reply any) error {
	info := &bytes.Buffer{}
	if err := binary.Write(info, binary.BigEndian, reply); err != nil {
		return fmt.Errorf("error in encodeInfoReply: %w", err)
	}

	if err := writeNegotiationReply(conn, optionID, protocol.NEGOTIATION_TYPE_REPLY_INFO, info.Bytes()); err != nil {
		return fmt.Errorf("error in writeNegotiationReply: %w", err)
	}

	return nil
}

func writeNegotiationReply(conn net.Conn, optionID uint32, replyType uint32, payload []byte) error {
	if err := writeNegotiationReplyHeader(conn, optionID, replyType, uint32(len(payload))); err != nil {
		return fmt.Errorf("error in writeNegotiationReplyHeader: %w", err)
	}

	if len(payload) == 0 {
		return nil
	}

	_, err := conn.Write(payload)

	if err != nil {
		return fmt.Errorf("error in writeNegotiationReplyPayload: %w", err)
	}

	return nil
}

func writeNegotiationReplyHeader(conn net.Conn, optionID uint32, replyType uint32, length uint32) error {
	return binary.Write(conn, binary.BigEndian, protocol.NegotiationReplyHeader{
		ReplyMagic: protocol.NEGOTIATION_MAGIC_REPLY,
		ID:         optionID,
		Type:       replyType,
		Length:     length,
	})
}

func discardOptionPayload(conn net.Conn, length uint32) error {
	if length == 0 {
		return nil
	}

	_, err := io.CopyN(io.Discard, conn, int64(length))

	if err != nil {
		return fmt.Errorf("error in discardOptionData: %w", err)
	}

	return nil
}

func serveTransmission(conn net.Conn, backend nbdbackend.Backend, options *Options) error {
	buffer := []byte{}

	for {
		var requestHeader protocol.TransmissionRequestHeader
		if err := binary.Read(conn, binary.BigEndian, &requestHeader); err != nil {
			return fmt.Errorf("error in readTransmissionRequestHeader: %w", err)
		}

		if requestHeader.RequestMagic != protocol.TRANSMISSION_MAGIC_REQUEST {
			return ErrInvalidMagic
		}

		if requestHeader.Length > uint32(options.MaximumRequestSize) {
			return ErrInvalidBlocksize
		}

		if requestHeader.Length != uint32(len(buffer)) {
			buffer = make([]byte, requestHeader.Length)
		}

		switch requestHeader.Type {
		case protocol.TRANSMISSION_TYPE_REQUEST_READ:
			if err := writeTransmissionReplyHeader(conn, requestHeader.Handle, 0); err != nil {
				return fmt.Errorf("error in writeReadReplyHeader: %w", err)
			}

			n, err := backend.ReadAt(buffer[:requestHeader.Length], int64(requestHeader.Offset))
			if err != nil {
				return fmt.Errorf("error in backend.ReadAt: %w", err)
			}

			if _, err := conn.Write(buffer[:n]); err != nil {
				return fmt.Errorf("error in writeReadReplyPayload: %w", err)
			}
		case protocol.TRANSMISSION_TYPE_REQUEST_WRITE:
			if options.ReadOnly {
				if _, err := io.CopyN(io.Discard, conn, int64(requestHeader.Length)); err != nil {
					return fmt.Errorf("error in discardReadonlyWritePayload: %w", err)
				}

				if err := writeTransmissionReplyHeader(conn, requestHeader.Handle, protocol.TRANSMISSION_ERROR_EPERM); err != nil {
					return fmt.Errorf("error in writeReadonlyReplyHeader: %w", err)
				}

				continue
			}

			if _, err := io.ReadFull(conn, buffer[:requestHeader.Length]); err != nil {
				return fmt.Errorf("error in readWritePayload: %w", err)
			}

			if _, err := backend.WriteAt(buffer[:requestHeader.Length], int64(requestHeader.Offset)); err != nil {
				return fmt.Errorf("error in backend.WriteAt: %w", err)
			}

			if err := writeTransmissionReplyHeader(conn, requestHeader.Handle, 0); err != nil {
				return fmt.Errorf("error in writeWriteReplyHeader: %w", err)
			}
		case protocol.TRANSMISSION_TYPE_REQUEST_DISC:
			if !options.ReadOnly {
				if err := backend.Sync(); err != nil {
					return fmt.Errorf("error in backend.Sync: %w", err)
				}
			}

			return nil
		default:
			if _, err := io.CopyN(io.Discard, conn, int64(requestHeader.Length)); err != nil {
				return fmt.Errorf("error in discardUnknownRequestPayload: %w", err)
			}

			if err := writeTransmissionReplyHeader(conn, requestHeader.Handle, protocol.TRANSMISSION_ERROR_EINVAL); err != nil {
				return fmt.Errorf("error in writeInvalidRequestReplyHeader: %w", err)
			}
		}
	}
}

func writeTransmissionReplyHeader(conn net.Conn, handle uint64, transmissionError uint32) error {
	return binary.Write(conn, binary.BigEndian, protocol.TransmissionReplyHeader{
		ReplyMagic: protocol.TRANSMISSION_MAGIC_REPLY,
		Error:      transmissionError,
		Handle:     handle,
	})
}

func closeBackend(backend nbdbackend.Backend) error {
	closer, ok := backend.(io.Closer)
	if !ok {
		return nil
	}

	return closer.Close()
}
