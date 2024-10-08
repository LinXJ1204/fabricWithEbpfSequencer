package server

import (
	"encoding/binary"
	"fmt"
	"net"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric/orderer/common/multichannel"
	"google.golang.org/protobuf/proto"
)

type UdpServer struct {
	host string
	port uint
	*multichannel.Registrar
	exitChanUDP chan struct{}
}

func NewUDPServer(
	_host string,
	_port uint,
	r *multichannel.Registrar,
) *UdpServer {
	return &UdpServer{host: _host, port: _port, exitChanUDP: make(chan struct{}), Registrar: r}
}

func (us *UdpServer) Start() error {
	addr, err := net.ResolveUDPAddr("udp", ":7072")
	if err != nil {
		fmt.Println("Error resolving address:", err)
		return err
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		fmt.Println("Error listening:", err)
		return err
	}

	buffer := make([]byte, 10240)
	for {
		select {
		case <-us.exitChanUDP:
			conn.Close()
			return nil
		default:
			// Read UDP data
			n, clientAddr, err := conn.ReadFromUDP(buffer)
			if err != nil {
				fmt.Println("Error reading from connection:", err)
				continue
			}

			// Extract the extra bytes from the tail
			if n < 2 {
				fmt.Println("Not enough data received")
				continue
			}
			extraBytes := buffer[n-2 : n] // The last 2 bytes are the extra bytes
			fmt.Printf("Received extra bytes: %x\n", extraBytes)
			// Create a larger byte slice to hold the uint64 value (8 bytes)
			var paddedBytes [8]byte
			// Copy the two bytes into the last two positions of the paddedBytes slice
			copy(paddedBytes[6:], extraBytes)

			// Convert to uint64 using BigEndian (for example)
			extraUint64 := binary.BigEndian.Uint64(paddedBytes[:])
			fmt.Printf("Received extra bytes as uint64: %d\n", extraUint64)

			// Unmarshal the remaining part into the Envelope struct (excluding the last 2 bytes)
			envelope := &common.Envelope{}
			err = proto.Unmarshal(buffer[:n-2], envelope)
			if err != nil {
				fmt.Println("Failed to unmarshal envelope:", err)
				continue
			}

			// Display the received Envelope
			fmt.Printf("Received Envelope from %s: Payload = %s, Signature = %s\n", clientAddr, envelope.GetPayload(), envelope.GetSignature())

			chdr, isConfig, processor, err := us.BroadcastChannelSupport(envelope)
			if err != nil {
				continue
			}

			if !isConfig {
				logger.Debugf("[channel: %s] Broadcast is processing normal message from %s with txid '%s'", chdr.ChannelId, addr, chdr.TxId)

				configSeq, err := processor.ProcessNormalMsg(envelope)
				if err != nil {
					logger.Warningf("[channel: %s] Rejecting broadcast of normal message from %s because of error: %s", chdr.ChannelId, addr, err)
					continue
				}

				if err = processor.WaitReady(); err != nil {
					logger.Warningf("[channel: %s] Rejecting broadcast of message from %s with SERVICE_UNAVAILABLE: rejected by Consenter: %s", chdr.ChannelId, addr, err)
					continue
				}

				err = processor.Order(envelope, configSeq, 1, extraUint64)
				if err != nil {
					logger.Warningf("[channel: %s] Rejecting broadcast of normal message from %s with SERVICE_UNAVAILABLE: rejected by Order: %s", chdr.ChannelId, addr, err)
					continue
				}
			} else { // isConfig
				logger.Debugf("[channel: %s] Broadcast is processing config update message from %s", chdr.ChannelId, addr)

				config, configSeq, err := processor.ProcessConfigUpdateMsg(envelope)
				if err != nil {
					logger.Warningf("[channel: %s] Rejecting broadcast of config message from %s because of error: %s", chdr.ChannelId, addr, err)
					continue
				}

				if err = processor.WaitReady(); err != nil {
					logger.Warningf("[channel: %s] Rejecting broadcast of message from %s with SERVICE_UNAVAILABLE: rejected by Consenter: %s", chdr.ChannelId, addr, err)
					continue
				}

				err = processor.Configure(config, configSeq)
				if err != nil {
					logger.Warningf("[channel: %s] Rejecting broadcast of config message from %s with SERVICE_UNAVAILABLE: rejected by Configure: %s", chdr.ChannelId, addr, err)
					continue
				}
			}
		}
	}
}

func (s *UdpServer) Close() {
	// Implementation to close the UDP server
	fmt.Println("Closing UDP server on", s.host, ":", s.port)
	// Logic to clean up resources goes here
	close(s.exitChanUDP) // Signal server exit, if needed
}
