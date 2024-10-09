package main

import (
	"fmt"
	"net"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"google.golang.org/protobuf/proto"
)

func main() {
	addr, err := net.ResolveUDPAddr("udp", ":7072")
	if err != nil {
		fmt.Println("Error resolving address:", err)
		return
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		fmt.Println("Error listening:", err)
		return
	}
	defer conn.Close()

	buffer := make([]byte, 10240)
	for {
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

		// Unmarshal the remaining part into the Envelope struct (excluding the last 2 bytes)
		envelope := &common.Envelope{}
		err = proto.Unmarshal(buffer[:n-2], envelope)
		if err != nil {
			fmt.Println("Failed to unmarshal envelope:", err)
			continue
		}

		// Display the received Envelope
		fmt.Printf("Received Envelope from %s: Payload = %s, Signature = %s\n", clientAddr, envelope.GetPayload(), envelope.GetSignature())

		err = forward(envelope)
		if err != nil {
			fmt.Println("Error forward to orderer:", err)
		}

		// Optionally, respond to the client
		response := []byte("Acknowledged")
		conn.WriteToUDP(response, clientAddr)
	}
}

func forward(tx *common.Envelope) error {
	serverAddr, err := net.ResolveUDPAddr("udp", ":7073")
	if err != nil {
		fmt.Println("Error resolving address:", err)
		return err
	}

	conn, err := net.DialUDP("udp", nil, serverAddr)
	if err != nil {
		fmt.Println("Error connecting to server:", err)
		return err
	}
	defer conn.Close()

	data, err := proto.Marshal(tx)
	if err != nil {
		fmt.Println("Failed to marshal envelope:", err)
		return err
	}

	seqBytes := []byte{0x00, 0x00} // The extra bytes you want to add
	dataWithseqBytes := append(data, seqBytes...)

	_, err = conn.Write(dataWithseqBytes)
	if err != nil {
		fmt.Println("Error sending envelope with extra bytes:", err)
		return err
	}

	return nil
}
