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

		// Optionally, respond to the client
		response := []byte("Acknowledged")
		conn.WriteToUDP(response, clientAddr)
	}
}
