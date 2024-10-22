package main

import (
	"fmt"
	"net"
	"sync"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"google.golang.org/protobuf/proto"
)

var wgg sync.WaitGroup

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

		data, err := proto.Marshal(envelope)
		if err != nil {
			fmt.Println("Failed to marshal envelope:", err)
		}

		seqBytes := []byte{0x00, 0x00} // The extra bytes you want to add
		dataWithseqBytes := append(data, seqBytes...)

		wgg.Add(5)
		ports := [5]string{"7073", "8073", "9073", "10073", "11073"}
		addrs := [5]string{"192.168.50.224", "192.168.50.224", "192.168.50.213", "192.168.50.213", "192.168.50.230"}
		for index, _ := range ports {
			// Pass the current value of p into the goroutine by adding it as a parameter
			go func(addr string, port string) {
				defer wgg.Done()                             // Mark this goroutine as done when finished
				err := forward(dataWithseqBytes, addr, port) // Use local err to avoid data race
				if err != nil {
					fmt.Println("Error forward to orderer:", err)
				}
			}(addrs[index], ports[index]) // Passing p as an argument ensures each goroutine gets a unique value of p
		}

		wgg.Wait() // Wait for all async transactions to complete
		// Optionally, respond to the client
		response := []byte("Acknowledged")
		conn.WriteToUDP(response, clientAddr)
	}
}

func forward(tx []byte, host string, port string) error {
	address := net.JoinHostPort(host, port)

	fmt.Println(address)
	serverAddr, err := net.ResolveUDPAddr("udp", address)
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

	_, err = conn.Write(tx)
	if err != nil {
		fmt.Println("Error sending envelope with extra bytes:", err)
		return err
	}

	return nil
}
