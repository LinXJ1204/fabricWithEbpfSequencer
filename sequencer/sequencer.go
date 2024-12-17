package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"sync"
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

	var count uint32 = 1

	buffer := make([]byte, 10240)

	defer conn.Close()

	for {
		// Read UDP data
		n, _, err := conn.ReadFromUDP(buffer)
		if err != nil {
			fmt.Println("Error reading from connection:", err)
			continue
		}

		// Extract the extra bytes from the tail
		if n < 2 {
			fmt.Println("Not enough data received")
			continue
		}
		seqBytes := make([]byte, 4) // The extra bytes you want to add
		binary.BigEndian.PutUint32(seqBytes, count)
		dataWithseqBytes := append(buffer[:n-4], seqBytes...)

		fmt.Println("=====MSG COUNT=====")
		fmt.Println(count)

		ports := [5]string{"7073", "8073", "9073", "10073", "11073"}
		addrs := [5]string{"192.168.50.224", "192.168.50.213", "192.168.50.230", "192.168.50.239", "192.168.50.219"}

		for i := 1; i < 5; i++ {
			ordererAddress := net.JoinHostPort(addrs[0], ports[0])

			ordererServerAddr, err := net.ResolveUDPAddr("udp", ordererAddress)
			if err != nil {
				fmt.Println("Error resolving address:", err)
				continue
			}

			ordererConn, err := net.DialUDP("udp", nil, ordererServerAddr)
			if err != nil {
				fmt.Println("Error connecting to server:", err)
			}

			err = forward(dataWithseqBytes, ordererConn) // Use local err to avoid data race
			if err != nil {
				fmt.Println("Error forward to orderer:", err)
			}
		}

		count++

		// Optionally, respond to the client
	}
}

func forward(tx []byte, conn *net.UDPConn) error {
	_, err := conn.Write(tx)
	if err != nil {
		fmt.Println("Error sending envelope with extra bytes:", err)
		return err
	}

	return nil
}
