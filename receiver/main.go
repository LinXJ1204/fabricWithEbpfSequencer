package receiver

import (
	"fmt"
	"net"
	"sync"
	"time"
)

func main() {
	// Define the port and address
	address := ":8086"

	// Create a UDP address
	udpAddr, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		fmt.Println("Error resolving address:", err)
		return
	}

	// Create a UDP connection
	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		fmt.Println("Error creating UDP connection:", err)
		return
	}
	defer conn.Close()

	fmt.Printf("UDP server listening on %s\n", address)

	// Counter and Mutex for thread-safe operations
	var counter int
	var mutex sync.Mutex

	// Channel to stop the timer
	stop := make(chan bool)

	// Goroutine to print the count every minute
	go func() {
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				mutex.Lock()
				fmt.Printf("Packets received in the last minute: %d\n", counter)
				counter = 0 // Reset the counter
				mutex.Unlock()
			case <-stop:
				return
			}
		}
	}()

	// Buffer to hold incoming data
	buffer := make([]byte, 10240)

	// Listen for incoming packets
	for {
		_, _, err := conn.ReadFromUDP(buffer)
		if err != nil {
			fmt.Println("Error reading from UDP:", err)
			continue
		}

		// Increment the counter
		mutex.Lock()
		counter++
		mutex.Unlock()
	}
}
