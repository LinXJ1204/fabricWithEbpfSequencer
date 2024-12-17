package gateway

import (
	"fmt"
	"net"
	"time"
)

func (gs *Server) connect() error {

	sequencerAddr, err := net.ResolveUDPAddr("udp", "192.168.50.184:7072")
	if err != nil {
		return fmt.Errorf("error connecting to server: %w", err)
	}

	conn, err := net.DialUDP("udp", nil, sequencerAddr)
	if err != nil {
		return fmt.Errorf("error connecting to server: %w", err)
	}
	fmt.Println("Connected to server")

	gs.UdpGateway = conn

	return nil
}

func (gs *Server) disconnect() error {

	err := gs.UdpGateway.Close()
	if err != nil {
		return fmt.Errorf("error connecting to server: %w", err)
	}

	return nil
}

func (gs *Server) reconnect() error {
	fmt.Println("Attempting to reconnect...")
	gs.UdpGateway.Close()
	time.Sleep(2 * time.Nanosecond) // Wait before attempting to reconnect

	return gs.connect()
}
