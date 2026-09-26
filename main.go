package main

import (
	"bufio"
	"net"
)

func StartEchoServer(port string) error {
	listener, err := net.Listen("tcp", port)
	if err != nil {
		return err
	}
	defer listener.Close()
	conn, err := listener.Accept()
	if err != nil {
		return err
	}
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		conn.Write([]byte(scanner.Text() + "\n"))
	}
	return nil
}

func main() {
	StartEchoServer(":8080")
}
