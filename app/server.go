package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

var keyvaluestore = make(map[string]any)

type Server struct {
	ListenAddr string
	role       string
	ln         net.Listener
}

func NewServer(ListenAddr string, role string) *Server {
	return &Server{
		ListenAddr: ListenAddr,
		role:       role,
	}
}

func (s *Server) Start() error {
	ln, err := net.Listen("tcp", "0.0.0.0:"+s.ListenAddr)
	if err != nil {
		return err
	}
	s.ln = ln
	slog.Info("goredis server running", "listenAddr", s.ListenAddr)
	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go handleConnection(conn)

	}

}

func main() {

	var ListenAddr string
	flag.StringVar(&ListenAddr, "port", "6379", "number of lines to read from the file")
	flag.Parse()

	server := NewServer(ListenAddr, "master")
	log.Fatal(server.Start())
}

func parseMsg(msg []byte) ([]byte, error) {
	s, resp := ReadNextRESP(msg)
	if s == 0 {
		return nil, errors.New("no resp object")
	}
	t := resp.Type
	var response []byte = nil
	switch t {
	case Integer:
	case String:
	case Bulk:
	case Array:
		response = handleCommand(resp)
	case Error:
	}
	return response, nil
}

func craftBulk(r string) []byte {
	return []byte("$" + strconv.Itoa(len(r)) + "\r\n" + r + "\r\n")
}

func craftSimp(r string) []byte {
	return []byte("+" + r + "\r\n")
}

func addToStore(cmd []RESP) []byte {
	key := cmd[1].String()
	var value any
	duration := 0
	expiry := time.Now()
	switch cmd[2].Type {
	case Bulk:
		value = cmd[2].String()
	case Integer:
		value = cmd[2].Int()
	default:
		return []byte("$-1\r\n")
	}

	if len(cmd) > 3 {
		if strings.ToLower(string(cmd[3].Data)) == "px" {
			Mil, err := strconv.Atoi(strings.ToLower(string(cmd[4].Data)))
			if err != nil {
				return []byte("$-1\r\n")
			}
			duration = Mil
			expiry = time.Now().Add(time.Millisecond * time.Duration(Mil))
		}

	}
	obj := TimedObject{
		value:    value,
		duration: duration,
		expiry:   expiry,
	}

	keyvaluestore[key] = obj
	return craftSimp("OK")
}

func getFromStore(cmd []RESP) []byte {
	obj, ok := keyvaluestore[cmd[1].String()]
	if !ok {
		return []byte("$-1\r\n")
	}
	o, _ := obj.(TimedObject)
	v, ok := o.value.(string)

	if !ok || (o.duration != 0 && time.Now().After(o.expiry)) {
		delete(keyvaluestore, cmd[1].String())
		return []byte("$-1\r\n")
	}
	return craftBulk(v)
}

func echo(cmd []RESP) []byte {
	return craftBulk(cmd[1].String())
}

func sendInfo(cmd []RESP) []byte {
	t := strings.ToLower(cmd[1].String())

	switch t {
	case "replication":
		return craftBulk("role:master")
	}
	return []byte("$-1\r\n")
}

func handleCommand(resp RESP) []byte {

	var cmd = resp.ForEach(func(resp RESP, results *[]RESP) bool {
		// Process RESP object if needed
		*results = append(*results, resp) // Append RESP object to the slice
		return true                       // Continue iterating
	})

	var response []byte = nil
	switch strings.ToLower(string(cmd[0].Data)) {
	case "ping":
		response = []byte("+PONG\r\n")
	case "echo":
		response = echo(cmd)
	case "set":
		response = addToStore(cmd)
	case "get":
		response = getFromStore(cmd)
	case "info":
		response = sendInfo(cmd)

	default:
		fmt.Println("error")
	}
	return response
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	for {
		buff := make([]byte, 1024)
		_, err := conn.Read(buff)

		if err != nil {
			fmt.Println("Failed to read buffer", err)

		}

		response, _ := parseMsg(buff)

		if response != nil {
			conn.Write(response)
		}

	}

}
