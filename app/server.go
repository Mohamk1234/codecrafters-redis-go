package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net"
	"os"
	"strings"
)

var keyvaluestore = make(map[string]any)

type Server struct {
	ListenAddr string
	// role       string
	ln net.Listener
	// masterurl  string
	// master_replid string
	// master_repl_offset int
}

var config = make(map[string]string)

func NewServer(ListenAddr string) *Server {
	return &Server{
		ListenAddr: ListenAddr,
	}
}

func (s *Server) Start() error {
	ln, err := net.Listen("tcp", "0.0.0.0:"+s.ListenAddr)
	if err != nil {
		return err
	}
	s.ln = ln
	slog.Info("goredis server running", "listenAddr", s.ListenAddr)
	s.ConnectMaster()
	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go handleConnection(conn)

	}

}

func (s *Server) ConnectMaster() error {
	conn, err := net.Dial("tcp", config["masterurl"])
	if err != nil {
		fmt.Println("Error:", err)
		return err
	}
	defer conn.Close()
	conn.Write(craftArray([]string{"ping"}))
	buff := make([]byte, 1024)
	_, err = conn.Read(buff)

	if err != nil {
		return err
	}
	_, response := ReadNextRESP(buff)
	if response.String() == "PONG" {
		conn.Write(craftArray([]string{"replconf", "listening-port", s.ListenAddr}))
		_, err = conn.Read(buff)

		if err != nil {
			return err
		}
		_, response = ReadNextRESP(buff)

		if response.String() == "OK" {
			conn.Write(craftArray([]string{"replconf", "capa", "psync2"}))
			_, err = conn.Read(buff)

			if err != nil {
				return err
			}
			_, response = ReadNextRESP(buff)
			if response.String() == "OK" {
				conn.Write(craftArray([]string{"psync", "?", "-1"}))
				_, err = conn.Read(buff)

				if err != nil {
					return err
				}
				_, response = ReadNextRESP(buff)
				return nil
			} else {
				return errors.New("Error connecting to Master")
			}
		} else {
			return errors.New("Error connecting to Master")
		}
	} else {
		return errors.New("Error connecting to Master")
	}
}

func findAfter(data []string, target string) string {
	for i := 0; i < len(data)-1; i++ {
		if data[i] == target {
			return data[i+1]
		}
	}
	return ""
}

func main() {

	var ListenAddr string
	var masterurl string

	flag.StringVar(&ListenAddr, "port", "6379", "number of lines to read from the file")
	flag.StringVar(&masterurl, "replicaof", "", "url of master node")
	flag.Parse()

	config["role"] = "master"
	if masterurl != "" {
		config["role"] = "slave"
		config["masterurl"] = masterurl + ":" + findAfter(os.Args[1:], masterurl)
	}

	config["master_replid"] = generateRandomString(40)
	config["master_repl_offset"] = "0"

	server := NewServer(ListenAddr)
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
	case Array:
		response = handleCommand(resp)

	}
	return response, nil
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
	case "replconf":
		response = replconf(cmd)
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
			//fmt.Println("Failed to read buffer", err)
		}

		response, _ := parseMsg(buff)

		if response != nil {
			conn.Write(response)
		}

	}

}
