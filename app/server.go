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
	ListenAddr         string
	role               string
	ln                 net.Listener
	masterurl          string
	master_replid      string
	master_repl_offset string
	slave_urls         []string
}

func NewServer(ListenAddr string, role string, masterurl string, master_replid string, master_repl_offset string) *Server {
	return &Server{
		ListenAddr:         ListenAddr,
		role:               role,
		masterurl:          masterurl,
		master_replid:      master_replid,
		master_repl_offset: master_repl_offset,
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

		go s.handleConnection(conn)

	}

}

func (s *Server) ConnectMaster() error {
	conn, err := net.Dial("tcp", s.masterurl)
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

	role := "master"
	if masterurl != "" {
		role = "slave"
		masterurl = masterurl + ":" + findAfter(os.Args[1:], masterurl)
	}

	server := NewServer(ListenAddr, role, masterurl, generateRandomString(40), "0")
	log.Fatal(server.Start())
}

func (s *Server) parseMsg(msg []byte, conn net.Conn) ([]byte, error) {
	si, resp := ReadNextRESP(msg)
	if si == 0 {
		return nil, errors.New("no resp object")
	}
	t := resp.Type
	var response []byte = nil
	switch t {
	case Array:
		response = s.handleCommand(resp, conn)

	}
	return response, nil
}

func (s *Server) handleCommand(resp RESP, conn net.Conn) []byte {

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
		response = s.sendInfo(cmd)
	case "replconf":
		response = s.replconf(cmd)
	case "psync":
		response = s.psync(cmd, conn)

	default:
		fmt.Println("error")
	}
	return response
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()
	for {
		buff := make([]byte, 1024)
		_, err := conn.Read(buff)

		if err != nil {
			//fmt.Println("Failed to read buffer", err)
		}

		response, _ := s.parseMsg(buff, conn)

		if response != nil {
			conn.Write(response)
		}

	}

}
