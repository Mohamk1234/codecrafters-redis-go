package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net"
	"os"
	"reflect"
	"strconv"
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
	slave_connections  map[net.Conn]struct{}
}

func NewServer(ListenAddr string, role string, masterurl string, master_replid string, master_repl_offset string) *Server {
	return &Server{
		ListenAddr:         ListenAddr,
		role:               role,
		masterurl:          masterurl,
		master_replid:      master_replid,
		master_repl_offset: master_repl_offset,
		slave_connections:  make(map[net.Conn]struct{}),
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
				_, err := conn.Read(buff)

				if err != nil {
					return err
				}
				_, response = ReadNextRESP(buff)

				_, err = conn.Read(buff)
				go s.commandsFromMaster(conn)
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

func (s *Server) commandsFromMaster(conn net.Conn) {
	defer conn.Close()
	bytesread := 0
	for {

		buff := make([]byte, 1024)
		_, err := conn.Read(buff)

		if err != nil {
			//fmt.Println("Failed to read buffer", err)
		}
		total_size := len(buff)
		total_read := 0
		for total_size > total_read {
			si, resp := ReadNextRESP(buff)

			if si == 0 {
				continue
			}
			var cmd = resp.ForEach(func(resp RESP, results *[]RESP) bool {
				// Process RESP object if needed
				*results = append(*results, resp) // Append RESP object to the slice
				return true                       // Continue iterating
			})
			fmt.Println(string(cmd[0].Data))
			switch strings.ToLower(string(cmd[0].Data)) {
			case "set":
				_ = addToStore(cmd)
			case "replconf":
				print("In replconf")
				conn.Write([]byte(craftArray(([]string{"REPLCONF", "ACK", strconv.Itoa(bytesread)}))))

			}

			bytesread = len(resp.Raw) + bytesread
			total_read += si
		}

	}
}

// func findAfter(data []string, target string) string {
// 	for i := 0; i < len(data)-1; i++ {
// 		if data[i] == target {
// 			return data[i+1]
// 		}
// 	}
// 	return ""
// }

func main() {

	var ListenAddr string
	var masterurl string

	flag.StringVar(&ListenAddr, "port", "6379", "number of lines to read from the file")
	flag.StringVar(&masterurl, "replicaof", "", "url of master node")
	flag.Parse()

	role := "master"
	if masterurl != "" {
		role = "slave"
		ss := strings.Split(masterurl, " ")
		masterurl = ss[0] + ":" + ss[1]
	}

	server := NewServer(ListenAddr, role, masterurl, generateRandomString(40), "0")
	log.Fatal(server.Start())
}

func (s *Server) parseMsg(msg []byte) ([]byte, string) {
	si, resp := ReadNextRESP(msg)
	if si == 0 {
		return nil, "no resp object"
	}
	t := resp.Type
	var response []byte = nil
	var topass string = ""
	switch t {
	case Array:
		response, topass = s.handleCommand(resp)

	}
	return response, topass
}

func (s *Server) handleCommand(resp RESP) ([]byte, string) {

	var cmd = resp.ForEach(func(resp RESP, results *[]RESP) bool {
		// Process RESP object if needed
		*results = append(*results, resp) // Append RESP object to the slice
		return true                       // Continue iterating
	})

	var response []byte = nil
	var topass string = ""
	switch strings.ToLower(string(cmd[0].Data)) {
	case "ping":
		response = []byte("+PONG\r\n")
	case "echo":
		response = echo(cmd)
	case "set":
		response = addToStore(cmd)
		if !reflect.DeepEqual(response, []byte("$-1\r\n")) {
			s.addtoreplicas(resp.Raw)
		}
	case "get":
		response = getFromStore(cmd)
	case "info":
		response = s.sendInfo(cmd)
	case "replconf":
		response = s.replconf(cmd)
	case "psync":
		response = s.psync(cmd)
		topass = "rdbsync"

	default:
		fmt.Println("error")
	}
	return response, topass
}

func (s *Server) addtoreplicas(command []byte) {
	for conn, _ := range s.slave_connections {
		conn.Write(command)
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	for {
		buff := make([]byte, 1024)
		_, err := conn.Read(buff)

		if err != nil {
			//fmt.Println("Failed to read buffer", err)
		}

		response, msg := s.parseMsg(buff)

		if response != nil {
			conn.Write(response)
		}

		if msg == "rdbsync" {
			s.rdbTransfer(conn)
			s.slave_connections[conn] = struct{}{}
		}

	}

}
