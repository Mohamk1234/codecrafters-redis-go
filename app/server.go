package main

import (
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net"
	"reflect"
	"strconv"
	"strings"
)

var keyvaluestore = make(map[string]any)

type Config struct {
	dir        string
	dbfilename string
}

type Server struct {
	ListenAddr         string
	role               string
	ln                 net.Listener
	masterurl          string
	master_replid      string
	master_repl_offset string
	slave_connections  map[net.Conn]*Replica
	conf               Config
}

type Replica struct {
	bytes_read     int64
	previous_acked bool
	rdbconfiged    bool
}

func NewServer(ListenAddr string, role string, masterurl string, master_replid string, master_repl_offset string, config *Config) *Server {
	return &Server{
		ListenAddr:         ListenAddr,
		role:               role,
		masterurl:          masterurl,
		master_replid:      master_replid,
		master_repl_offset: master_repl_offset,
		slave_connections:  make(map[net.Conn]*Replica),
		conf:               *config,
	}
}

func (s *Server) Start() error {
	ln, err := net.Listen("tcp", "0.0.0.0:"+s.ListenAddr)
	if err != nil {
		return err
	}
	s.ln = ln
	slog.Info("goredis server running", "listenAddr", s.ListenAddr)
	if s.role == "slave" {
		go s.ConnectMaster()
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			slog.Info("error reading from connection", "err:", err)
		}

		go s.handleConnection(conn)

	}

}

func (s *Server) ConnectMaster() error {
	conn, err := net.Dial("tcp", s.masterurl)
	if err != nil {
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
				slog.Info("message in buffer", "message", response.Data)

				_, err = conn.Read(buff)

				if err != nil {
					slog.Info("error reading from connection", "err:", err)
				}

				_, response = ReadNextRESP(buff)
				slog.Info("message in buffer", "message", response.Data)
				go s.commandsFromMaster(conn)

				return nil

			}
		}
	}
	return nil
}

func (s *Server) commandsFromMaster(conn net.Conn) {
	slog.Info("Receiving commands from Master at ", "listenAddr", s.ListenAddr)
	defer conn.Close()
	bytesread := 0
	for {

		buff := make([]byte, 1024)
		bufflen, err := conn.Read(buff)

		if err != nil {
			slog.Info("error reading from connection", "err:", err)
		}
		total_read := 0
		for bufflen > total_read {
			si, resp := ReadNextRESP(buff)

			if si == 0 {
				continue
			}
			var cmd = resp.ForEach(func(resp RESP, results *[]RESP) bool {
				// Process RESP object if needed
				*results = append(*results, resp) // Append RESP object to the slice
				return true                       // Continue iterating
			})
			switch strings.ToLower(string(cmd[0].Data)) {
			case "set":
				_ = addToStore(cmd)
			case "replconf":
				conn.Write([]byte(craftArray(([]string{"REPLCONF", "ACK", strconv.Itoa(bytesread)}))))

			}

			buff = buff[si:]
			bytesread = si + bytesread
			total_read += si
		}

	}
}

func main() {

	var ListenAddr string
	var masterurl string
	var dir string
	var dbfilename string

	flag.StringVar(&ListenAddr, "port", "6379", "number of lines to read from the file")
	flag.StringVar(&masterurl, "replicaof", "", "url of master node")
	flag.StringVar(&dir, "dir", "", "Directory of rdb file")
	flag.StringVar(&dbfilename, "dbfilename", "", "Filename for rdb")
	flag.Parse()

	role := "master"
	if masterurl != "" {
		role = "slave"
		ss := strings.Split(masterurl, " ")
		masterurl = ss[0] + ":" + ss[1]
	}
	cfg := new(Config)
	cfg.dir = dir
	cfg.dbfilename = dbfilename
	server := NewServer(ListenAddr, role, masterurl, generateRandomString(40), "0", cfg)
	log.Fatal(server.Start())
}

func (s *Server) handleCommand(msg []byte) ([]byte, string) {

	_, resp := ReadNextRESP(msg)
	var cmd = resp.ForEach(func(resp RESP, results *[]RESP) bool {
		// Process RESP object if needed
		*results = append(*results, resp) // Append RESP object to the slice
		return true                       // Continue iterating
	})

	if len(cmd) == 0 {
		return nil, ""
	}

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
			go s.addtoreplicas(resp.Raw)
		}
	case "keys":
		response = s.getKeys(cmd)
	case "get":
		response = getFromStore(cmd)
	case "info":
		response = s.sendInfo(cmd)
	case "replconf":
		response, topass = s.replconf(cmd)
	case "psync":
		response = s.psync(cmd)
		topass = "rdbsync"
	case "wait":
		response = s.handleWait(cmd)
	case "config":
		response = s.getConfig(cmd)
	default:
		fmt.Println("error")
	}
	return response, topass
}

func (s *Server) addtoreplicas(command []byte) {

	for conn, _ := range s.slave_connections {
		s.slave_connections[conn].previous_acked = false
		conn.Write(command)
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	for {
		buff := make([]byte, 1024)
		_, err := conn.Read(buff)

		if err != nil {
			slog.Info("error reading from connection", "err:", err)
		}

		response, msg := s.handleCommand(buff)

		if response != nil {
			conn.Write(response)
		}

		if msg == "rdbsync" {
			s.slave_connections[conn] = &Replica{bytes_read: 0, previous_acked: true, rdbconfiged: false}
			s.rdbTransfer(conn)

		}

		if msg == "Set_ack" {
			s.slave_connections[conn].previous_acked = true
		}

	}

}
