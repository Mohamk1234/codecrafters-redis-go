package main

import (
	"encoding/hex"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

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

func (s *Server) sendInfo(cmd []RESP) []byte {
	t := strings.ToLower(cmd[1].String())

	switch t {
	case "replication":
		return craftBulk("role:" + s.role + "master_replid:" + s.master_replid + "master_repl_offset:" + s.master_repl_offset)
	}
	return []byte("$-1\r\n")
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

func (s *Server) replconf(cmd []RESP) ([]byte, string) {

	var topass string = ""
	command := cmd[1].String()

	if command == "listening-port" {
		return craftSimp("OK"), topass
	} else if command == "capa" {
		return craftSimp("OK"), topass
	} else if command == "ACK" {
		return nil, "Set_ack"
	} else {
		return []byte("$-1\r\n"), topass
	}

}

func (s *Server) psync(cmd []RESP) []byte {
	return craftSimp("FULLRESYNC " + s.master_replid + " " + s.master_repl_offset)
}

func (s *Server) rdbTransfer(conn net.Conn) {
	rawHex := "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"

	data, err := hex.DecodeString(rawHex)
	if err != nil {
		return
	}
	response := "$" + strconv.Itoa(len(data)) + "\r\n"
	conn.Write([]byte(response))
	conn.Write(data)
	s.slave_connections[conn].rdbconfiged = true
}

func (s *Server) handleWait(cmd []RESP) []byte {
	duration := time.Millisecond * time.Duration(cmd[2].Int())
	total_acked := 0
	for conn, _ := range s.slave_connections {
		conn.Write(craftArray([]string{"REPLCONF", "GETACK", "*"}))

	}

	if int64(total_acked) != cmd[1].Int() {
		time.Sleep(duration)
	}

	for _, rep := range s.slave_connections {
		if rep.previous_acked {
			total_acked += 1
		}

	}
	slog.Info("Number of clients", "clients", total_acked)
	return craftInt(strconv.Itoa(total_acked))
}

func (s *Server) getConfig(cmd []RESP) []byte {
	command := cmd[2].String()

	switch strings.ToLower(command) {
	case "dir":
		return craftArray([]string{"dir", s.conf.dir})
	case "dbfilename":
		return craftArray([]string{"dbfilename", s.conf.dbfilename})
	}

	return []byte("$-1\r\n")
}

func (s *Server) getKeys(cmd []RESP) []byte {
	path := s.conf.dir + "/" + s.conf.dbfilename
	content, _ := os.ReadFile(path)
	fmt.Println("MAGIC", string(content[:5]))
	fmt.Println("VERSION", string(content[5:9]))
	fmt.Println("TABLE", parseTable(content))
	key := parseTable(content)
	len := key[3]
	str := key[4 : 4+len]
	return craftArray([]string{string(str)})

}
func indexOf(item byte, byteArray []byte) int {
	for i, value := range byteArray {
		if value == item {
			return i
		}
	}
	return -1
}
func parseTable(bytes []byte) []byte {
	start := indexOf(251, bytes)
	end := indexOf(255, bytes)
	return bytes[start+1 : end]
}
