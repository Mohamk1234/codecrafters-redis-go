package main

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)

type Type byte

const (
	Integer = ':'
	String  = '+'
	Bulk    = '$'
	Array   = '*'
	Error   = '-'
)

type RESP struct {
	Type  Type
	Raw   []byte
	Data  []byte
	Count int
}

var keyvaluestore = make(map[string]any)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go handleConnection(conn)

	}

}

func (r RESP) Bytes() []byte {
	return r.Data
}

func (r RESP) String() string {
	return string(r.Data)
}

func (r RESP) Int() int64 {
	x, _ := strconv.ParseInt(r.String(), 10, 64)
	return x
}

func (r RESP) Float() float64 {
	x, _ := strconv.ParseFloat(r.String(), 10)
	return x
}

// ForEach iterates over each Array element
// func (r RESP) ForEach(iter func(resp RESP) bool) {
// 	data := r.Data
// 	for i := 0; i < r.Count; i++ {
// 		n, resp := ReadNextRESP(data)
// 		if !iter(resp) {
// 			return
// 		}
// 		data = data[n:]
// 	}
// }

func (r RESP) ForEach(iter func(resp RESP, results *[]RESP) bool) []RESP {
	data := r.Data
	var respObjects []RESP // Declare an empty slice to store RESP objects
	for i := 0; i < r.Count; i++ {
		n, resp := ReadNextRESP(data)
		if !iter(resp, &respObjects) { // Pass the slice address using &
			return respObjects
		}
		data = data[n:]
	}
	return respObjects
}

func ReadNextRESP(b []byte) (n int, resp RESP) {
	if len(b) == 0 {
		return 0, RESP{} // no data to read
	}
	resp.Type = Type(b[0])
	switch resp.Type {
	case Integer, String, Bulk, Array, Error:
	default:
		return 0, RESP{} // invalid kind
	}
	// read to end of line
	i := 1
	for ; ; i++ {
		if i == len(b) {
			return 0, RESP{} // not enough data
		}
		if b[i] == '\n' {
			if b[i-1] != '\r' {
				return 0, RESP{} //, missing CR character
			}
			i++
			break
		}
	}
	resp.Raw = b[0:i]
	resp.Data = b[1 : i-2]
	if resp.Type == Integer {
		// Integer
		if len(resp.Data) == 0 {
			return 0, RESP{} //, invalid integer
		}
		var j int
		if resp.Data[0] == '-' {
			if len(resp.Data) == 1 {
				return 0, RESP{} //, invalid integer
			}
			j++
		}
		for ; j < len(resp.Data); j++ {
			if resp.Data[j] < '0' || resp.Data[j] > '9' {
				return 0, RESP{} // invalid integer
			}
		}
		return len(resp.Raw), resp
	}
	if resp.Type == String || resp.Type == Error {
		// String, Error
		return len(resp.Raw), resp
	}
	var err error
	resp.Count, err = strconv.Atoi(string(resp.Data))
	if resp.Type == Bulk {
		// Bulk
		if err != nil {
			return 0, RESP{} // invalid number of bytes
		}
		if resp.Count < 0 {
			resp.Data = nil
			resp.Count = 0
			return len(resp.Raw), resp
		}
		if len(b) < i+resp.Count+2 {
			return 0, RESP{} // not enough data
		}
		if b[i+resp.Count] != '\r' || b[i+resp.Count+1] != '\n' {
			return 0, RESP{} // invalid end of line
		}
		resp.Data = b[i : i+resp.Count]
		resp.Raw = b[0 : i+resp.Count+2]
		resp.Count = 0
		return len(resp.Raw), resp
	}
	// Array
	if err != nil {
		return 0, RESP{} // invalid number of elements
	}
	var tn int
	sdata := b[i:]
	for j := 0; j < resp.Count; j++ {
		rn, rresp := ReadNextRESP(sdata)
		if rresp.Type == 0 {
			return 0, RESP{}
		}
		tn += rn
		sdata = sdata[rn:]
	}
	resp.Data = b[i : i+tn]
	resp.Raw = b[0 : i+tn]
	return len(resp.Raw), resp
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
	key := string(cmd[1].Data)
	var value any

	switch cmd[2].Type {
	case Bulk:
		value = string(cmd[2].Data)
	case Integer:
		value, _ = strconv.Atoi(string(cmd[2].Data))
	default:
		return []byte("$-1\r\n")
	}

	keyvaluestore[key] = value
	return craftSimp("OK")
}

func getFromStore(cmd []RESP) []byte {
	value, err := keyvaluestore[string(cmd[1].Data)]
	fmt.Println(value)
	if err {
		return []byte("$-1\r\n")
	}
	v, ok := value.(string)

	if !ok {
		return []byte("$-1\r\n")
	}
	fmt.Println(v)
	return craftBulk(v)
}

func echo(cmd []RESP) []byte {
	return craftBulk(string(cmd[1].Data))
}

func handleCommand(resp RESP) []byte {

	var cmd []RESP

	cmd = resp.ForEach(func(resp RESP, results *[]RESP) bool {
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
			os.Exit(1)
		}

		response, err := parseMsg(buff)

		if err != nil {
			fmt.Println("Error reading resp")
			os.Exit(1)
		}

		if response != nil {
			conn.Write(response)
		}

	}

}
