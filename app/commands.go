package main

import (
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

func sendInfo(cmd []RESP) []byte {
	t := strings.ToLower(cmd[1].String())

	switch t {
	case "replication":
		return craftBulk("role:" + config["role"] + "master_replid:" + config["master_replid"] + "master_repl_offset:" + config["master_repl_offset"])
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

func replconf(cmd []RESP) []byte {
	command := cmd[1].String()

	if command == "listening-port" {
		config["peer_port"] = cmd[2].String()
		return craftSimp("OK")
	} else if command == "capa" {
		return craftSimp("OK")
	} else {
		return []byte("$-1\r\n")
	}

}

func psync(cmd []RESP) []byte {
	return craftSimp("fullresync " + config["master_replid"] + " " + config["master_repl_offset"])
}
