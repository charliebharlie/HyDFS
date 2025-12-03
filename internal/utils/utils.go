package utils

import (
	"bytes"
	"cs425/MP3/internal/member"
	"encoding/gob"
	"fmt"
	"net"
	"os"
)

// server hostnames
var HOSTS = []string{
	"fa25-cs425-a901.cs.illinois.edu",
	"fa25-cs425-a902.cs.illinois.edu",
	"fa25-cs425-a903.cs.illinois.edu",
	"fa25-cs425-a904.cs.illinois.edu",
	"fa25-cs425-a905.cs.illinois.edu",
	"fa25-cs425-a906.cs.illinois.edu",
	"fa25-cs425-a907.cs.illinois.edu",
	"fa25-cs425-a908.cs.illinois.edu",
	"fa25-cs425-a909.cs.illinois.edu",
	"fa25-cs425-a910.cs.illinois.edu",
}

const DEFAULT_PORT int = 8788

func GetHostName() (string, error) {
	name, err := os.Hostname()
	if err != nil {
		return "", err
	}
	return name, nil
}

// Define message transmission tools and datatypes
type MessageType int

const (
	Ping MessageType = iota
	Pong
	PingReq
	Gossip
	Probe // message for joining
	ProbeAckGossip
	ProbeAckSwim
	UseSwimSus
	UseSwimNoSus
	UseGossipSus
	UseGossipNoSus
	Leave // message for voluntary leave
)

// Message data type for transmission
type Message struct {
	Type          MessageType            // message type
	SenderInfo    member.Info            // sender's info (counter and timestamp here are not used!!!)
	TargetInfo    member.Info            // target's info (counter and timestamp here are not used!!!)
	RequesterInfo member.Info            // requester's info (if direct ping -> sender, if indirect ping -> who start the ping request)
	InfoMap       map[uint64]member.Info // membership Info map
}

func Serialize(obj Message) ([]byte, error) {
	buffer := bytes.Buffer{}
	encoder := gob.NewEncoder(&buffer)
	err := encoder.Encode(obj)
	if err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func Deserialize(data []byte) (Message, error) {
	buffer := bytes.Buffer{}
	buffer.Write(data)
	decoder := gob.NewDecoder(&buffer)

	result := Message{}
	err := decoder.Decode(&result)
	if err != nil {
		return Message{}, err
	}
	return result, nil
}

func SendMessage(message Message, hostname string, port int) (int64, error) {
	address := fmt.Sprintf("%s:%d", hostname, port)
	udpAddress, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		return 0, fmt.Errorf("error resolving UDP address: %s", err.Error())
	}
	//  establist a udp connection
	conn, err := net.DialUDP("udp", nil, udpAddress)
	if err != nil {
		return 0, fmt.Errorf("error creating udp connection: %s", err.Error())
	}
	defer conn.Close()

	// serialize
	serializedMessage, err := Serialize(message)
	if err != nil {
		return 0, fmt.Errorf("error serializing message: %s", err.Error())
	}
	numOfBytes := len(serializedMessage)
	// send the message
	_, err = conn.Write(serializedMessage)
	if err != nil {
		return 0, fmt.Errorf("error sending data: %s", err.Error())
	}
	return int64(numOfBytes), nil
}
