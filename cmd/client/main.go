package main

import (
	"cs425/MP3/internal/utils"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

const (
	CONNECTION_TIMEOUT = 1 * time.Second
	CALL_TIMEOUT       = 1 * time.Second
)

// arguments for cli tool
type Args struct {
	Command     string
	Rate        float64
	FileName    string
	FileContent []byte
	Time        int64
	ClientId    string
}

func CallWithTimeout(
	hostname string,
	port int,
	args Args,
	result *string) {

	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", hostname, port), CONNECTION_TIMEOUT)
	log.Printf("Server %s:%d\n", hostname, port)
	if err != nil {
		log.Printf("Failed to dial server %s:%d: %s\n", hostname, port, err.Error())
		return
	}
	defer conn.Close()

	client := rpc.NewClient(conn)
	callChan := make(chan error, 1)

	go func() {
		callChan <- client.Call("Server.CLI", args, result)
	}()
	select {
	case err := <-callChan:
		if err != nil {
			log.Printf("RPC call to server %s:%d failed: %s\n", hostname, port, err.Error())
		}
	case <-time.After(CALL_TIMEOUT):
		log.Printf("RPC call to server %s:%d timed out\n", hostname, port)
	}
}

func main() {
	// if len(os.Args) < 3 {
	// 	log.Println("Usage: ./client <Query> <Hostname> -p <Port>")
	// 	log.Println("Example: ./client list_mem fa25-cs425-b601.cs.illinois.edu")
	// 	log.Println("Example: ./client create localfilename HyDFSfilename")
	// 	log.Println("Available commands: list_mem, list_self, join, leave, display_suspects, display_protocol, switch(protocol, suspicion), create, get, append")
	// }

	port := 12346
	// TODO: Change it so that we would use the current vm for handling the client requests
	hostname, err := utils.GetHostName()
	if err != nil {
		log.Fatal("Cannot find Hostname")
		os.Exit(1)
	}

	client, err := utils.GetHostName()
	if err != nil {
		log.Fatal("Cannot find Hostname")
		os.Exit(1)
	}
	// client = "C1"

	var otherArgs []string
	for i := 1; i < len(os.Args); i++ {
		switch os.Args[i] {
		case "-p":
			if (i + 1) >= len(os.Args) {
				log.Fatal("Error: -p port requires a port number.")
			}
			num, err := strconv.Atoi(os.Args[i+1])
			port = num
			if err != nil {
				log.Fatal("Invalid port number!")
			}
			i++
		default:
			otherArgs = append(otherArgs, os.Args[i])
		}
	}

	// if len(otherArgs) < 2 {
	// 	log.Fatal("Please specify query and hostname")
	// }

	command := otherArgs[0]
	result := new(string)
	fmt.Println(otherArgs)

	var args Args
	args.Command = command
	args.Rate = 0
	args.ClientId = client
	args.Time = time.Now().UnixMilli()

	switch command {
	case "create":
		if len(otherArgs) < 3 {
			log.Fatal("Usage: ./client create <localfilename> <HyDFSfilename>")
		}
		localFile := otherArgs[1]
		args.FileName = otherArgs[2]

		content, err := os.ReadFile(localFile)
		if err != nil {
			log.Fatalf("Failed to read local file %s: %v", localFile, err)
		}
		args.FileContent = content
		CallWithTimeout(hostname, port, args, result)
		log.Printf("\n%s", *result)

		log.Printf("[Client] Successfully Created %s.", args.FileName)

	case "get":
		if len(otherArgs) < 3 {
			log.Fatal("Usage: ./client get <HyDFSfilename> <localfilename>")
		}
		args.FileName = otherArgs[1]
		localFile := otherArgs[2]

		CallWithTimeout(hostname, port, args, result)

		var resp struct {
			Contents    []byte
			ReceiveLog  string
			CompleteLog string
		}
		if err := json.Unmarshal([]byte(*result), &resp); err != nil {
			log.Fatalf("Failed to decode server response: %v", err)
		}

		if err := os.WriteFile(localFile, resp.Contents, 0644); err != nil {
			log.Fatalf("Failed to write to local file %s: %v", localFile, err)
		}

		// Print replica timing info for the client terminal
		log.Println(resp.ReceiveLog)
		log.Println(resp.CompleteLog)
		log.Printf("[Client] Successfully got %s.", args.FileName)

	case "append":
		if len(otherArgs) < 3 {
			log.Fatal("Usage: ./client append <HyDFSfilename> <localfilename>")
		}
		localFile := otherArgs[1]
		args.FileName = otherArgs[2]

		content, err := os.ReadFile(localFile)
		if err != nil {
			log.Fatalf("Failed to read local file %s: %v", localFile, err)
		}
		args.FileContent = content
		CallWithTimeout(hostname, port, args, result)
		log.Printf("\n%s", *result)

		log.Printf("[Client] Successfully appended to %s.", args.FileName)

	case "merge":
		if len(otherArgs) < 2 {
			log.Fatal("Usage: ./client merge <HyDFSfilename>")
		}
		args.FileName = otherArgs[1]

		CallWithTimeout(hostname, port, args, result)
		log.Printf("\n%s", *result)

		log.Printf("[Client] Successfully merged %s.", args.FileName)

	// Debugging
	case "ls":
		if len(otherArgs) < 2 {
			log.Fatal("Usage: ./client ls <HyDFSfilename>")
		}
		args.FileName = otherArgs[1]

		CallWithTimeout(hostname, port, args, result)
		log.Printf("\n%s", *result)
		log.Printf("[Client] Successfully finished ls for %s.", args.FileName)

	case "liststore":
		// args.ClientId = otherArgs[1]

		CallWithTimeout(hostname, port, args, result)
		log.Printf("\n%s", *result)
		log.Printf("[Client] Successfully finished liststore for %s.", args.ClientId)

	case "list_mem_ids":
		if len(otherArgs) < 1 {
			log.Fatal("Usage: ./client list_mem_ids")
		}

		CallWithTimeout(hostname, port, args, result)
		log.Printf("\n%s", *result)
		log.Printf("[Client] Successfully finished list_mem_ids.")

	case "getfromreplica":
		if len(otherArgs) < 4 {
			log.Fatal("Usage: ./client getfromreplica VMAddress HyDFSFileName LocalFileName")
		}
		args.ClientId = otherArgs[1]
		args.FileName = otherArgs[2]
		localFile := otherArgs[3]
		log.Printf("[Client] Successfully got %s from %s.", args.FileName, args.ClientId)

		CallWithTimeout(hostname, port, args, result)

		var resp struct {
			Contents    []byte
			ReceiveLog  string
			CompleteLog string
		}
		if err := json.Unmarshal([]byte(*result), &resp); err != nil {
			log.Fatalf("Failed to decode server response: %v", err)
		}

		if err := os.WriteFile(localFile, resp.Contents, 0644); err != nil {
			log.Fatalf("Failed to write to local file %s: %v", localFile, err)
		}

		// Print replica timing info for the client terminal
		log.Println(resp.ReceiveLog)
		log.Println(resp.CompleteLog)

		log.Printf("[Client] Successfully finished list_mem_ids.")

	case "multiappend":
		if len(otherArgs) < 4 {
			log.Fatal("Usage: ./client multiappend <HyDFSfilename> <vm1,vm2,...> <file1,file2,...>")
		}
		targets := otherArgs[2]
		localFiles := otherArgs[3]

		payload := map[string]string{
			"targets":    targets,
			"localFiles": localFiles,
		}
		jsonBytes, _ := json.Marshal(payload)

		args.FileContent = jsonBytes
		args.FileName = otherArgs[1]

		CallWithTimeout(hostname, port, args, result)
		log.Printf("\n%s", *result)
		log.Printf("[Client] Successfully finished multiappend of %s from %s.", localFiles, targets)

		// TODO:
	default:
		if len(otherArgs) >= 2 {
			hostname = otherArgs[len(otherArgs)-1]
		}
		// all other commands from MP2
		CallWithTimeout(hostname, port, args, result)
		log.Printf("\n%s", *result)
	}

	// TODO: Fix this ordering
	// if otherArgs[0] == "set_drop_rate" && len(otherArgs) >= 3 {
	// 	args.Command = otherArgs[0]
	// 	rate, err := strconv.ParseFloat(otherArgs[1], 64) // "set_drop_rate 0.5"
	// 	if err != nil {
	// 		log.Fatal("Invalid drop rate")
	// 	}
	// 	args.Rate = rate
	// 	hostname = otherArgs[2] // "localhost"
	// } else {
	// 	args.Command = otherArgs[0]
	// 	hostname = otherArgs[1]
	// }

}
