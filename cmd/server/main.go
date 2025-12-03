package main

import (
	"cs425/MP3/internal/gossip"
	"cs425/MP3/internal/hashring"
	"cs425/MP3/internal/member"
	"cs425/MP3/internal/swim"
	"cs425/MP3/internal/utils"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var N_replicas int = 3

const StorageDir = "/cs425/mp3/charl_files"
const DemoDir = "/cs425/mp3/demo_inputs/business"

func ensureStorageDir() {
	if err := os.RemoveAll(StorageDir); err != nil {
		log.Printf("[HyDFS] Warning: failed to remove old storage dir: %v", err)
	}
	if err := os.MkdirAll(StorageDir, 0777); err != nil {
		log.Fatalf("[HyDFS] Failed to create storage dir %s: %v", StorageDir, err)
	} else {
		log.Printf("[HyDFS] Created storage dir %s successfully", StorageDir)
	}
}

func startMembership() (*Server, net.Listener) {
	// make sure that "/cs425/mp3/data" exists
	ensureStorageDir()

	// some default parameters
	cliPort := 12346
	serverPort := utils.DEFAULT_PORT
	Tround := time.Second / 10
	Tsuspect := Tround * 10
	Tfail := Tround * 10
	Tcleanup := Tround * 300 // don't cleanup too fast, I need this to record FP rate and other stuff
	TpingFail := Tround * 5
	TpingReqFail := Tround * 5

	// parse command line arguments
	if len(os.Args) >= 2 {
		for i := 1; i < len(os.Args); i++ {
			switch os.Args[i] {
			case "-p":
				if i+1 >= len(os.Args) {
					fmt.Println("Error: -p flag requires a port number argument.")
					os.Exit(1)
				}
				_, err := fmt.Sscanf(os.Args[i+1], "%d", &serverPort)
				if err != nil {
					fmt.Printf("Error: invalid port number %s\n", os.Args[i+1])
					os.Exit(1)
				}
				i++
			default:
				fmt.Printf("Warning: unknown argument %s\n", os.Args[i])
			}
		}
	}

	// setup logger
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	// setup my info and membership list
	hostname, err := utils.GetHostName()
	if err != nil {
		log.Fatalf("Failed to get hostname: %s", err.Error())
	}
	myInfo := member.Info{
		Hostname:  hostname,
		Port:      serverPort,
		Version:   time.Now(),
		Timestamp: time.Now(),
		Counter:   0,
		State:     member.Alive,
	}
	myId := member.HashInfo(myInfo)

	log.Printf("My ID: %d, Hostname: %s, Port: %d\n", myId, myInfo.Hostname, myInfo.Port)
	membership := member.Membership{
		InfoMap: map[uint64]member.Info{
			myId: myInfo,
		},
		Members: []uint64{myId},
	}

	// create gossip instance
	myGossip := gossip.NewGossip(&membership)
	// create swim instance
	mySwim := swim.NewSwim(&membership)
	// create server instance
	myServer := Server{
		Tround:        Tround,
		Tsuspect:      Tsuspect,
		Tfail:         Tfail,
		TpingFail:     TpingFail,
		TpingReqFail:  TpingReqFail,
		Tcleanup:      Tcleanup,
		Id:            myId,
		K:             3, // ping req to 3 other member
		Info:          myInfo,
		membership:    &membership,
		gossip:        myGossip,
		swim:          mySwim,
		state:         Init,
		suspicionMode: true, // suspicion enabled by default
		dropRate:      0.0,  // no message dropping by default
		appendLogs:    make(map[string][]AppendEvent),
		mergeList:     make(map[string][]AppendEvent),
	}

	// register rpc server for CLI function
	rpc.Register(&myServer)
	tcpListener, err := net.Listen("tcp", fmt.Sprintf(":%d", cliPort))
	if err != nil {
		log.Fatalf("TCP Listen failed: %s", err.Error())
	}
	go func() {
		log.Printf("TCP RPC service listening on port %d", cliPort)
		rpc.Accept(tcpListener)
	}()

	go myServer.Work()

	return &myServer, tcpListener
}

func main() {
	time.Sleep(4 * time.Second)
	// start membership protocol
	server, tcpListener := startMembership()
	log.Println("---------------------------------------------- Started MP3 ---------------------------------------------- ")

	ring := hashring.NewHashRing(N_replicas) // N replicas per file
	server.ring = ring

	ring.UpdateRing(server.membership)
	// figure out how to hash files to their location

	go func() {
		// var prevFailCounter uint64 = 0
		// var prevJoinCounter uint64 = 0

		for {
			// server.lock.RLock()
			// currFailCounter := server.membership.FailureCounter
			// currJoinCounter := server.membership.JoinCounter
			// server.lock.RUnlock()

			server.lock.Lock()
			ring.UpdateRing(server.membership)
			server.lock.Unlock()
			go server.ReplicateFiles()
			go server.RebalanceFiles()

			// TODO: Come up with a better way to update ring

			// if prevFailCounter < currFailCounter {
			// 	log.Printf("[HyDFS] Detected failure version change (%d → %d), updating ring...", prevFailCounter, currFailCounter)
			// 	server.lock.Lock()
			// 	ring.UpdateRing(server.membership)
			// 	server.lock.Unlock()
			//
			// 	prevFailCounter = currFailCounter
			// 	go server.ReplicateFiles()
			// }
			//
			// if prevJoinCounter < currJoinCounter {
			// 	log.Printf("[HyDFS] Detected join version change (%d → %d), updating ring...", prevJoinCounter, currJoinCounter)
			// 	ring.UpdateRing(server.membership)
			// 	prevJoinCounter = currJoinCounter
			// 	go server.RebalanceFiles()
			// }

			time.Sleep(1 * time.Second)
		}
	}()

	// handle syscall SIGTERM
	// Block until Ctrl+C
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig

	// close the TCP connection for the membership protocol after we end HyDFS
	tcpListener.Close()
}
