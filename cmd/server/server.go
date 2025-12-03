package main

import (
	"cs425/MP3/internal/flow"
	"cs425/MP3/internal/gossip"
	"cs425/MP3/internal/hashring"
	"cs425/MP3/internal/member"
	"cs425/MP3/internal/swim"
	"cs425/MP3/internal/utils"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

type ServerState int

const (
	Gossip ServerState = iota
	Swim
	Failed
	Init
)

const ProbeTimeout time.Duration = 2 * time.Second

type Server struct {
	Tround        time.Duration            // Duration of a round
	Tsuspect      time.Duration            // suspect time for gossip
	Tfail         time.Duration            // fail time for gossip
	TpingFail     time.Duration            // direct ping fail time for swim
	TpingReqFail  time.Duration            // indirect ping fail time for swim
	Tcleanup      time.Duration            // time to cleanup failed member's information
	Id            uint64                   // node's ID
	K             int                      // k for swim
	Info          member.Info              // node's own information
	membership    *member.Membership       // member ship information
	gossip        *gossip.Gossip           // gossip instance
	swim          *swim.Swim               // swim instance
	ring          *hashring.HashRing       // hashring instance
	state         ServerState              // server's state
	suspicionMode bool                     // whether suspicion mechanism is enabled
	dropRate      float64                  // message drop rate (0.0 to 1.0)
	lock          sync.RWMutex             // mutex for server's property
	InFlow        flow.FlowCounter         // input network counter
	OutFlow       flow.FlowCounter         // output network counter
	appendLogs    map[string][]AppendEvent // filename -> (clientId, timestamp, appendContent)
	mergeList     map[string][]AppendEvent // filename -> (clientId, timestamp)} (used for merging)
	Files         []string                 // list of filesnames on the current server
}

// arguments for cli tool
type Args struct {
	Command       string
	Rate          float64
	FileName      string
	FileContent   []byte
	LocalFileName string
	Time          int64
	ClientId      string
}

type AppendEvent struct {
	ClientID  string
	Timestamp int64
	Content   []byte
}

// filename -> most recent
//client, replica, filename
// mostuptodatereplica[filename][client] = replica, for this file and this client thats reading, this should be the replica that is most up to date
// read -> initial request

// (clientid, localtimestampatthisclient)
// s.appendLogs[fileName][clientid] = [(timestamp, appendContent), (timestamp, appendContent)]
// s.appendLogs[fileName] = [((clientid, timestamp), appendContent), (timestamp, appendContent)]
// send it to each replicates (overwrite function to replca)

func (s *Server) GetId(args Args, reply *uint64) error {
	s.lock.RLock()
	defer s.lock.RUnlock()
	*reply = s.Id
	return nil
}

func (s *Server) MemberTable(args Args, reply *map[uint64]member.Info) error {
	res := s.membership.GetInfoMap()
	*reply = res
	return nil
}

func (s *Server) GetTotalFlow(args Args, reply *float64) error {
	*reply = s.InFlow.Get() + s.OutFlow.Get()
	return nil
}

func (s *Server) GetInFlow(args Args, reply *float64) error {
	*reply = s.InFlow.Get()
	return nil
}

func (s *Server) GetOutFlow(args Args, reply *float64) error {
	*reply = s.OutFlow.Get()
	return nil
}

func getHostName() string {
	name, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}
	return name
}

const (
	CONNECTION_TIMEOUT = 1 * time.Second
	CALL_TIMEOUT       = 1 * time.Second
)

func (s *Server) CLI(args Args, reply *string) error {
	// for CLI tool
	// log.Printf("received cli command: %s", args.Command)

	switch args.Command {
	case "status":
		s.lock.RLock()
		state := s.state
		dropRate := s.dropRate
		susMode := s.suspicionMode
		s.lock.RUnlock()
		*reply = fmt.Sprintf("Current protocol: %s, suspect mode: %v, drop rate: %f, Input: %f bytes/s, Output %f bytes/s\n", s.getStateString(state), susMode, dropRate, s.InFlow.Get(), s.OutFlow.Get())
		*reply = *reply + s.membership.Table(false)
	case "list_mem":
		*reply = "\n" + s.membership.Table(false)
	case "list_self":
		*reply = fmt.Sprintf("Self ID: %d, Hostname: %s, Port: %d", s.Id, s.Info.Hostname, s.Info.Port)
	case "join":
		*reply = "Already in group"
	case "stop":
		*reply = "Stopping the server"
		defer os.Exit(1)
	case "leave":
		s.leaveGroup()
		*reply = "Left the group voluntarily"
	case "display_suspects":
		*reply = s.getSuspectedNodes()
	case "display_protocol":
		s.lock.RLock()
		state := s.state
		suspicionMode := s.suspicionMode
		s.lock.RUnlock()
		protocol := "gossip"
		if state == Swim {
			protocol = "ping"
		}
		suspicion := "nosuspect"
		if suspicionMode {
			suspicion = "suspect"
		}
		*reply = fmt.Sprintf("<%s, %s>", protocol, suspicion)
	case "set_drop_rate":
		*reply = s.handleDropRateCommand(args)

	case "create":
		*reply = s.handleCreateFile(args.FileName, args.FileContent, args.ClientId, args.Time)

	case "get":
		*reply = s.handleGetFile(args.FileName, args.ClientId)

	case "append":
		*reply = s.handleAppendFile(args.FileName, args.FileContent, args.ClientId, args.Time)

	case "merge":
		*reply = s.handleMergeReplicas(args.FileName)

	case "ls":
		fileID := hashring.HashFile(args.FileName)
		replicas := s.ring.GetReplicas(args.FileName)

		var b strings.Builder
		fmt.Fprintf(&b, "FileID for %s: %d\n", args.FileName, fileID)
		fmt.Fprintln(&b, "Replicas on ring:")

		// For each replica, find its ring ID by matching Hostname to InfoMap
		for i, r := range replicas {
			var nodeID uint64
			for id, info := range s.membership.InfoMap {
				if info.Hostname == r.Hostname {
					nodeID = id
					break
				}
			}
			fmt.Fprintf(&b, "  %d. %s (ID: %d)\n", i+1, r.Hostname, nodeID)
		}

		*reply = b.String()

	case "liststore":
		serializedList := new(string)

		CallWithTimeout(s.Info.Hostname, 12346, "Server.GetListStore",
			Args{}, serializedList)

		type FileInfo struct {
			FileName string `json:"filename"`
			FileID   uint64 `json:"fileID"`
		}

		var fileInfos []FileInfo
		if err := json.Unmarshal([]byte(*serializedList), &fileInfos); err != nil {
			*reply = fmt.Sprintf("[HyDFS] Failed to decode file list: %v", err)
			return nil
		}

		var b strings.Builder
		fmt.Fprintf(&b, "Files stored on %s:\n", s.Info.Hostname)
		for i, f := range fileInfos {
			fmt.Fprintf(&b, "  %d. %s (FileID: %d)\n", i+1, f.FileName, f.FileID)
		}

		*reply = b.String()

	case "list_mem_ids":
		*reply = "\n" + s.membership.Table(true)

	case "getfromreplica":
		fileResult := new(string)
		CallWithTimeout(args.ClientId, 12346, "Server.GetFile",
			Args{FileName: args.FileName}, fileResult)
		*reply = *fileResult

		return nil

	case "multiappend":
		var payload map[string]string
		if err := json.Unmarshal(args.FileContent, &payload); err != nil {
			*reply = fmt.Sprintf("[HyDFS] Invalid multiappend payload: %v", err)
			return nil
		}

		// VMi,...VMj
		targets := strings.Split(payload["targets"], ",")

		// localfilei,...localfilej
		localFiles := strings.Split(payload["localFiles"], ",")

		fileName := args.FileName

		clientTime := args.Time

		if len(targets) != len(localFiles) {
			*reply = "[HyDFS] multiappend: number of VMs and files must match"
			return nil
		}

		var wg sync.WaitGroup
		var mu sync.Mutex
		results := make([]string, 0, len(targets))

		for i := range targets {
			wg.Add(1)
			go func(targetVM, localFile string) {
				defer wg.Done()
				start := time.Now()

				mu.Lock()
				results = append(results, fmt.Sprintf("[HyDFS] Multiappend: Starting append from %s of %s at %v\n",
					targetVM, localFile, start.Format("15:04:05")))
				mu.Unlock()

				result := new(string)
				CallWithTimeout(targetVM, 12346, "Server.RemoteAppendRequest",
					Args{FileName: fileName, LocalFileName: localFile, ClientId: targetVM, Time: clientTime}, result)

				mu.Lock()
				results = append(results, fmt.Sprintf("[HyDFS] Output: → %s: %s\n", targetVM, *result))
				results = append(results, fmt.Sprintf("[HyDFS] Finished append from %s at %v\n",
					targetVM, time.Now().Format("15:04:05")))
				mu.Unlock()
			}(targets[i], localFiles[i])
		}

		wg.Wait()

		// Combine results into one response string
		var b strings.Builder
		fmt.Fprintf(&b, "[HyDFS] Multiappend completed for %s across %d targets\n", fileName, len(targets))
		for _, res := range results {
			fmt.Fprintf(&b, "%s\n", res)
		}

		*reply = b.String()

		return nil

	default:
		// Check for switch command with parameters
		if len(args.Command) > 6 && args.Command[:6] == "switch" {
			*reply = s.handleSwitchCommand(args.Command)
		} else {
			*reply = "Unknown command. Available commands: gossip, swim, status, list, self, rejoin, leave, display_suspects, display_protocol, switch(protocol,suspicion), set_drop_rate, list_ring"
		}
	}
	return nil
}

func (s *Server) getStateString(state ServerState) string {
	switch state {
	case Gossip:
		return "Gossip"
	case Swim:
		return "SWIM"
	case Failed:
		return "Failed"
	case Init:
		return "Init"
	default:
		return "Unknown"
	}
}

func (s *Server) leaveGroup() {
	s.lock.Lock()
	defer s.lock.Unlock()

	// Notify all members about voluntary leave
	infoMap := s.membership.GetInfoMap()
	for _, info := range infoMap {
		if info.State == member.Alive && info.Hostname != s.Info.Hostname {
			leaveMessage := utils.Message{
				Type:       utils.Leave,
				SenderInfo: s.Info,
				TargetInfo: info,
				InfoMap:    make(map[uint64]member.Info), // Empty map
			}
			utils.SendMessage(leaveMessage, info.Hostname, info.Port)
		}
	}

	// Update local state
	s.state = Failed
	log.Printf("Voluntarily left the group")
}

func (s *Server) getSuspectedNodes() string {
	infoMap := s.membership.GetInfoMap()
	suspected := []string{}

	for _, info := range infoMap {
		if info.State == member.Suspected {
			suspected = append(suspected, fmt.Sprintf("ID: %d, Hostname: %s, Port: %d",
				member.HashInfo(info), info.Hostname, info.Port))
		}
	}

	if len(suspected) == 0 {
		return "No suspected nodes"
	}

	result := "Suspected nodes:\n"
	for _, node := range suspected {
		result += node + "\n"
	}
	return result
}

func (s *Server) handleSwitchCommand(command string) string {
	// Parse switch(protocol, suspicion) command
	// Expected format: switch(gossip,suspect) or switch(ping,nosuspect)

	// Remove "switch(" and ")"
	if len(command) < 8 || command[:6] != "switch" {
		return "Invalid switch command format. Use: switch(protocol, suspicion)"
	}

	// Find the parameters inside parentheses
	start := 6
	if command[start] != '(' {
		return "Invalid switch command format. Use: switch(protocol, suspicion)"
	}

	end := len(command) - 1
	if command[end] != ')' {
		return "Invalid switch command format. Use: switch(protocol, suspicion)"
	}

	params := command[start+1 : end]
	parts := strings.Split(params, ",")
	if len(parts) != 2 {
		return "Invalid switch command format. Use: switch(protocol, suspicion)"
	}

	protocol := strings.TrimSpace(parts[0])
	suspicion := strings.TrimSpace(parts[1])

	// Switch protocol and suspicion mode
	s.lock.Lock()
	defer s.lock.Unlock()

	switch protocol {
	case "gossip":
		s.state = Gossip
		switch suspicion {
		case "suspect":
			s.suspicionMode = true
			s.sendSwitchMessage(utils.UseGossipSus, "")
		case "nosuspect":
			s.suspicionMode = false
			s.sendSwitchMessage(utils.UseGossipNoSus, "")
		default:
			return "Invalid suspicion mode. Use 'suspect' or 'nosuspect', got " + protocol + " " + suspicion
		}
	case "ping":
		s.state = Swim
		switch suspicion {
		case "suspect":
			s.suspicionMode = true
			s.sendSwitchMessage(utils.UseSwimSus, "")
		case "nosuspect":
			s.suspicionMode = false
			s.sendSwitchMessage(utils.UseSwimNoSus, "")
		default:
			return "Invalid suspicion mode. Use 'suspect' or 'nosuspect', got " + protocol + " " + suspicion
		}
	default:
		return "Invalid protocol. Use 'gossip' or 'ping'"
	}

	log.Printf("Switched to %s protocol with %s suspicion", protocol, suspicion)
	return fmt.Sprintf("Switched to %s protocol with %s suspicion", protocol, suspicion)
}

func (s *Server) handleDropRateCommand(args Args) string {
	if args.Rate < 0.0 || args.Rate > 1.0 {
		return "Drop rate must be between 0.0 and 1.0"
	}

	s.lock.Lock()
	s.dropRate = args.Rate
	s.lock.Unlock()

	return fmt.Sprintf("Set drop rate to %.2f", args.Rate)
}

func (s *Server) sendSwitchMessage(messageType utils.MessageType, excludeHostname string) {
	// Get all members and send switch message to each
	infoMap := s.membership.GetInfoMap()
	for _, info := range infoMap {
		// Skip excluded hostname (for forwarding)
		if excludeHostname != "" && info.Hostname == excludeHostname {
			continue
		}
		if info.State == member.Alive {
			switchMessage := utils.Message{
				Type:       messageType,
				SenderInfo: s.Info,
				TargetInfo: info,
				InfoMap:    infoMap,
			}
			_, err := utils.SendMessage(switchMessage, info.Hostname, info.Port)
			if err != nil {
				log.Printf("Failed to send switch message to %s: %s", info.String(), err.Error())
			} else {
				// s.OutFlow.Add(size) // only count
				log.Printf("Sent switch message to %s", info.String())
			}
		}
	}
}

func (s *Server) handleSwitchMessage(message utils.Message) {
	s.lock.Lock()
	defer s.lock.Unlock()

	// Merge membership information first
	// s.membership.Merge(message.InfoMap, time.Now())

	// Check if we need to switch protocols
	var newState ServerState
	var newSusMode bool
	switch message.Type {
	case utils.UseGossipSus:
		newState = Gossip
		newSusMode = true
	case utils.UseGossipNoSus:
		newState = Gossip
		newSusMode = false
	case utils.UseSwimSus:
		newState = Swim
		newSusMode = true
	case utils.UseSwimNoSus:
		newState = Swim
		newSusMode = false
	default:
		log.Printf("Unknown switch message type: %v", message.Type)
		return
	}
	s.suspicionMode = newSusMode
	// Only switch if we're not already in the target state
	if s.state != newState {
		log.Printf("Received switch message to %s from %s", s.getStateString(newState), message.SenderInfo.String())
		s.state = newState

		// Forward the switch message to other members (gossip-style propagation)
		s.sendSwitchMessage(message.Type, message.SenderInfo.Hostname)
	} else {
		log.Printf("Already using %s protocol, ignoring switch message", s.getStateString(newState))
	}
}

func (s *Server) joinGroup() { // the init join
	s.lock.Lock()
	defer s.lock.Unlock()

	if getHostName() == utils.HOSTS[0] {
		log.Printf("I am introducer. Starting with gossip")
		s.state = Gossip
		return
	}

	addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf(":%d", s.Info.Port))
	if err != nil {
		log.Fatalf("UDP ResolveAddr failed: %s", err.Error())
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		log.Fatalf("UDP listen failed: %s", err.Error())
	}
	defer conn.Close()
	conn.SetReadDeadline(time.Now().Add(ProbeTimeout))

	// Send message to 10 other introducer
	message := utils.Message{
		Type:       utils.Probe,
		SenderInfo: s.Info,
		InfoMap:    s.membership.GetInfoMap(),
	}
	for _, hostname := range utils.HOSTS[:1] { // use first node as a introducer
		_, err := utils.SendMessage(message, hostname, utils.DEFAULT_PORT)
		if err != nil {
			log.Printf("Failed to send probe message to %s:%d: %s", hostname, utils.DEFAULT_PORT, err.Error())
		} // else {
		// 	s.OutFlow.Add(size) // only count protocol message
		// }
	}
	// wait for probe ack
	buffer := make([]byte, 4096)
	for {
		_, _, err := conn.ReadFromUDP(buffer)
		if err != nil {
			log.Printf("Failed to get probe ack, stopping")
			os.Exit(1)
			// s.state = Gossip
			// break
		}
		// s.InFlow.Add(int64(size))
		message, err := utils.Deserialize(buffer)
		if err != nil {
			log.Printf("Failed to deserialize message: %s", err.Error())
			continue
		}
		if message.Type == utils.ProbeAckGossip {
			s.state = Gossip
			s.membership.Merge(message.InfoMap, time.Now())
			break
		} else if message.Type == utils.ProbeAckSwim {
			s.state = Swim
			s.membership.Merge(message.InfoMap, time.Now())
			break
		}
	}
}

func (s *Server) startUDPListenerLoop(waitGroup *sync.WaitGroup) {
	defer waitGroup.Done()
	addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf(":%d", s.Info.Port))
	if err != nil {
		log.Fatalf("UDP ResolveAddr failed: %s", err.Error())
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		log.Fatalf("UDP listen failed: %s", err.Error())
	}
	defer conn.Close()
	log.Printf("UDP service listening on port %d", s.Info.Port)

	data := make([]byte, 4096)
	for {
		size, _, err := conn.ReadFromUDP(data)
		if err != nil {
			log.Printf("UDP Read error: %s", err.Error())
			continue
		}
		// ignore all message when failed or leave group
		s.lock.RLock()
		currentState := s.state
		s.lock.RUnlock()
		if currentState == Failed {
			continue
		}
		message, err := utils.Deserialize(data)
		if err != nil {
			log.Printf("Failed to deserialize message: %s", err.Error())
		} else {
			switch message.Type {
			case utils.Gossip, utils.Ping, utils.PingReq, utils.Pong:
				// only apply message drop and flow counter to protocol messages
				s.lock.RLock()
				dropRate := s.dropRate
				s.lock.RUnlock()

				if dropRate > 0 && rand.Float64() < dropRate {
					// log.Printf("Dropping message (drop rate: %.2f)", dropRate)
					continue
				}
				s.InFlow.Add(int64(size))

				switch message.Type {
				case utils.Gossip:
					s.gossip.HandleIncomingMessage(message)
				case utils.Ping, utils.Pong, utils.PingReq:
					size := s.swim.HandleIncomingMessage(message, s.Info)
					s.OutFlow.Add(size)
				}
			case utils.Leave:
				// Handle voluntary leave - remove the sender from membership
				senderId := member.HashInfo(message.SenderInfo)
				s.membership.RemoveMember(senderId)
				log.Printf("Node %s voluntarily left the group", message.SenderInfo.String())
			case utils.Probe:
				// someone want to join the group, send some information back
				log.Printf("Receive probe message from: %s:%d", message.SenderInfo.Hostname, message.SenderInfo.Port)
				s.membership.Merge(message.InfoMap, time.Now()) // merge new member
				messageType := utils.ProbeAckGossip
				s.lock.RLock()
				if s.state == Swim {
					messageType = utils.ProbeAckSwim
				}
				s.lock.RUnlock()
				ackMessage := utils.Message{
					Type:       messageType,
					SenderInfo: s.Info,
					TargetInfo: message.SenderInfo,
					InfoMap:    s.membership.GetInfoMap(),
				}
				size, err := utils.SendMessage(ackMessage, message.SenderInfo.Hostname, message.SenderInfo.Port)
				if err != nil {
					log.Printf("Failed to send probe ack message to %s: %s", message.SenderInfo.String(), err.Error())
				} else {
					s.OutFlow.Add(size)
					log.Printf("Welcome, send probe ack message to %s:%d: %v", message.SenderInfo.Hostname, message.SenderInfo.Port, ackMessage)
				}
			case utils.UseSwimSus, utils.UseGossipSus, utils.UseSwimNoSus, utils.UseGossipNoSus:
				// Handle algorithm switch messages
				s.handleSwitchMessage(message)
			default:
				// ignore probe ack, since the node should already join the group
			}
		}
	}
}

func (s *Server) startFailureDetectorLoop(waitGroup *sync.WaitGroup) {
	defer waitGroup.Done()
	// create ticker
	ticker := time.NewTicker(s.Tround)
	defer ticker.Stop()
	log.Printf("Failure detector stared with a period of %s", s.Tround.String())

	// The main event loop
	for range ticker.C {
		s.lock.RLock()
		suspicionMode := s.suspicionMode
		state := s.state
		s.lock.RUnlock()

		infoMap := s.membership.GetInfoMap()
		if len(infoMap) == 1 && getHostName() != utils.HOSTS[0] {
			log.Fatal("The node might failed, restarting")
		}

		if !s.membership.Exists(utils.HOSTS[0], utils.DEFAULT_PORT) {
			log.Fatal("Isolated from the master, restarting")
		}

		switch state {
		case Swim:
			size := s.swim.SwimStep(s.Id, s.Info, s.K, s.TpingFail, s.TpingReqFail, s.Tcleanup, suspicionMode)
			s.OutFlow.Add(size)
		case Gossip:
			size := s.gossip.GossipStep(s.Id, s.Tfail, s.Tsuspect, s.Tcleanup, suspicionMode)
			s.OutFlow.Add(size)
		}
		// TODO: change period
	}
}

// If the primaryReplica's Files contains the fileName already, then we should not create this file
func (s *Server) DoesFileExist(fileName string, replicas []member.Info) bool {
	primaryReplica := replicas[0]
	serializedList := new(string)

	var primaryReplicaFiles []string
	CallWithTimeout(primaryReplica.Hostname, 12346, "Server.GetFileList",
		Args{}, serializedList)
	json.Unmarshal([]byte(*serializedList), &primaryReplicaFiles)

	for _, currFileName := range primaryReplicaFiles {
		if currFileName == fileName {
			return true
		}
	}
	return false
}

func unionAppendEvents(local, remote []AppendEvent) []AppendEvent {
	seen := make(map[string]bool)
	merged := make([]AppendEvent, 0, len(local)+len(remote))

	for _, e := range local {
		key := fmt.Sprintf("%s_%d", e.ClientID, e.Timestamp)
		seen[key] = true
		merged = append(merged, e)
	}
	for _, e := range remote {
		key := fmt.Sprintf("%s_%d", e.ClientID, e.Timestamp)
		if !seen[key] {
			seen[key] = true
			merged = append(merged, e)
		}
	}
	return merged
}
func (s *Server) ReplicateFiles() {
	// Ask all alive surviving nodes what files they currently store (can be better if have time)
	allFiles := make(map[string]bool)
	infoMap := s.membership.GetInfoMap()
	for _, info := range infoMap {
		if info.State != member.Alive {
			continue
		}

		result := new(string)
		CallWithTimeout(info.Hostname, 12346, "Server.GetFileList", Args{}, result)

		if *result == "" {
			continue
		}

		var fileList []string
		if err := json.Unmarshal([]byte(*result), &fileList); err == nil {
			for _, f := range fileList {
				allFiles[f] = true
			}
		}

		for fileName := range allFiles {
			newReplicas := s.ring.GetReplicas(fileName)

			// 2. Check if THIS node is now one of the replicas for that file (note that all non-faulty nodes will detect a failure and call this function in the go func() of the server main())
			shouldHave := false
			for _, r := range newReplicas {
				if r.Hostname == s.Info.Hostname {
					shouldHave = true
					break
				}
			}

			// 3. Check if this node already has the file
			alreadyHas := false

			for _, f := range s.Files {
				if f == fileName {
					// log.Printf("[HyDFS] Already have %s (failed node: %s)...", f, fileName)
					alreadyHas = true
					break
				}
			}

			if shouldHave && !alreadyHas {
				// 4. Pull file content from another existing replica (already guaranteed that the current node is the new replica)
				log.Printf("[HyDFS] Node %s now responsible for %s; pulling from replicas...", s.Info.Hostname, fileName)
				var mergedAppendLog []AppendEvent

				logResult := new(string)
				primaryReplica := newReplicas[0]
				if primaryReplica.Hostname != s.Info.Hostname {
					CallWithTimeout(primaryReplica.Hostname, 12346, "Server.GetAppendLog",
						Args{FileName: fileName}, logResult)

					if *logResult != "" {
						var remoteAppendLog []AppendEvent
						if err := json.Unmarshal([]byte(*logResult), &remoteAppendLog); err == nil {
							mergedAppendLog = unionAppendEvents(mergedAppendLog, remoteAppendLog)
						} else {
							log.Printf("[HyDFS] Failed to parse appendLog from %s: %v", primaryReplica.Hostname, err)
						}
					}

				} else {
					// Pull from all replicas, as the new node joined as the leader (may be the first replica in the replica list of the file), so we should to union all appendLogs across the other replicas
					for _, src := range newReplicas {
						if src.Hostname == s.Info.Hostname || src.State == member.Failed {
							continue
						}

						log.Printf("%s is attempting to GetAppendLog from: %s", s.Info.Hostname, src.Hostname)
						logResult := new(string)

						// 3. Try fetching appendLog metadata
						CallWithTimeout(src.Hostname, 12346, "Server.GetAppendLog",
							Args{FileName: fileName}, logResult)
						if *logResult != "" {
							var remoteAppendLog []AppendEvent
							if err := json.Unmarshal([]byte(*logResult), &remoteAppendLog); err == nil {
								mergedAppendLog = unionAppendEvents(mergedAppendLog, remoteAppendLog)
							} else {
								log.Printf("[HyDFS] Failed to parse appendLog from %s: %v", src.Hostname, err)
							}
						}
					}
				}

				if s.appendLogs[fileName] == nil {
					s.appendLogs[fileName] = make([]AppendEvent, 0)
				}

				// Step 5: save locally
				s.appendLogs[fileName] = mergedAppendLog
				s.Files = append(s.Files, fileName)
				s.writeFileFromLog(fileName, mergedAppendLog)
				log.Printf("[HyDFS] Re-replicated file %s onto %s", fileName, s.Info.Hostname)

				s.handleMergeReplicas(fileName)
			}
		}
	}

}

func (s *Server) RebalanceFiles() {
	// 1. Discover all files that exist anywhere in the cluster
	allFiles := make(map[string]bool)

	infoMap := s.membership.GetInfoMap()
	for _, info := range infoMap {
		if info.State != member.Alive {
			continue
		}

		result := new(string)
		CallWithTimeout(info.Hostname, 12346, "Server.GetFileList", Args{}, result)
		if *result == "" {
			continue
		}

		var fileList []string
		if err := json.Unmarshal([]byte(*result), &fileList); err != nil {
			log.Printf("[HyDFS] Failed to decode file list from %s: %v", info.Hostname, err)
			continue
		}

		for _, fileName := range fileList {
			allFiles[fileName] = true
		}
	}

	for fileName := range allFiles {
		newReplicas := s.ring.GetReplicas(fileName)

		// Check if THIS node should store it
		shouldHave := false
		for _, r := range newReplicas {
			if r.Hostname == s.Info.Hostname {
				shouldHave = true
				break
			}
		}

		alreadyHas := false
		for _, f := range s.Files {
			if f == fileName {
				// log.Printf("[HyDFS] Already have %s (failed node: %s)...", f, fileName)
				alreadyHas = true
				break
			}
		}

		// ---- CASE 1: Node no longer a replica of the file ----
		if !shouldHave && alreadyHas {
			// Remove the file from this node
			s.removeFileMetadata(fileName)
			continue

		}

		// ---- CASE 2: Node should have the file but doesn't ---- (is a replica but doesn't have the file yet)
		if shouldHave && !alreadyHas {
			var mergedAppendLog []AppendEvent

			// Step 2a: Gather appendLogs from surviving replicas
			logResult := new(string)
			primaryReplica := newReplicas[0]
			if primaryReplica.Hostname != s.Info.Hostname {
				CallWithTimeout(primaryReplica.Hostname, 12346, "Server.GetAppendLog",
					Args{FileName: fileName}, logResult)
				if *logResult != "" {
					var remoteAppendLog []AppendEvent
					if err := json.Unmarshal([]byte(*logResult), &remoteAppendLog); err == nil {
						mergedAppendLog = unionAppendEvents(mergedAppendLog, remoteAppendLog)
					} else {
						log.Printf("[HyDFS] Failed to parse appendLog from %s: %v", primaryReplica.Hostname, err)
					}
				}
			} else {
				// Pull from all replicas, as the new node joined as the leader (may be the first replica in the replica list of the file), so we should to union all appendLogs across the other replicas
				for _, src := range newReplicas {
					if src.Hostname == s.Info.Hostname || src.State == member.Failed {
						continue
					}

					logResult := new(string)
					CallWithTimeout(src.Hostname, 12346, "Server.GetAppendLog",
						Args{FileName: fileName}, logResult)
					if *logResult == "" {
						continue
					}

					var remoteAppendLog []AppendEvent
					if err := json.Unmarshal([]byte(*logResult), &remoteAppendLog); err == nil {
						mergedAppendLog = unionAppendEvents(mergedAppendLog, remoteAppendLog)
					} else {
						log.Printf("[HyDFS] Failed to parse appendLog from %s: %v", src.Hostname, err)
					}
				}
			}

			// Step 2b: Store metadata locally
			if s.appendLogs[fileName] == nil {
				s.appendLogs[fileName] = make([]AppendEvent, 0)
			}

			// Step 5: save locally
			s.appendLogs[fileName] = mergedAppendLog
			s.Files = append(s.Files, fileName)
			s.writeFileFromLog(fileName, mergedAppendLog)
			log.Printf("[HyDFS] Re-replicated file %s onto %s", fileName, s.Info.Hostname)

			// Step 2c: Run merge to synchronize all replicas
			s.handleMergeReplicas(fileName)
		}
	}
}
func (s *Server) removeFileMetadata(fileName string) {
	newList := []string{}
	for _, f := range s.Files {
		if f != fileName {
			newList = append(newList, f)
		}
	}
	s.Files = newList

	// Remove from appendLogs and mergeList
	delete(s.appendLogs, fileName)
	delete(s.mergeList, fileName)
}

func (s *Server) Work() {
	// join group
	s.joinGroup()
	waitGroup := new(sync.WaitGroup)
	// start UDP listener
	waitGroup.Add(1)
	go s.startUDPListenerLoop(waitGroup)
	// start failure detector
	waitGroup.Add(1)
	go s.startFailureDetectorLoop(waitGroup)
	waitGroup.Wait()
}
