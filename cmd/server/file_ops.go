package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"time"

	"cs425/MP3/internal/hashring"
	"cs425/MP3/internal/member"
)

func CallWithTimeout(
	hostname string,
	port int,
	method string,
	args Args,
	result *string) {

	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", hostname, port), CONNECTION_TIMEOUT)
	if err != nil {
		log.Printf("Failed to dial server %s:%d: %s\n", hostname, port, err.Error())
		return
	}
	defer conn.Close()

	client := rpc.NewClient(conn)
	callChan := make(chan error, 1)

	go func() {
		callChan <- client.Call(method, args, result)
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

func (s *Server) handleCreateFile(fileName string, content []byte, clientId string, clientTime int64) string {
	// replicate to successors
	replicas := s.ring.GetReplicas(fileName)
	output := ""

	// 1. Fail if the file already exists in HyDFS.
	if s.DoesFileExist(fileName, replicas) {
		errMsg := fmt.Sprintf("[HyDFS] File '%s' already exists in HyDFS (metadata). Skipping create.", fileName)
		log.Println(errMsg)
		output = errMsg
		return output
	}

	result := ""
	for _, r := range replicas {
		// port 12345 is for tcp connections on server, 8787 is for udp
		go func(r member.Info) {
			tempResult := new(string)
			CallWithTimeout(r.Hostname, 12346, "Server.CreateFile",
				Args{FileName: fileName, FileContent: []byte(content), ClientId: clientId, Time: clientTime}, tempResult)
			log.Printf("\n%s", *tempResult)
			result += *tempResult + "\n"
		}(r)
	}

	if !strings.Contains(result, "Failed") {
		output = fmt.Sprintf("Created and replicated %s on %d nodes:\n", fileName, len(replicas))
	} else {
		output = fmt.Sprintf("At least one node failed to create the file!")

	}
	// Stop once we successfully retrieve content

	// Iterate over the replicas slice
	for i, replica := range replicas {
		// Append each replica to the result string, perhaps numbered
		output += fmt.Sprintf("  %d. %s\n", i+1, replica.Hostname)
	}

	return output
}

func (s *Server) CreateFile(args Args, reply *string) error {
	log.Printf("Attempting to create file: %s on node: %s", args.FileName, s.Info.Hostname)

	fileName := args.FileName
	fileContent := args.FileContent

	filePath := fmt.Sprintf("%s/%s", StorageDir, fileName)

	// 2. Only executed if the file does not exist because of the previous check.
	err := os.WriteFile(filePath, fileContent, 0777)
	if err != nil {
		msg := fmt.Sprintf("[HyDFS] Failed to write file %s: %v", fileName, err)
		log.Println(msg)
		*reply = msg
		return err
	}

	// 3. Update membership info to include this file
	s.lock.Lock()
	s.Files = append(s.Files, fileName)

	// 4. Update appendLogs, as creating a file in HyDFS takes the localfilename's content
	// Note that we check it here because a node will not get appends for a file if it doesn't contain anything
	event := AppendEvent{
		ClientID:  args.ClientId,
		Timestamp: args.Time,
		Content:   args.FileContent,
	}

	if s.appendLogs[fileName] == nil {
		s.appendLogs[fileName] = make([]AppendEvent, 0)
	}

	s.appendLogs[fileName] = append(s.appendLogs[fileName], event)
	s.lock.Unlock()

	log.Printf("[HyDFS] Stored file %s successfully at %s", fileName, filePath)
	*reply = fmt.Sprintf("Stored file %s successfully on %s", fileName, s.Info.Hostname)
	return nil
}

func (s *Server) handleGetFile(fileName string, clientId string) string {
	replicas := s.ring.GetReplicas(fileName)
	result := new(string)

	// Get from primary replica as it will have the most recent writes from all clients
	CallWithTimeout(replicas[0].Hostname, 12346, "Server.GetFile",
		Args{FileName: fileName, ClientId: clientId}, result)

	// TODO: Primary replica died, not sure if this is right, because we can handle this in the replicate/rebalance logic
	if *result == "" && strings.Contains(*result, "[HyDFS] Failed") {
		for _, replica := range replicas {
			// Forward "Server.FetchFile" RPC to each replica (port 12345 for TCP)
			CallWithTimeout(replica.Hostname, 12346, "Server.GetFile",
				Args{FileName: fileName, ClientId: clientId}, result)

			if *result != "" && !strings.Contains(*result, "[HyDFS] Failed") {
				// Stop once we successfully retrieve content
				break
			}
		}
	}

	if *result == "" || strings.Contains(*result, "[HyDFS] Failed") {
		return fmt.Sprintf("[HyDFS] Error: failed to fetch file %s from all replicas", fileName)
	}

	// Just return the serialized JSON string — let the CLI handle decoding
	log.Printf("[HyDFS] Retrieved file %s successfully", fileName)
	return *result
}

func (s *Server) GetFile(args Args, reply *string) error {
	start := time.Now()
	receiveLog := fmt.Sprintf(
		"[HyDFS] Replica %s RECEIVED GetFile(%s) from client %s at %v",
		s.Info.Hostname, args.FileName, args.ClientId, start.Format("15:04:05"),
	)
	log.Println(receiveLog)

	filePath := fmt.Sprintf("%s/%s", StorageDir, args.FileName)
	content, err := os.ReadFile(filePath)
	if err != nil {
		msg := fmt.Sprintf("[HyDFS] Failed to read file %s on %s: %v",
			filePath, s.Info.Hostname, err)
		log.Println(msg)
		*reply = msg
		return nil
	}

	end := time.Now()
	completeLog := fmt.Sprintf(
		"[HyDFS] Replica %s COMPLETED GetFile(%s) at %v ",
		s.Info.Hostname, args.FileName, end.Format("15:04:05"))
	log.Println(completeLog)

	// Serialize reply
	resp := struct {
		Contents    []byte
		ReceiveLog  string
		CompleteLog string
	}{
		Contents:    content,
		ReceiveLog:  receiveLog,
		CompleteLog: completeLog,
	}

	bytes, _ := json.Marshal(resp)
	*reply = string(bytes)
	return nil
}

// func (s *Server) PullTimer() {
// 	for range time.Tick(s.Tpull) {
// 		for _, file := range s.Info.Files {
// 			s.mergeCurrReplicaWithPrimary(file)
// 			log.Printf("[HyDFS] Pulled updated mergeList for %s", file)
// 		}
//
// 	}
// }
//
// func (s *Server) mergeCurrReplicaWithPrimary(fileName string) string {
// 	log.Printf("[HyDFS] Starting merge for %s (replica pull from primary)...", fileName)
//
// 	primary := s.ring.GetReplicas(fileName)[0]
// 	if primary.Hostname == s.Info.Hostname {
// 		msg := fmt.Sprintf("[HyDFS] Skipping pull for %s — already on primary node.", fileName)
// 		log.Println(msg)
// 		return msg
// 	}
//
// 	go s.performCurrReplicaMergeWithPrimary(fileName)
// 	return fmt.Sprintf("Started merge pull successfully for %s", fileName)
// }
//
// func (s *Server) performCurrReplicaMergeWithPrimary(fileName string) {
// 	log.Printf("[HyDFS] Performing merge pull for %s from primary...", fileName)
//
// 	primary := s.ring.GetReplicas(fileName)[0]
// 	result := new(string)
//
// 	// get the merge list from primary
// 	CallWithTimeout(primary.Hostname, 12347, "Server.GetMergeList",
// 		Args{FileName: fileName}, result)
//
// 	var mergeList []AppendEvent
// 	if err := json.Unmarshal([]byte(*result), &mergeList); err != nil {
// 		log.Printf("[HyDFS] Failed to decode mergeList for %s: %v", fileName, err)
// 		return
// 	}
//
// 	// merge local list with primary nodes list
// 	args := Args{
// 		FileName:    fileName,
// 		FileContent: []byte(*result),
// 	}
// 	reply := new(string)
// 	err := s.ApplyMergeList(args, reply)
// 	if err != nil {
// 		log.Printf("[HyDFS] ApplyMergeList failed for %s: %v", fileName, err)
// 		return
// 	}
//
// 	log.Printf("[HyDFS] Replica %s successfully pulled and applied merge for %s",
// 		s.Info.Hostname, fileName)
// }

func (s *Server) handleMergeReplicas(fileName string) string {
	log.Printf("[HyDFS] Starting merge for %s (primary)...", fileName)

	replicas := s.ring.GetReplicas(fileName)
	primaryReplica := replicas[0]
	result := new(string)

	// TODO: Maybe add some error handling if the primaryReplica cannot be connected?
	CallWithTimeout(primaryReplica.Hostname, 12346, "Server.UpdateFromMergeList",
		Args{FileName: fileName}, result)

	if strings.Contains(*result, "success") {
		log.Printf("[HyDFS] Merge for %s complete.", fileName)
	}
	return *result
}

func (s *Server) UpdateFromMergeList(args Args, reply *string) error {
	log.Printf("[HyDFS] Received merge request for %s (RPC-triggered)...", args.FileName)

	// Only the primary replica should actually execute it.
	primary := s.ring.GetReplicas(args.FileName)[0]
	if primary.Hostname != s.Info.Hostname {
		msg := fmt.Sprintf("[HyDFS] Failed merge for %s — not primary.", args.FileName)
		log.Println(msg)
		*reply = msg
		return nil
	}

	go s.performMerge(args.FileName)
	*reply = "Started merge successfully on primary"
	return nil
}

func (s *Server) performMerge(fileName string) {
	log.Printf("[HyDFS] Performing merge for %s (primary)...", fileName)

	s.lock.Lock()
	defer s.lock.Unlock()

	// 1. Get the current appendLog
	appendLog := s.appendLogs[fileName]

	// 2. Sort and update mergeList
	s.mergeList[fileName] = sortMergeList(appendLog)
	sortedMergeList := s.mergeList[fileName]

	// 3. Reorder appendLog according to sorted mergeList
	s.appendLogs[fileName] = reorderAppendLogFromMergeList(appendLog, sortedMergeList)

	// 4. Rewrite local file from ordered appendLog
	s.writeFileFromLog(fileName, s.appendLogs[fileName])

	// 5. Broadcast sorted mergeList to replicas
	replicas := s.ring.GetReplicas(fileName)
	payload, _ := json.Marshal(sortedMergeList)
	result := new(string)

	for _, r := range replicas {
		// If the replica is the primary replica, skip
		if r.Hostname == s.Info.Hostname {
			continue
		}

		go CallWithTimeout(r.Hostname, 12346, "Server.ApplyMergeList",
			Args{FileName: fileName, FileContent: payload}, result)
	}

	log.Printf("[HyDFS] %s ", *result)
}

func (s *Server) ApplyMergeList(args Args, reply *string) error {
	log.Printf("[HyDFS] Starting applied mergeList for %s on %s.", args.FileName, s.Info.Hostname)

	var mergeList []AppendEvent
	if err := json.Unmarshal([]byte(args.FileContent), &mergeList); err != nil {
		return err
	}

	s.lock.RLock()
	localLog := s.appendLogs[args.FileName]
	s.lock.RUnlock()

	// Step 1: find missing appends
	missingKeys := findMissingAppends(localLog, mergeList)
	if len(missingKeys) > 0 {
		log.Printf("[HyDFS] %s missing %d appends for %s, pulling from primary...",
			s.Info.Hostname, len(missingKeys), args.FileName)

		payload, _ := json.Marshal(missingKeys)
		result := new(string)
		primary := s.ring.GetReplicas(args.FileName)[0]
		CallWithTimeout(primary.Hostname, 12346, "Server.GetMissingAppends",
			Args{FileName: args.FileName, FileContent: payload}, result)

		var missingEvents []AppendEvent
		json.Unmarshal([]byte(*result), &missingEvents)

		// Step 2: merge new events into appendLog
		s.lock.Lock()
		s.appendLogs[args.FileName] = append(s.appendLogs[args.FileName], missingEvents...)
		s.mergeList[args.FileName] = mergeList
		s.appendLogs[args.FileName] = reorderAppendLogFromMergeList(
			s.appendLogs[args.FileName], mergeList)
		s.lock.Unlock()

		s.writeFileFromLog(args.FileName, s.appendLogs[args.FileName])
		log.Printf("[HyDFS] %s updated file %s with missing appends.", s.Info.Hostname, args.FileName)
	} else {
		// Already complete
		s.lock.Lock()
		s.mergeList[args.FileName] = mergeList
		s.appendLogs[args.FileName] = reorderAppendLogFromMergeList(
			s.appendLogs[args.FileName], mergeList)
		s.lock.Unlock()
		s.writeFileFromLog(args.FileName, s.appendLogs[args.FileName])
	}

	*reply = fmt.Sprintf("Merge fully applied on: %s", s.Info.Hostname)
	return nil
}

func findMissingAppends(local, mergeList []AppendEvent) []string {
	have := make(map[string]bool)
	for _, e := range local {
		have[fmt.Sprintf("%s-%d", e.ClientID, e.Timestamp)] = true
	}
	missing := []string{}
	for _, e := range mergeList {
		key := fmt.Sprintf("%s-%d", e.ClientID, e.Timestamp)
		if !have[key] {
			missing = append(missing, key)
		}
	}
	return missing
}

// Args.FileName = file, Args.FileContent = JSON array of missing keys
func (s *Server) GetMissingAppends(args Args, reply *string) error {
	var missingKeys []string
	json.Unmarshal(args.FileContent, &missingKeys)

	s.lock.RLock()
	defer s.lock.RUnlock()

	full := []AppendEvent{}
	for _, e := range s.appendLogs[args.FileName] {
		key := fmt.Sprintf("%s-%d", e.ClientID, e.Timestamp)
		for _, want := range missingKeys {
			if want == key {
				full = append(full, e)
			}
		}
	}

	bytes, _ := json.Marshal(full)
	*reply = string(bytes)
	return nil
}

func (s *Server) RemoteAppendRequest(args Args, reply *string) error {
	log.Printf("[HyDFS] RemoteAppendRequest received on %s: append local file %s -> %s",
		s.Info.Hostname, args.LocalFileName, args.FileName)

	// Construct local path on THIS node
	localPath := fmt.Sprintf("%s/%s", DemoDir, args.LocalFileName)
	content, err := os.ReadFile(localPath)
	if err != nil {
		msg := fmt.Sprintf("[HyDFS] Failed to read local file %s on %s: %v",
			localPath, s.Info.Hostname, err)
		log.Println(msg)
		*reply = msg
		return nil
	}

	// Use existing append logic (replicates and handles quorum)
	output := s.handleAppendFile(args.FileName, content, args.ClientId, args.Time)

	*reply = fmt.Sprintf("[HyDFS] Remote append success on %s: %s", s.Info.Hostname, output)
	return nil
}

func (s *Server) handleAppendFile(fileName string, fileContent []byte, clientId string, clientTime int64) string {
	replicas := s.ring.GetReplicas(fileName)
	primaryReplica := replicas[0]
	log.Printf("Starting append on %v: ", primaryReplica.Hostname)
	output := ""

	result := new(string)
	// ClientId shouldnt be s.Id and Time shouldn't be time.Now(), that would mean the coordinator node is the client...
	// Instead it should just forward the client and time passed to CLI() from the actual client
	CallWithTimeout(primaryReplica.Hostname, 12346, "Server.AppendFile",
		Args{FileName: fileName, FileContent: fileContent, Time: clientTime, ClientId: clientId}, result)
	output += *result + "\n"

	if !strings.Contains(*result, "success") {
		// Try all replicas again, including the primaryReplica
		for _, replica := range replicas {
			CallWithTimeout(replica.Hostname, 12346, "Server.AppendFile",
				Args{FileName: fileName, FileContent: fileContent, Time: clientTime, ClientId: clientId}, result)
		}
		output += *result + "\n"
	}
	output += fmt.Sprintf("Appended to primary replica: %s\n", primaryReplica.Hostname)

	return output
}

func (s *Server) AppendFile(args Args, reply *string) error {
	start := time.Now()
	msg := fmt.Sprintf("[HyDFS] START append %s from client %s at %v\n", args.FileName, args.ClientId, start.Format("15:04:05"))

	fileName := args.FileName
	filePath := fmt.Sprintf("%s/%s", StorageDir, fileName)

	_, err := os.Stat(filePath)

	// 1. Fail if the file doesn't exist in the current node's Files
	exists := false
	for _, fName := range s.Files {
		if fName == fileName {
			exists = true
			break
		}
	}

	if !exists {
		msg := fmt.Sprintf("[HyDFS] File %s does not exist on replica %s", fileName, s.Info.Hostname)
		log.Println(msg)
		*reply = msg
		return nil
	}

	// 2. Check if the file exists locally on disk. If HyDFS metadata says it exists, it SHOULD exist locally.
	_, err = os.Stat(filePath)
	if os.IsNotExist(err) {
		// This is a critical error: HyDFS says the file is here, but the disk doesn't.
		msg := fmt.Sprintf("[HyDFS] CRITICAL: File %s exists in HyDFS metadata but is missing on disk %s. Aborting append.", fileName, s.Info.Hostname)
		log.Println(msg)
		*reply = msg
		return errors.New("file missing on local disk despite existing in HyDFS metadata")
	}

	// 3. Open file for appending.
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		msg := fmt.Sprintf("[HyDFS] Failed to open file %s for append on %s: %v", filePath, s.Info.Hostname, err)
		log.Println(msg)
		*reply = msg
		return err
	}
	defer file.Close()

	// 4. Write content to the local file.
	_, err = file.Write(args.FileContent)
	if err != nil {
		msg := fmt.Sprintf("[HyDFS] Failed to write to file %s on %s: %v", filePath, s.Info.Hostname, err)
		log.Println(msg)
		*reply = msg
		return err
	}

	// 5. Create AppendEvent and update the log (must be atomic with respect to other log readers/writers).
	event := AppendEvent{ // filename -> clientID, timestamp, filecontent
		ClientID:  args.ClientId,
		Timestamp: args.Time,
		Content:   args.FileContent,
	}

	// TODO: For multiappend
	s.lock.Lock()
	s.appendLogs[fileName] = append(s.appendLogs[fileName], event)
	s.lock.Unlock()

	// 6. Success message
	msg += fmt.Sprintf("[HyDFS] Appended %d bytes to %s successfully on %s\n",
		len(args.FileContent), fileName, s.Info.Hostname)

	msg += fmt.Sprintf("[HyDFS] END append %s from client %s at %v\n",
		args.FileName, args.ClientId, time.Now().Format("15:04:05"))

	log.Println(msg)
	*reply = msg
	return nil
}

// Return mergeList for a file (serialized)
func (s *Server) GetMergeList(args Args, reply *string) error {
	s.lock.RLock()
	defer s.lock.RUnlock()
	if list, ok := s.mergeList[args.FileName]; ok {
		bytes, _ := json.Marshal(list)
		*reply = string(bytes)
		return nil
	}
	*reply = ""
	return nil
}
func (s *Server) GetListStore(args Args, reply *string) error {
	s.lock.RLock()
	defer s.lock.RUnlock()

	type FileInfo struct {
		FileName string `json:"filename"`
		FileID   uint64 `json:"fileID"`
	}

	fileInfos := make([]FileInfo, 0, len(s.Files))
	for _, fname := range s.Files {
		fileInfos = append(fileInfos, FileInfo{
			FileName: fname,
			FileID:   hashring.HashFile(fname),
		})
	}

	data, err := json.MarshalIndent(fileInfos, "", "  ")
	if err != nil {
		return err
	}

	*reply = string(data)
	return nil
}

// Return mergeList for a file (serialized)
func (s *Server) GetFileList(args Args, reply *string) error {
	s.lock.RLock()
	defer s.lock.RUnlock()

	data, err := json.Marshal(s.Files)
	if err != nil {
		return err
	}
	*reply = string(data)
	return nil
}

// Return appendLog for a file (serialized)
func (s *Server) GetAppendLog(args Args, reply *string) error {
	s.lock.RLock()
	defer s.lock.RUnlock()
	if logList, ok := s.appendLogs[args.FileName]; ok {
		bytes, _ := json.Marshal(logList)
		*reply = string(bytes)
		return nil
	}
	*reply = ""
	return nil
}

// func (s *Server) PullTimer() {
// 	for range time.Tick(Tpull) {
// 		for _, file := range s.Info.Files {
// 			primary := s.ring.GetReplicas(file)[0]
// 			if primary.Hostname == s.Info.Hostname {
// 				continue // skip if I’m primary
// 			}
//
// 			result := new(string)
// 			CallWithTimeout(primary.Hostname, primary.Port, "Server.GetMergeList",
// 				Args{FileName: file}, result)
//
// 			var mergeList []AppendEvent
// 			json.Unmarshal([]byte(*result), &mergeList)
//
// 			s.lock.Lock()
// 			s.mergeList[file] = mergeList
// 			s.appendLogs[file] = reorderAppendLogFromMergeList(s.appendLogs[file], mergeList)
// 			s.lock.Unlock()
//
// 			s.writeFileFromLog(file, s.appendLogs[file])
// 			log.Printf("[HyDFS] Pulled updated mergeList for %s", file)
// 		}
// 	}
// }

func (s *Server) writeFileFromLog(fileName string, logEntries []AppendEvent) {
	filePath := fmt.Sprintf("%s/%s", StorageDir, fileName)

	// Will replace the already existing file
	f, err := os.Create(filePath)
	if err != nil {
		log.Printf("[HyDFS] Failed to rewrite %s: %v", fileName, err)
		return
	}
	defer f.Close()

	for _, e := range logEntries {
		f.Write(e.Content)
	}
}

func sortMergeList(logs []AppendEvent) []AppendEvent {
	sorted := make([]AppendEvent, len(logs))
	copy(sorted, logs)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].ClientID == sorted[j].ClientID {
			return sorted[i].Timestamp < sorted[j].Timestamp
		}
		return sorted[i].ClientID < sorted[j].ClientID
	})
	return sorted
}

func reorderAppendLogFromMergeList(appendLog, mergeList []AppendEvent) []AppendEvent {
	order := make(map[string]int)
	for i, e := range mergeList {
		key := fmt.Sprintf("%s-%d", e.ClientID, e.Timestamp)
		order[key] = i
	}

	sorted := make([]AppendEvent, len(appendLog))
	copy(sorted, appendLog)

	sort.Slice(sorted, func(i, j int) bool {
		keyI := fmt.Sprintf("%s-%d", sorted[i].ClientID, sorted[i].Timestamp)
		keyJ := fmt.Sprintf("%s-%d", sorted[j].ClientID, sorted[j].Timestamp)
		return order[keyI] < order[keyJ]
	})

	return sorted
}
