package member

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"
)

// member states
type MemberState int

const (
	Alive MemberState = iota
	Failed
	Suspected
)

var stateName = map[MemberState]string{
	Alive:     "Alive",
	Failed:    "Failed",
	Suspected: "Suspected",
}

type Info struct {
	Hostname  string      // hostname
	Port      int         // port number
	Version   time.Time   // version timestamp
	Timestamp time.Time   // local timestamp
	Counter   uint64      // heartbeat counter
	State     MemberState // member state
}

type Membership struct {
	lock            sync.RWMutex    // read write mutex
	Members         []uint64        // list of member Ids that is randomly permuted for gossip or pinging
	InfoMap         map[uint64]Info // member info map (might contain info of failed members)
	roundRobinIndex int             // random permutation round robin index
	// FailureCounter  uint64          // incremented whenever a failed node is detected (currently used for the ring)
	// JoinCounter     uint64          // incremented whenever a new node has joined (currently used for the ring)
}

func (i *Info) String() string {
	return fmt.Sprintf("Hostname: %s, Port: %d, Version: %s", i.Hostname, i.Port, i.Version.Format(time.RFC3339Nano))
}

func (m *Membership) Merge(memberInfo map[uint64]Info, currentTime time.Time) bool {
	// merge the sender's memberInfo into m
	m.lock.Lock()
	defer m.lock.Unlock()
	membershipChanged := false
	for id, info := range memberInfo {
		if info.Port == 0 || info.Hostname == "" {
			continue
		}
		if member, ok := m.InfoMap[id]; ok {
			if info.State == Failed {
				if member.State != Failed {
					info.Timestamp = currentTime
					m.InfoMap[id] = info // update to failed state
					log.Printf("FAILED: Node %d (%s:%d) failed\n",
						id, info.Hostname, info.Port)
					membershipChanged = true

					// Update the ring when a node failure occurs
					// m.FailureCounter++
				}
			} else {
				if member.State == Failed && info.State == Alive {
					log.Printf("REJOIN: Node %d (%s:%d) has rejoined\n", id, info.Hostname, info.Port)
					info.Timestamp = currentTime
					m.InfoMap[id] = info
					// m.JoinCounter++ // Rebalance ring
					membershipChanged = true
				} else if info.Counter > member.Counter {
					// update member info with higher counter
					info.Timestamp = currentTime

					// info.Files = member.Files
					m.InfoMap[id] = info
				}
			}
		} else if info.State != Failed {
			info.Timestamp = currentTime
			m.InfoMap[id] = info // add new member
			membershipChanged = true
		}
	}

	if membershipChanged {
		m.Members = make([]uint64, 0, len(m.InfoMap))
		for id, info := range m.InfoMap {
			if info.State != Failed {
				m.Members = append(m.Members, id)
			}
		}
		RandomPermutation(&m.Members) // randomize member ids for gossip or pinging
		m.roundRobinIndex = 0         // reset round robin index
	}
	return membershipChanged
}

func (m *Membership) UpdateStateGossip(currentTime time.Time, Tfail time.Duration, Tsuspect time.Duration, suspicionEnabled bool) bool {
	// check member states based on timestamps and thresholds
	m.lock.Lock()
	defer m.lock.Unlock()
	anyFailed := false
	for id, info := range m.InfoMap {
		elapsed := currentTime.Sub(info.Timestamp)
		oldState := info.State
		if oldState == Alive && elapsed > Tsuspect {
			if suspicionEnabled {
				info.State = Suspected
				info.Timestamp = currentTime
				m.InfoMap[id] = info

				if oldState != Suspected {
					log.Printf("SUSPECTED: Node %d (%s:%d) is now suspected\n",
						id, info.Hostname, info.Port)
				}
			} else {
				// Skip suspicion, go directly to failed
				log.Printf("FAILED: Node %d (%s:%d) failed\n",
					id, info.Hostname, info.Port)
				info.State = Failed
				info.Timestamp = currentTime
				m.InfoMap[id] = info
				anyFailed = true
			}
		} else if oldState == Suspected && elapsed > Tfail {
			log.Printf("FAILED: Node %d (%s:%d) failed\n",
				id, info.Hostname, info.Port)
			info.State = Failed
			info.Timestamp = currentTime
			m.InfoMap[id] = info
			anyFailed = true
		}
	}
	if anyFailed {
		m.Members = make([]uint64, 0, len(m.InfoMap))
		for id, info := range m.InfoMap {
			if info.State != Failed {
				m.Members = append(m.Members, id)
			}
		}
		RandomPermutation(&m.Members) // randomize member ids for gossip or pinging
		m.roundRobinIndex = 0         // reset round robin index
	}
	return anyFailed
}

func (m *Membership) UpdateStateSwim(currentTime time.Time, id uint64, state MemberState, suspicionEnabled bool) bool {
	// update state for a specific id, return true if any member beceom Failed.
	m.lock.Lock()
	defer m.lock.Unlock()
	info, ok := m.InfoMap[id]
	if ok && info.State != Failed {
		oldState := info.State
		info.Timestamp = currentTime

		if state == Suspected && !suspicionEnabled {
			// Skip suspicion, go directly to failed
			info.State = Failed
		} else {
			info.State = state
		}
		m.InfoMap[id] = info

		if info.State == Suspected && oldState != Suspected && suspicionEnabled {
			log.Printf("SUSPECTED: Node %d (%s:%d) is now suspected\n",
				id, info.Hostname, info.Port)
		}

		if info.State == Failed {
			log.Printf("FAILED: Node %d (%s:%d) failed\n",
				id, info.Hostname, info.Port)
			m.Members = make([]uint64, 0, len(m.InfoMap))
			for id, info := range m.InfoMap {
				if info.State != Failed {
					m.Members = append(m.Members, id)
				}
			}
			RandomPermutation(&m.Members) // randomize member ids for gossip or pinging
			m.roundRobinIndex = 0         // reset round robin index
			return true
		}
	}
	return false
}

func (m *Membership) Cleanup(currentTime time.Time, Tcleanup time.Duration) {
	// remove failed members
	m.lock.Lock()
	defer m.lock.Unlock()
	for id, info := range m.InfoMap {
		if info.State == Failed && currentTime.Sub(info.Timestamp) > Tcleanup {
			delete(m.InfoMap, id) // remove info about failed members
		}
	}
}

func (m *Membership) RemoveMember(id uint64) {
	// remove a member completely (for voluntary leave)
	m.lock.Lock()
	defer m.lock.Unlock()

	// Remove from InfoMap
	delete(m.InfoMap, id)

	// Remove from Members list
	for i, memberId := range m.Members {
		if memberId == id {
			m.Members = append(m.Members[:i], m.Members[i+1:]...)
			break
		}
	}

	// Reset round robin index if needed
	if m.roundRobinIndex >= len(m.Members) {
		RandomPermutation(&m.Members)
		m.roundRobinIndex = 0
	}
}

func (m *Membership) String() string {
	res := "Membership:\n"
	m.lock.RLock()
	defer m.lock.RUnlock()
	for _, id := range m.Members {
		info := m.InfoMap[id]
		res += fmt.Sprintf("ID: %d, Hostname: %s, Port: %d, Version: %s, Timestamp: %s, Counter: %d, State: %s\n",
			id, info.Hostname, info.Port, info.Version.Format(time.RFC3339Nano), info.Timestamp.Format(time.RFC3339Nano), info.Counter, stateName[info.State])
	}
	return res
}

func (m *Membership) Table(isRing bool) string {
	m.lock.RLock()
	defer m.lock.RUnlock()
	type Pair struct {
		Id       uint64
		Hostname string
		Port     int
	}
	infoList := make([]Pair, 0, 16)
	for _, id := range m.Members {
		info := m.InfoMap[id]
		infoList = append(infoList, Pair{
			Id:       id,
			Hostname: info.Hostname,
			Port:     info.Port,
		})
	}

	if isRing {
		sort.Slice(infoList, func(i, j int) bool {
			return infoList[i].Id < infoList[j].Id
		})
	} else {
		sort.Slice(infoList, func(i, j int) bool {
			if infoList[i].Hostname != infoList[j].Hostname {
				return infoList[i].Hostname < infoList[j].Hostname
			}
			return infoList[i].Port < infoList[j].Port
		})

	}
	// --------------------------------------------------------------------
	// | ID    |  Hostname | Port | Version | Timestamp | Counter | State |
	// | ID    |  Hostname | Port | Version | Timestamp | Counter | State |
	// --------------------------------------------------------------------
	maxLengths := map[string]int{
		"Id":        2,
		"Hostname":  8,
		"Port":      4,
		"Version":   7,
		"Timestamp": 9,
		"Counter":   7,
		"State":     5,
	}
	for _, i := range infoList {
		info := m.InfoMap[i.Id]
		lengths := map[string]int{
			"Id":        len(fmt.Sprintf("%d", i.Id)),
			"Hostname":  len(i.Hostname),
			"Port":      len(fmt.Sprintf("%d", i.Port)),
			"Version":   len(info.Version.Format(time.RFC3339Nano)),
			"Timestamp": len(info.Timestamp.Format(time.RFC3339Nano)),
			"Counter":   len(fmt.Sprintf("%d", info.Counter)),
			"State":     len(stateName[info.State]),
		}
		for key, value := range lengths {
			if maxLengths[key] < value {
				maxLengths[key] = value
			}
		}
	}
	totalLength := 30
	for _, v := range maxLengths {
		totalLength = totalLength + v
	}

	res := strings.Repeat("-", totalLength) + "\n"

	// Helper to format header string
	formatHeader := func(title string) string {
		s := title
		if len(s) < maxLengths[title] {
			s += strings.Repeat(" ", maxLengths[title]-len(s))
		}
		return s
	}

	// Add column headers
	header := "| "
	header += formatHeader("Id") + " | "
	header += formatHeader("Hostname") + " | "
	header += formatHeader("Port") + " | "
	header += formatHeader("Version") + " | "
	header += formatHeader("Timestamp") + " | "
	header += formatHeader("Counter") + " | "
	header += formatHeader("State") + " | "

	res += header + "\n"
	res += strings.Repeat("-", totalLength) + "\n"

	for _, i := range infoList {
		info := m.InfoMap[i.Id]
		line := "| "

		// Helper to format data string (right-pad)
		formatData := func(key string, s string) string {
			if len(s) < maxLengths[key] {
				s += strings.Repeat(" ", maxLengths[key]-len(s))
			}
			return s
		}
		sId := fmt.Sprintf("%d", i.Id)
		line += formatData("Id", sId) + " | "

		line += formatData("Hostname", i.Hostname) + " | "

		sPort := fmt.Sprintf("%d", i.Port)
		line += formatData("Port", sPort) + " | "

		sVersion := fmt.Sprintf("%v", info.Version.Format(time.RFC3339Nano))
		line += formatData("Version", sVersion) + " | "

		sTimestamp := fmt.Sprintf("%v", info.Timestamp.Format(time.RFC3339Nano))
		line += formatData("Timestamp", sTimestamp) + " | "

		// Counter
		sCounter := fmt.Sprintf("%d", info.Counter)
		line += formatData("Counter", sCounter) + " | "

		// State
		sState := stateName[info.State]
		line += formatData("State", sState) + " | "

		res = res + line + "\n"
	}
	res = res + strings.Repeat("-", totalLength) + "\n"
	return res
}

func (m *Membership) GetTarget() (Info, error) {
	// Round Robin with random permutation
	// TODO: maybe not to send message to self
	m.lock.Lock()
	defer m.lock.Unlock()
	if len(m.Members) == 0 {
		return Info{}, fmt.Errorf("no existing member")
	}
	targetInfo, ok := m.InfoMap[m.Members[m.roundRobinIndex]]
	if !ok {
		return Info{}, fmt.Errorf("inconsistent membership")
	}
	m.roundRobinIndex++
	if m.roundRobinIndex == len(m.Members) {
		RandomPermutation(&m.Members)
		m.roundRobinIndex = 0
	}
	return targetInfo, nil
}

func (m *Membership) Exists(hostname string, port int) bool {
	m.lock.RLock()
	defer m.lock.RUnlock()
	for _, info := range m.InfoMap {
		if info.Hostname == hostname && info.Port == port {
			return true
		}
	}
	return false
}

func (m *Membership) GetInfoMap() map[uint64]Info {
	m.lock.RLock()
	defer m.lock.RUnlock()

	// Create a new map to hold the copy.
	infoMapCopy := make(map[uint64]Info, len(m.InfoMap))

	// Copy the data from the internal map to the new one.
	for id, info := range m.InfoMap {
		infoMapCopy[id] = info
	}

	return infoMapCopy // Return the safe copy
}

func (m *Membership) Heartbeat(id uint64, currentTime time.Time) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	info, ok := m.InfoMap[id]
	if ok {
		if info.State == Failed {
			return fmt.Errorf("you failed")
		}
		info.Timestamp = currentTime
		info.Counter++
		m.InfoMap[id] = info
		return nil
	}
	return fmt.Errorf("no such ID: %d, the node might fail", id)
}

func HashInfo(info Info) uint64 {
	// hash hostname, port, and timestamp to 64 bit integer for map lookup
	hash := sha256.Sum256([]byte(fmt.Sprintf("%s:%d:%s", info.Hostname, info.Port, info.Version.Format(time.RFC3339Nano))))
	return uint64(binary.BigEndian.Uint64(hash[:8]))
}

func RandomPermutation(arr *[]uint64) {
	rng := rand.NewSource(time.Now().UnixNano())
	n := len(*arr)
	for i := 0; i < n; i++ {
		j := rng.Int63() % int64(i+1)
		if int64(i) != j {
			(*arr)[i], (*arr)[j] = (*arr)[j], (*arr)[i] // swap elements
		}
	}
}
