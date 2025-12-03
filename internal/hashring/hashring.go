package hashring

import (
	"crypto/sha1"
	"cs425/MP3/internal/member"
	"encoding/binary"
	"sort"
	"sync"
)

type HashRing struct {
	lock      sync.RWMutex
	ring      []uint64               // sorted node IDs
	nodes     map[uint64]member.Info // ID → node info
	nReplicas int                    // number of replicas per file
}

func NewHashRing(nReplicas int) *HashRing {
	return &HashRing{
		ring:      make([]uint64, 0),
		nodes:     make(map[uint64]member.Info),
		nReplicas: nReplicas,
	}
}

// build the current node's ring based on its membership table
func (h *HashRing) UpdateRing(m *member.Membership) {
	h.lock.Lock()
	defer h.lock.Unlock()
	h.nodes = make(map[uint64]member.Info)
	h.ring = nil
	for id, info := range m.GetInfoMap() {
		if info.State == member.Alive {
			h.ring = append(h.ring, id)
			h.nodes[id] = info
		}
	}

	sort.Slice(h.ring, func(i, j int) bool { return h.ring[i] < h.ring[j] })
}

// we already have HashInfo, which hashes nodes. So we need a function to hash files based on their filename
func HashFile(filename string) uint64 {
	h := sha1.Sum([]byte(filename))
	return binary.BigEndian.Uint64(h[:8])
}

func (h *HashRing) GetReplicas(filename string) []member.Info {
	h.lock.RLock()
	defer h.lock.RUnlock()

	fileHash := HashFile(filename)
	replicas := []member.Info{}

	if len(h.ring) == 0 {
		return replicas
	}

	// Binary search for the first node >= fileHash
	idx := sort.Search(len(h.ring), func(i int) bool { return h.ring[i] >= fileHash })
	if idx == len(h.ring) {
		idx = 0 // wrap around ring
	}

	// Collect n successor nodes
	// TODO: Perhaps add a Failed node checker here
	for i := 0; i < h.nReplicas; i++ {
		nodeID := h.ring[(idx+i)%len(h.ring)]
		replicas = append(replicas, h.nodes[nodeID])
	}

	return replicas
}

func (h *HashRing) GetRingIDs() []uint64 {
	h.lock.RLock()
	defer h.lock.RUnlock()
	ringCopy := make([]uint64, len(h.ring))
	copy(ringCopy, h.ring)

	return ringCopy // Return the safe copy
}

func (h *HashRing) GetNodeInfo(id uint64) member.Info {
	h.lock.RLock()
	defer h.lock.RUnlock()
	return h.nodes[id]
}

func (h *HashRing) GetNodeByIndex(index int) member.Info {
	h.lock.RLock()
	defer h.lock.RUnlock()
	return h.nodes[h.ring[index]]
}
