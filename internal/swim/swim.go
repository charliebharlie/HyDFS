package swim

import (
	"cs425/MP3/internal/member"
	"cs425/MP3/internal/utils"
	"log"
	"os"
	"sync"
	"time"
)

type Swim struct {
	Membership       *member.Membership     // membership
	waitAcksDirect   map[uint64]member.Info // members that are pinged yet haven't ponged back
	waitAcksIndirect map[uint64]member.Info // members that are indirectly pinged yet haven't ponged back
	lock             sync.RWMutex           // read write mutex for waitAcks
}

func (s *Swim) HandleIncomingMessage(message utils.Message, myInfo member.Info) int64 {
	// merge incoming information
	currentTime := time.Now()
	anyChanged := s.Membership.Merge(message.InfoMap, currentTime)
	if anyChanged {
		log.Printf("Membership updated: %s\n", s.Membership.String())
	}

	switch message.Type {
	case utils.Ping: // ping
		// create ack message
		ackMessage := utils.Message{
			Type:          utils.Pong,
			InfoMap:       s.Membership.GetInfoMap(),
			SenderInfo:    myInfo,
			TargetInfo:    message.SenderInfo,
			RequesterInfo: message.RequesterInfo,
		}
		// send ping ack
		n, err := utils.SendMessage(ackMessage, message.SenderInfo.Hostname, message.SenderInfo.Port)
		if err != nil {
			log.Printf("Failed to send ping ack message to %s: %s", message.SenderInfo.String(), err.Error())
		}
		return n
	case utils.Pong: // ping ack message
		// three types of ping ack:
		// 1. ping ack of my own ping
		// 2. ping ack of my own indirect ping
		// 3. ping ack of other's indirect ping
		senderId := member.HashInfo(message.SenderInfo)
		targetId := member.HashInfo(message.TargetInfo)
		requesterId := member.HashInfo(message.RequesterInfo)
		if requesterId == targetId { // my ack
			s.lock.Lock()
			// log.Printf("Receive ping ack message from %s", message.SenderInfo.String())
			// direct ack
			_, ok := s.waitAcksDirect[senderId]
			if ok {
				// delete senderId from the map, don't need extra update to the member info, since the info is already merged
				delete(s.waitAcksDirect, senderId)
			}
			// indirect ack
			_, ok = s.waitAcksIndirect[senderId]
			if ok {
				// delete senderId from the map, don't need extra update to the member info, since the info is already merged
				delete(s.waitAcksIndirect, senderId)
			}
			s.lock.Unlock()
			// ignore ping ack that is not recorded
		} else { // other's ack, just send an ack back
			ackMessage := utils.Message{
				Type:          utils.Pong,
				InfoMap:       s.Membership.GetInfoMap(), // use the latest info map for faster convergence
				SenderInfo:    message.SenderInfo,
				TargetInfo:    message.RequesterInfo,
				RequesterInfo: message.RequesterInfo,
			}
			// send ping ack
			n, err := utils.SendMessage(ackMessage, message.RequesterInfo.Hostname, message.RequesterInfo.Port)
			if err != nil {
				log.Printf("Failed to send indirect ping ack message to %s: %s", message.RequesterInfo.String(), err.Error())
			}
			return n
		}
	case utils.PingReq: // ping request
		// create ping message
		pingMessage := utils.Message{
			Type:          utils.Ping,
			InfoMap:       s.Membership.GetInfoMap(), // use the latest info map for faster convergence
			SenderInfo:    myInfo,
			TargetInfo:    message.TargetInfo,
			RequesterInfo: message.RequesterInfo,
		}
		// send ping message
		n, err := utils.SendMessage(pingMessage, message.TargetInfo.Hostname, message.TargetInfo.Port)
		if err != nil {
			log.Printf("Failed to forward indirect ping message to %s: %s", message.TargetInfo.String(), err.Error())
		}
		return n
	}
	return 0
}

func (s *Swim) SendPingReq(k int, myInfo member.Info, targetInfo member.Info) int64 {
	// Send ping request to other k member
	var totalBytes int64 = 0
	infoMap := s.Membership.GetInfoMap()
	for i := 0; i < k; i++ {
		forwarderInfo, err := s.Membership.GetTarget()
		if err != nil {
			log.Printf("Failed to get target info for ping request: %s", err.Error())
			os.Exit(1) // fatal error
		}
		message := utils.Message{
			Type:          utils.PingReq,
			InfoMap:       infoMap,
			SenderInfo:    myInfo,
			TargetInfo:    targetInfo,
			RequesterInfo: myInfo,
		}
		// send message
		size, err := utils.SendMessage(message, forwarderInfo.Hostname, forwarderInfo.Port)
		if err != nil {
			log.Printf("Failed to send message to %s: %s", targetInfo.String(), err.Error())
		}
		totalBytes += size
	}
	return totalBytes
}

func (s *Swim) SwimStep(
	myId uint64,
	myInfo member.Info,
	k int,
	TpingFail time.Duration,
	TpingReqFail time.Duration,
	Tcleanup time.Duration,
	suspicionEnabled bool) int64 {
	currentTime := time.Now()
	// increase heartbeat counter; here it acts as an incarnation number
	err := s.Membership.Heartbeat(myId, currentTime)
	if err != nil {
		log.Fatalf("Failed to heartbeat: %s", err.Error())
	}

	// Mutex lock
	s.lock.Lock()

	var totalBytes int64 = 0
	var membersToPingReq []member.Info

	// check if there's any waitAcksIndirect haven't receive pong after TpingReqFail
	for id, info := range s.waitAcksIndirect {
		if currentTime.Sub(info.Timestamp) > TpingReqFail {
			switch info.State {
			case member.Alive: // update its state to suspect
				s.Membership.UpdateStateSwim(currentTime, id, member.Suspected, suspicionEnabled)
				info.Timestamp = currentTime
				info.State = member.Suspected
				s.waitAcksIndirect[id] = info
			case member.Suspected: // node failed, update its state and remove it from waitAcksIndirect
				s.Membership.UpdateStateSwim(currentTime, id, member.Failed, suspicionEnabled)
				delete(s.waitAcksIndirect, id)
			}
		}
	}

	// check if there's any waitAcksDirect haven't receive pong after TpingFail
	for id, info := range s.waitAcksDirect {
		if currentTime.Sub(info.Timestamp) > TpingFail {
			// remove it from waitAcksDirect
			delete(s.waitAcksDirect, id)
			// add it to waitAcksIndirect
			info.Timestamp = currentTime
			s.waitAcksIndirect[id] = info
			// send ping request to other k member
			// n := s.SendPingReq(k, myInfo)
			// totalBytes += n
			membersToPingReq = append(membersToPingReq, info)
		}
	}
	s.lock.Unlock()

	for _, info := range membersToPingReq {
		n := s.SendPingReq(k, myInfo, info)
		totalBytes += n
	}

	s.Membership.Cleanup(currentTime, Tcleanup)

	// get ping target info
	targetInfo, err := s.Membership.GetTarget()
	if err != nil {
		log.Printf("Failed to get target info: %s", err.Error())
		os.Exit(1) // fatal error, restart
	}

	// create ping message
	message := utils.Message{
		Type:          utils.Ping,
		InfoMap:       s.Membership.GetInfoMap(), // use the latest info map
		SenderInfo:    myInfo,
		TargetInfo:    targetInfo,
		RequesterInfo: myInfo,
	}
	// send message
	n, err := utils.SendMessage(message, targetInfo.Hostname, targetInfo.Port)
	if err != nil {
		log.Printf("Failed to send ping message to %s: %s", targetInfo.String(), err.Error())
	} else {
		// add message to waitAcksDirect
		totalBytes += n
		targetId := member.HashInfo(targetInfo)
		targetInfo.Timestamp = currentTime
		s.lock.Lock()
		s.waitAcksDirect[targetId] = targetInfo
		s.lock.Unlock()
	}
	return totalBytes
}

func NewSwim(membership *member.Membership) *Swim {
	return &Swim{
		Membership:       membership,
		waitAcksDirect:   make(map[uint64]member.Info, 32),
		waitAcksIndirect: make(map[uint64]member.Info, 32),
	}
}
