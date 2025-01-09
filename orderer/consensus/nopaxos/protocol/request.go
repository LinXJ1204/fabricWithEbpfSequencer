// Copyright 2019-present Open Networking Foundation.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package protocol

import (
	"fmt"

	"github.com/gogo/protobuf/proto"
)

// Result is a stream result
type Result struct {
	Value interface{}
	Error error
}

type NewCommandRequest struct {
	*CommandRequest
	ConfigSeq uint64
}

// Failed returns a boolean indicating whether the operation failed
func (r Result) Failed() bool {
	return r.Error != nil
}

// Succeeded returns a boolean indicating whether the operation was successful
func (r Result) Succeeded() bool {
	return !r.Failed()
}

func (s *NOPaxos) Command(request *NewCommandRequest, stream ClientService_ClientStreamServer) {
	s.logger.Receive("CommandRequest", request)
	fmt.Println("=====CommandRequest Receive=====")

	s.mu.Lock()
	defer s.mu.Unlock()

	// If the replica's status is not Normal, skip the commit
	if s.status != StatusNormal {
		s.logger.Trace("Dropping CommandRequest: Replica status is not Normal")
		return
	}

	if request.SessionNum == s.viewID.SessionNum && request.MessageNum == s.sessionMessageNum {
		// Command received in the normal case
		slotNum := s.log.LastSlot() + 1
		entry := &NewLogEntry{
			&LogEntry{
				SlotNum:    slotNum,
				Timestamp:  request.Timestamp,
				MessageNum: request.MessageNum,
				Value:      request.Value,
			},
			request.ConfigSeq,
		}
		s.log.Set(entry)

		if s.getLeader(s.viewID) == s.cluster.Member() {

		}

		// Apply the command to the state machine before responding if leader
		if stream != nil {
			if s.getLeader(s.viewID) == s.cluster.Member() {
				ch := make(chan Result)
				viewID := s.viewID
				go func() {
					for result := range ch {
						indexed := &Indexed{}
						if err := proto.Unmarshal(result.Value.([]byte), indexed); err != nil {
							continue
						}
						commandReply := &CommandReply{
							MessageNum: request.MessageNum,
							Sender:     s.cluster.Member(),
							ViewID:     viewID,
							SlotNum:    LogSlotID(indexed.Index),
							Value:      indexed.Value,
						}
						message := &ClientMessage{
							Message: &ClientMessage_CommandReply{
								CommandReply: commandReply,
							},
						}
						// TODO: Send state machine errors
						s.logger.Send("CommandReply", commandReply)
						if err := stream.Send(message); err != nil {
							s.logger.Error("Failed to send CommandReply")
						}
					}

					commandClose := &CommandClose{
						MessageNum: request.MessageNum,
						ViewID:     s.viewID,
					}
					message := &ClientMessage{
						Message: &ClientMessage_CommandClose{
							CommandClose: commandClose,
						},
					}
					s.logger.Send("CommandClose", commandClose)
					if err := stream.Send(message); err != nil {
						s.logger.Error("Failed to send CommandClose")
					}
				}()
			} else {
				commandReply := &CommandReply{
					MessageNum: request.MessageNum,
					Sender:     s.cluster.Member(),
					ViewID:     s.viewID,
					SlotNum:    slotNum,
				}
				message := &ClientMessage{
					Message: &ClientMessage_CommandReply{
						CommandReply: commandReply,
					},
				}
				s.logger.Send("CommandReply", commandReply)
				if err := stream.Send(message); err != nil {
					s.logger.Error("Failed to send CommandReply")
				}
			}
		}
		s.sessionMessageNum++
	} else if request.SessionNum > s.viewID.SessionNum {
		s.logger.Info("Session %d terminated", s.viewID.SessionNum)
		s.logger.Info("Requesting view change for session %d", request.SessionNum)

		// Command received in the session terminated case
		newViewID := &ViewId{
			SessionNum: request.SessionNum,
			LeaderNum:  s.viewID.LeaderNum,
		}
		viewChangeRequest := &ViewChangeRequest{
			ViewID: newViewID,
		}
		message := &ReplicaMessage{
			Message: &ReplicaMessage_ViewChangeRequest{
				ViewChangeRequest: viewChangeRequest,
			},
		}
		for _, member := range s.cluster.Members() {
			s.logger.SendTo("ViewChangeRequest", viewChangeRequest, member)
			go s.send(message, member)
		}
	} else if request.SessionNum == s.viewID.SessionNum && request.MessageNum > s.sessionMessageNum {
		s.logger.Debug("Received drop notification for %d", s.sessionMessageNum)

		// Drop notification. If leader commit a gap, otherwise ask the leader for the slot
		if s.getLeader(s.viewID) == s.cluster.Member() {
			s.sendGapCommit()
		} else {
			leader := s.getLeader(s.viewID)
			slotLookup := &SlotLookup{
				Sender:     s.cluster.Member(),
				ViewID:     s.viewID,
				MessageNum: request.MessageNum,
			}
			message := &ReplicaMessage{
				Message: &ReplicaMessage_SlotLookup{
					SlotLookup: slotLookup,
				},
			}
			s.logger.SendTo("SlotLookup", slotLookup, leader)
			go s.send(message, leader)
		}
	}
}

func (s *NOPaxos) query(request *QueryRequest, stream ClientService_ClientStreamServer) {
	s.logger.Receive("QueryRequest", request)

	s.mu.RLock()
	defer s.mu.RUnlock()

	// If the replica's status is not Normal, skip the commit
	if s.status != StatusNormal {
		return
	}

	if request.SessionNum == s.viewID.SessionNum && stream != nil && s.getLeader(s.viewID) == s.cluster.Member() {
		ch := make(chan Result)
		go func() {
			for result := range ch {
				// TODO: Send state machine errors
				queryReply := &QueryReply{
					MessageNum: request.MessageNum,
					Sender:     s.cluster.Member(),
					ViewID:     s.viewID,
					Value:      result.Value.([]byte),
				}
				message := &ClientMessage{
					Message: &ClientMessage_QueryReply{
						QueryReply: queryReply,
					},
				}
				s.logger.Send("QueryReply", queryReply)
				if err := stream.Send(message); err != nil {
					s.logger.Error("Failed to send QueryReply")
				}
			}

			queryClose := &QueryClose{
				MessageNum: request.MessageNum,
				ViewID:     s.viewID,
			}
			message := &ClientMessage{
				Message: &ClientMessage_QueryClose{
					QueryClose: queryClose,
				},
			}
			s.logger.Send("QueryClose", queryClose)
			if err := stream.Send(message); err != nil {
				s.logger.Error("Failed to send QueryClose")
			}
		}()
	}
}

func (s *NOPaxos) handleSlot(request *NewCommandRequest) {
	s.Command(request, nil)
}