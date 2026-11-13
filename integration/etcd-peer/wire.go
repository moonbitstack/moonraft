// The line format the MoonBit peer and this harness share. One message is one
// line of space-separated fields, so an exchange that goes wrong can be read in
// the log of either side:
//
//	msg <type> <to> <from> <term> <logterm> <index> <commit> <reject> <hint> <force> <context> <n> [<type> <term> <index> <data>]...
//
// `context` and `data` are hex, `-` for empty. A snapshot appends the membership
// it records and the state it carries: `<voters> <ids...> <learners> <ids...> <data>`.
package main

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	pb "go.etcd.io/raft/v3/raftpb"
)

var carriedTypes = map[pb.MessageType]string{
	pb.MessageType_MsgApp:           "App",
	pb.MessageType_MsgAppResp:       "AppResp",
	pb.MessageType_MsgVote:          "Vote",
	pb.MessageType_MsgVoteResp:      "VoteResp",
	pb.MessageType_MsgPreVote:       "PreVote",
	pb.MessageType_MsgPreVoteResp:   "PreVoteResp",
	pb.MessageType_MsgHeartbeat:     "Heartbeat",
	pb.MessageType_MsgHeartbeatResp: "HeartbeatResp",
	pb.MessageType_MsgSnap:          "Snap",
}

var carriedNames = map[string]pb.MessageType{
	"App":           pb.MessageType_MsgApp,
	"AppResp":       pb.MessageType_MsgAppResp,
	"Vote":          pb.MessageType_MsgVote,
	"VoteResp":      pb.MessageType_MsgVoteResp,
	"PreVote":       pb.MessageType_MsgPreVote,
	"PreVoteResp":   pb.MessageType_MsgPreVoteResp,
	"Heartbeat":     pb.MessageType_MsgHeartbeat,
	"HeartbeatResp": pb.MessageType_MsgHeartbeatResp,
	"Snap":          pb.MessageType_MsgSnap,
}

func bit(v bool) string {
	if v {
		return "1"
	}
	return "0"
}

func hexOrDash(b []byte) string {
	if len(b) == 0 {
		return "-"
	}
	return hex.EncodeToString(b)
}

func boolField(text string) (bool, error) {
	switch text {
	case "1":
		return true, nil
	case "0":
		return false, nil
	}
	return false, fmt.Errorf("not a flag: %q", text)
}

func number(text string) (uint64, error) {
	n, err := strconv.ParseUint(text, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("not a number: %q", text)
	}
	return n, nil
}

func unhex(text string) ([]byte, error) {
	if text == "-" {
		return nil, nil
	}
	return hex.DecodeString(text)
}

// encodeMessage renders a message in the shared format.
func encodeMessage(m *pb.Message) (string, error) {
	name, ok := carriedTypes[m.GetType()]
	if !ok {
		return "", fmt.Errorf("message type %v is not carried across the wire", m.GetType())
	}
	var b strings.Builder
	// A snapshot's own metadata carries the baseline; the enclosing message
	// leaves index and logTerm at zero.
	logTerm, index := m.GetLogTerm(), m.GetIndex()
	if snap := m.GetSnapshot(); snap != nil {
		logTerm = snap.GetMetadata().GetTerm()
		index = snap.GetMetadata().GetIndex()
	}
	fmt.Fprintf(&b, "msg %s %d %d %d %d %d %d %s %d %d %s %d",
		name,
		m.GetTo(), m.GetFrom(), m.GetTerm(),
		logTerm, index, m.GetCommit(),
		bit(m.GetReject()), m.GetRejectHint(), 0,
		hexOrDash(m.GetContext()),
		len(m.GetEntries()),
	)
	for _, e := range m.GetEntries() {
		fmt.Fprintf(&b, " %d %d %d %s", int(e.GetType()), e.GetTerm(), e.GetIndex(), hexOrDash(e.GetData()))
	}
	if snap := m.GetSnapshot(); snap != nil {
		conf := snap.GetMetadata().GetConfState()
		fmt.Fprintf(&b, " %d", len(conf.GetVoters()))
		for _, v := range conf.GetVoters() {
			fmt.Fprintf(&b, " %d", v)
		}
		fmt.Fprintf(&b, " %d", len(conf.GetLearners()))
		for _, l := range conf.GetLearners() {
			fmt.Fprintf(&b, " %d", l)
		}
		fmt.Fprintf(&b, " %s", hexOrDash(snap.GetData()))
	}
	return b.String(), nil
}

// decodeMessage reads a line written by the MoonBit peer back into a message.
func decodeMessage(line string) (*pb.Message, error) {
	f := strings.Fields(line)
	if len(f) < 13 || f[0] != "msg" {
		return nil, fmt.Errorf("not a message: %q", line)
	}
	kind, ok := carriedNames[f[1]]
	if !ok {
		return nil, fmt.Errorf("unknown message type %q", f[1])
	}
	to, err := number(f[2])
	if err != nil {
		return nil, err
	}
	from, err := number(f[3])
	if err != nil {
		return nil, err
	}
	term, err := number(f[4])
	if err != nil {
		return nil, err
	}
	logTerm, err := number(f[5])
	if err != nil {
		return nil, err
	}
	index, err := number(f[6])
	if err != nil {
		return nil, err
	}
	commit, err := number(f[7])
	if err != nil {
		return nil, err
	}
	reject, err := boolField(f[8])
	if err != nil {
		return nil, err
	}
	hint, err := number(f[9])
	if err != nil {
		return nil, err
	}
	context, err := unhex(f[11])
	if err != nil {
		return nil, err
	}
	count, err := number(f[12])
	if err != nil {
		return nil, err
	}
	var entries []*pb.Entry
	at := 13
	for i := uint64(0); i < count; i++ {
		if at+3 >= len(f) {
			return nil, fmt.Errorf("the message ends inside its entries: %q", line)
		}
		entryType, err := number(f[at])
		if err != nil {
			return nil, err
		}
		entryTerm, err := number(f[at+1])
		if err != nil {
			return nil, err
		}
		entryIndex, err := number(f[at+2])
		if err != nil {
			return nil, err
		}
		data, err := unhex(f[at+3])
		if err != nil {
			return nil, err
		}
		entries = append(entries, &pb.Entry{
			Type:  pb.EntryType(entryType).Enum(),
			Term:  ptr(entryTerm),
			Index: ptr(entryIndex),
			Data:  data,
		})
		at += 4
	}
	msg := &pb.Message{
		Type:    kind.Enum(),
		To:      ptr(to),
		From:    ptr(from),
		Term:    ptr(term),
		LogTerm: ptr(logTerm),
		Index:   ptr(index),
		Commit:  ptr(commit),
		Reject:  ptr(reject),
		Entries: entries,
		Context: context,
	}
	switch kind {
	case pb.MessageType_MsgAppResp:
		msg.RejectHint = ptr(hint)
	case pb.MessageType_MsgSnap:
		voters, next, err := readIDs(f, at)
		if err != nil {
			return nil, err
		}
		learners, next, err := readIDs(f, next)
		if err != nil {
			return nil, err
		}
		if next >= len(f) {
			return nil, fmt.Errorf("the snapshot carries no data: %q", line)
		}
		data, err := unhex(f[next])
		if err != nil {
			return nil, err
		}
		msg.Snapshot = &pb.Snapshot{
			Data: data,
			Metadata: &pb.SnapshotMetadata{
				Index:     ptr(index),
				Term:      ptr(logTerm),
				ConfState: &pb.ConfState{Voters: voters, Learners: learners},
			},
		}
	}
	return msg, nil
}

func readIDs(f []string, at int) ([]uint64, int, error) {
	if at >= len(f) {
		return nil, at, fmt.Errorf("the message ends before its membership")
	}
	count, err := number(f[at])
	if err != nil {
		return nil, at, err
	}
	out := make([]uint64, 0, count)
	for i := uint64(0); i < count; i++ {
		if at+1+int(i) >= len(f) {
			return nil, at, fmt.Errorf("the message ends inside its membership")
		}
		id, err := number(f[at+1+int(i)])
		if err != nil {
			return nil, at, err
		}
		out = append(out, id)
	}
	return out, at + 1 + int(count), nil
}

func ptr[T any](v T) *T { return &v }
