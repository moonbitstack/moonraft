// The MoonBit node as a child process. The harness ticks it and hands it
// messages on stdin; the node answers on stdout, one line per message, and says
// `done` when it has finished handling the command. Nothing about the protocol
// lives on this side of the pipe.
package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

type peerProcess struct {
	id   uint64
	exe  string
	args []string
	cmd  *exec.Cmd
	in   io.WriteCloser
	out  *bufio.Reader
}

func startPeer(exe string, id uint64, peers []uint64, electionTick, heartbeatTick int) (*peerProcess, error) {
	ids := make([]string, 0, len(peers))
	for _, p := range peers {
		ids = append(ids, strconv.FormatUint(p, 10))
	}
	args := []string{
		"--id", strconv.FormatUint(id, 10),
		"--peers", strings.Join(ids, ","),
		"--election-tick", strconv.Itoa(electionTick),
		"--heartbeat-tick", strconv.Itoa(heartbeatTick),
	}
	return startPeerWith(exe, id, args)
}

func startPeerWith(exe string, id uint64, args []string) (*peerProcess, error) {
	cmd := exec.Command(exe, args...)
	cmd.Stderr = os.Stderr
	in, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	return &peerProcess{id: id, exe: exe, args: args, cmd: cmd, in: in, out: bufio.NewReader(out)}, nil
}

// do writes one command and collects the lines the node answers with, up to the
// `done` that marks the end of the reply.
func (p *peerProcess) do(command string) ([]string, error) {
	if _, err := fmt.Fprintln(p.in, command); err != nil {
		return nil, fmt.Errorf("peer %d: %w", p.id, err)
	}
	var lines []string
	for {
		line, err := p.out.ReadString('\n')
		if err != nil {
			return nil, fmt.Errorf("peer %d while answering %q: %w", p.id, command, err)
		}
		line = strings.TrimSpace(line)
		switch {
		case line == "done":
			return lines, nil
		case strings.HasPrefix(line, "error "):
			return nil, fmt.Errorf("peer %d: %s", p.id, strings.TrimPrefix(line, "error "))
		case line != "":
			lines = append(lines, line)
		}
	}
}

// stop ends the child. A killed process reports a non-zero exit status, which is
// the expected outcome here, so the wait error is not propagated.
func (p *peerProcess) stop() error {
	_ = p.in.Close()
	if p.cmd.Process != nil {
		_ = p.cmd.Process.Kill()
	}
	_ = p.cmd.Wait()
	return nil
}
