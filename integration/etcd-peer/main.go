// Two Raft implementations in one cluster: one node is this repository's
// MoonBit code, the other two are etcd-io/raft. The harness owns the clock and
// the wire; neither side knows what the other is, and both have to agree.
package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	peerExe := flag.String("peer", "", "the MoonBit peer executable")
	name := flag.String("scenario", "all", "election, replication, failover, snapshot, or all")
	verbose := flag.Bool("v", false, "print every message")
	flag.Parse()
	if *peerExe == "" {
		fmt.Fprintln(os.Stderr, "usage: harness --peer <moonraft peer executable> [--scenario all]")
		os.Exit(2)
	}
	failed := false
	for _, s := range scenarios() {
		if *name != "all" && *name != s.name {
			continue
		}
		// The shorter election timeout decides who campaigns first: five ticks
		// for the MoonBit node makes it the candidate, forty leaves the field to
		// the etcd nodes.
		peerElection := 5
		if s.name == "replication" || s.name == "snapshot" {
			peerElection = 40
		}
		c, err := newCluster(*peerExe, peerElection, 10, *verbose)
		if err != nil {
			fmt.Printf("FAIL %s: %v\n", s.name, err)
			failed = true
			continue
		}
		fmt.Printf("%s\n", s.name)
		err = s.run(c, func(format string, args ...interface{}) {
			fmt.Printf("  %s\n", fmt.Sprintf(format, args...))
		})
		c.close()
		if err != nil {
			fmt.Printf("FAIL %s: %v\n", s.name, err)
			failed = true
			continue
		}
		fmt.Printf("PASS %s\n", s.name)
	}
	if failed {
		os.Exit(1)
	}
}
