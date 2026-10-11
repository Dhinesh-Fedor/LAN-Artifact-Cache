// Picks which peers should hold a replica of a key.
package replication

import "github.com/Fedoraaa/lan-artifact-cache/internal/hashing"

type Planner struct {
	Replicas int
}

func (p Planner) Peers(r *hashing.Ring, key string, peers []string) []string {
	if (len(peers) == 0) {
		return nil
	}

	if (p.Replicas < 1) {
		p.Replicas = 1
	}

	// The consistent-hash owner always goes first, then  fill
	// the remaining slots with distinct peers.
	first := r.Owner(key)
	out := []string{}
	if (first != "") {
		out = append(out, first)
	}

	for _, x := range (peers) {
		if (len(out) >= p.Replicas) {
			break
		}

		seen := false
		for _, y := range (out) {
			if (x == y) {
				seen = true
			}
		}

		if (!seen) {
			out = append(out, x)
		}
	}

	return out
}
