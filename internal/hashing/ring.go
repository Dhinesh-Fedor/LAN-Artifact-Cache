// SHA-256 based consistent hashing ring.
package hashing

import (
	"crypto/sha256"
	"encoding/binary"
	"sort"
)

type Ring struct {
	points   []point
	replicas int
}

type point struct {
	hash uint64
	peer string
}

// replicas is the number of virtual nodes each peer owns; more points
// means smoother load distribution.
func New(peers []string, replicas int) *Ring {
	if (replicas < 1) {
		replicas = 32
	}

	r := &Ring{replicas: replicas}
	for _, p := range (peers) {
		r.Add(p)
	}

	return r
}

func (r *Ring) Add(peer string) {
	for i := 0; i < r.replicas; i++ {
		r.points = append(r.points, point{hash: hash64(peer + string(rune(i))), peer: peer})
	}

	sort.Slice(r.points, func(i, j int) bool { return r.points[i].hash < r.points[j].hash })
}

func (r *Ring) Remove(peer string) {
	p := r.points[:0]
	for _, x := range (r.points) {
		if (x.peer != peer) {
			p = append(p, x)
		}
	}
	r.points = p
}

func (r *Ring) Owner(key string) string {
	if (len(r.points) == 0) {
		return ""
	}

	h := hash64(key)
	i := sort.Search(len(r.points), func(i int) bool { return r.points[i].hash >= h })
	if (i == len(r.points)) {
		i = 0
	}

	return r.points[i].peer
}

func hash64(s string) uint64 {
	h := sha256.Sum256([]byte(s))
	return binary.BigEndian.Uint64(h[:8])
}
