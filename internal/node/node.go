// Node wires together cache, discovery, hashing, and transport.
package node

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/Fedoraaa/lan-artifact-cache/internal/cache"
	"github.com/Fedoraaa/lan-artifact-cache/internal/discovery"
	"github.com/Fedoraaa/lan-artifact-cache/internal/hashing"
	"github.com/Fedoraaa/lan-artifact-cache/internal/replication"
	"github.com/Fedoraaa/lan-artifact-cache/internal/stats"
	"github.com/Fedoraaa/lan-artifact-cache/internal/transport"
)

type Node struct {
	Cache     *cache.Cache
	Discovery *discovery.Service
	Stats     *stats.Stats
	id        string
	server    *transport.Server
}

func New(root, id, tcp, udp string) (*Node, error) {
	s, err := cache.NewStorage(root)
	if err != nil {
		return nil, err
	}

	c := cache.New(s)
	d := discovery.New(id, tcp, udp)

	return &Node{id: id, Cache: c, Discovery: d, Stats: &stats.Stats{}}, nil
}

func (n *Node) Put(path string) (string, error) {
	h, err := hashing.File(path)
	if err != nil {
		return "", err
	}

	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	return h, n.Cache.Put(h, f)
}

func (n *Node) Start() error {
	if n.server != nil {
		return nil
	}
	n.server = &transport.Server{Addr: n.Discovery.TCPAddress, Fetch: func(hash string) (io.ReadCloser, error) {
		return n.Cache.Open(hash)
	}}
	if err := n.server.Start(); err != nil {
		n.server = nil
		return err
	}
	n.Discovery.TCPAddress = reachableAddress(n.server.Addr)
	if err := n.Discovery.Start(); err != nil {
		_ = n.server.Close()
		n.server = nil
		return err
	}
	return nil
}

func (n *Node) Stop() error {
	var first error
	if n.Discovery != nil {
		if err := n.Discovery.Stop(); err != nil {
			first = err
		}
	}
	if n.server != nil {
		if err := n.server.Close(); err != nil && first == nil {
			first = err
		}
		n.server = nil
	}
	return first
}

func (n *Node) Get(hash, out string) error {
	f, err := n.Cache.Open(hash)
	if err == nil {
		defer f.Close()
		if err := verifyReader(hash, f); err != nil {
			return err
		}
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return err
		}

		o, e := os.Create(out)
		if e != nil {
			return e
		}
		defer o.Close()

		_, e = io.Copy(o, f)
		if e == nil {
			n.Stats.Hits.Add(1)
		}

		return e
	}

	n.Stats.Misses.Add(1)
	if _, err := hex.DecodeString(hash); err != nil || len(hash) != sha256.Size*2 {
		return fmt.Errorf("invalid artifact hash %q", hash)
	}

	if err := n.Start(); err != nil {
		return fmt.Errorf("start node services: %w", err)
	}

	// Discovery is asynchronous. Give peer announcements time to arrive
	// before deciding that no peers are available.
	deadline := time.Now().Add(3 * time.Second)
	peers := n.Discovery.Peers()
	for len(peers) == 0 && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
		peers = n.Discovery.Peers()
	}
	if len(peers) == 0 {
		return fmt.Errorf("artifact %s not in local cache and no peers are available", hash)
	}

	ids := make([]string, 0, len(peers)+1)
	ids = append(ids, n.id)
	byID := make(map[string]discovery.Peer, len(peers))
	for _, p := range peers {
		ids = append(ids, p.ID)
		byID[p.ID] = p
	}
	ring := hashing.New(ids, 32)
	orderedIDs := replication.Planner{Replicas: len(ids)}.Peers(ring, hash, ids)
	seen := make(map[string]bool)
	for _, id := range orderedIDs {
		if id == n.id || seen[id] {
			continue
		}
		seen[id] = true
		if p, ok := byID[id]; ok {
			if err := n.fetchPeer(p.Address, hash, out); err == nil {
				return nil
			}
		}
	}
	return fmt.Errorf("artifact %s not found on available peers", hash)
}

func (n *Node) Serve(addr string) error {
	n.Discovery.TCPAddress = addr
	if err := n.Start(); err != nil {
		return err
	}
	select {}
}

func (n *Node) fetchPeer(address, hash, out string) error {
	r, err := transport.FetchWithTimeout(address, hash, 5*time.Second)
	if err != nil {
		return err
	}
	defer r.Close()

	tmp, err := os.CreateTemp(filepath.Dir(n.Cache.Storage.Root), ".download-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	h := sha256.New()
	_, copyErr := io.Copy(io.MultiWriter(tmp, h), r)
	closeErr := tmp.Close()
	if copyErr != nil {
		return fmt.Errorf("receive artifact: %w", copyErr)
	}
	if closeErr != nil {
		return closeErr
	}
	if hex.EncodeToString(h.Sum(nil)) != hash {
		return fmt.Errorf("peer returned corrupted artifact")
	}
	input, err := os.Open(tmpName)
	if err != nil {
		return err
	}
	putErr := n.Cache.Put(hash, input)
	closeErr = input.Close()
	if putErr != nil {
		return putErr
	}
	if closeErr != nil {
		return closeErr
	}

	input, err = n.Cache.Open(hash)
	if err != nil {
		return err
	}
	defer input.Close()
	output, err := os.Create(out)
	if err != nil {
		return err
	}
	_, copyErr = io.Copy(output, input)
	closeErr = output.Close()
	if copyErr != nil {
		return copyErr
	}
	return closeErr
}

func reachableAddress(address string) string {
	host, port, err := net.SplitHostPort(address)
	if err != nil || (host != "" && host != "::") {
		return address
	}
	local := "127.0.0.1"
	if c, err := net.Dial("udp", "8.8.8.8:80"); err == nil {
		local = c.LocalAddr().(*net.UDPAddr).IP.String()
		_ = c.Close()
	}
	return net.JoinHostPort(local, port)
}

func verifyReader(expected string, r io.Reader) error {
	h := sha256.New()
	if _, err := io.Copy(h, r); err != nil {
		return err
	}
	if hex.EncodeToString(h.Sum(nil)) != expected {
		return fmt.Errorf("local artifact failed SHA-256 verification")
	}
	return nil
}
