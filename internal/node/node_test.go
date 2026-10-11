package node

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/Fedoraaa/lan-artifact-cache/internal/discovery"
)

func TestCrossNodeRetrievalCachesVerifiedArtifact(t *testing.T) {
	udp := freeUDPPort(t)
	a, err := New(t.TempDir(), "node-a", "127.0.0.1:0", "255.255.255.255:"+udp)
	if err != nil {
		t.Fatal(err)
	}
	b, err := New(t.TempDir(), "node-b", "127.0.0.1:0", "255.255.255.255:"+udp)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Start(); err != nil {
		t.Fatal(err)
	}
	defer a.Stop()
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	defer b.Stop()

	data := []byte("artifact shared over the LAN")
	sum := sha256.Sum256(data)
	hash := hex.EncodeToString(sum[:])
	if err := a.Cache.Put(hash, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	if !waitForPeer(b, "node-a") {
		t.Fatal("node B did not discover node A")
	}

	first := filepath.Join(t.TempDir(), "first")
	if err := b.Get(hash, first); err != nil {
		t.Fatal(err)
	}
	if got, err := readFile(first); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("first retrieval mismatch: %v", err)
	}
	second := filepath.Join(t.TempDir(), "second")
	if err := b.Get(hash, second); err != nil {
		t.Fatal(err)
	}
	if got, err := readFile(second); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("cached retrieval mismatch: %v", err)
	}
}

func TestUnknownArtifactReturnsError(t *testing.T) {
	n, err := New(t.TempDir(), "node", "127.0.0.1:0", "255.255.255.255:"+freeUDPPort(t))
	if err != nil {
		t.Fatal(err)
	}
	defer n.Stop()
	sum := sha256.Sum256([]byte("unknown"))
	if err := n.Get(hex.EncodeToString(sum[:]), filepath.Join(t.TempDir(), "out")); err == nil {
		t.Fatal("unknown artifact unexpectedly succeeded")
	}
}

func TestConcurrentRetrievalsRemainValid(t *testing.T) {
	udp := freeUDPPort(t)
	a, _ := New(t.TempDir(), "a", "127.0.0.1:0", "255.255.255.255:"+udp)
	b, _ := New(t.TempDir(), "b", "127.0.0.1:0", "255.255.255.255:"+udp)
	if err := a.Start(); err != nil {
		t.Fatal(err)
	}
	defer a.Stop()
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	defer b.Stop()
	data := bytes.Repeat([]byte("concurrent"), 1000)
	sum := sha256.Sum256(data)
	hash := hex.EncodeToString(sum[:])
	if err := a.Cache.Put(hash, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	if !waitForPeer(b, "a") {
		t.Fatal("peer discovery failed")
	}
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		go func(i int) {
			errs <- b.Get(hash, filepath.Join(t.TempDir(), "out-"+strconv.Itoa(i)))
		}(i)
	}
	for i := 0; i < 8; i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
}

func TestCorruptedPeerDataIsRejected(t *testing.T) {
	a, _ := New(t.TempDir(), "a", "127.0.0.1:0", "255.255.255.255:"+freeUDPPort(t))
	b, _ := New(t.TempDir(), "b", "127.0.0.1:0", "255.255.255.255:"+freeUDPPort(t))
	if err := a.Start(); err != nil {
		t.Fatal(err)
	}
	defer a.Stop()
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	defer b.Stop()
	sum := sha256.Sum256([]byte("expected"))
	hash := hex.EncodeToString(sum[:])
	if err := a.Cache.Put(hash, bytes.NewReader([]byte("corrupted"))); err != nil {
		t.Fatal(err)
	}
	b.Discovery.AddPeer(discovery.Peer{ID: "a", Address: a.Discovery.TCPAddress})
	if err := b.Get(hash, filepath.Join(t.TempDir(), "out")); err == nil {
		t.Fatal("corrupted peer data unexpectedly succeeded")
	}
	if b.Cache.Storage.Has(hash) {
		t.Fatal("corrupted data was cached")
	}
}

func TestUnavailablePeerFailsWithoutHanging(t *testing.T) {
	b, _ := New(t.TempDir(), "b", "127.0.0.1:0", "255.255.255.255:"+freeUDPPort(t))
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	defer b.Stop()
	sum := sha256.Sum256([]byte("missing"))
	hash := hex.EncodeToString(sum[:])
	b.Discovery.AddPeer(discovery.Peer{ID: "dead", Address: "127.0.0.1:1"})
	start := time.Now()
	if err := b.Get(hash, filepath.Join(t.TempDir(), "out")); err == nil {
		t.Fatal("unavailable peer unexpectedly succeeded")
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("unavailable peer request hung")
	}
}

func waitForPeer(n *Node, id string) bool {
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		for _, p := range n.Discovery.Peers() {
			if p.ID == id {
				return true
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}

func freeUDPPort(t *testing.T) string {
	t.Helper()
	c, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4zero, Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	port := c.LocalAddr().(*net.UDPAddr).Port
	_ = c.Close()
	return strconv.Itoa(port)
}

func readFile(path string) ([]byte, error) {
	return os.ReadFile(path)
}
