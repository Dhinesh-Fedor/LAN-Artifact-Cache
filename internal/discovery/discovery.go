// UDP peer discovery and membership tracking.
package discovery

import (
	"encoding/json"
	"log"
	"net"
	"sync"
	"syscall"
	"time"
)

type message struct {
	ID, Address string
}

const peerTTL = 6 * time.Second

type peerEntry struct {
	Peer
	lastSeen time.Time
}

type Service struct {
	ID, TCPAddress, UDPAddress string
	mu                         sync.RWMutex
	peers                      map[string]peerEntry
	stop                       chan struct{}
	conn                       *net.UDPConn
	done                       chan struct{}
	started                    bool
	startOnce                  sync.Once
	stopOnce                   sync.Once
}

func New(id, tcp, udp string) *Service {
	return &Service{
		ID:         id,
		TCPAddress: tcp,
		UDPAddress: udp,
		peers:      map[string]peerEntry{},
		stop:       make(chan struct{}),
		done:       make(chan struct{}),
	}
}

func (s *Service) Start() error {
	var startErr error
	s.startOnce.Do(func() {
		startErr = s.start()
	})
	return startErr
}

func (s *Service) start() error {
	a, err := net.ResolveUDPAddr("udp4", s.UDPAddress)
	if err != nil {
		return err
	}

	lc := net.ListenConfig{Control: func(_, _ string, c syscall.RawConn) error {
		var controlErr error
		if err := c.Control(func(fd uintptr) {
			if err := syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1); err != nil {
				controlErr = err
				return
			}
			controlErr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_BROADCAST, 1)
		}); err != nil {
			return err
		}
		return controlErr
	}}
	c, err := lc.ListenPacket(nil, "udp4", a.String())
	if err != nil {
		return err
	}
	u, ok := c.(*net.UDPConn)
	if !ok {
		c.Close()
		return net.UnknownNetworkError("discovery listener is not UDP")
	}
	s.mu.Lock()
	s.conn = u
	s.started = true
	s.mu.Unlock()

	go s.read(u)
	go s.announce()
	return nil
}

func (s *Service) read(c *net.UDPConn) {
	defer close(s.done)
	buf := make([]byte, 1024)

	for {
		n, _, err := c.ReadFromUDP(buf)
		if err != nil {
			return
		}

		var m message
		if json.Unmarshal(buf[:n], &m) == nil && m.ID != "" && m.ID != s.ID && m.Address != "" {
			s.mu.Lock()
			s.peers[m.ID] = peerEntry{Peer: Peer{ID: m.ID, Address: m.Address}, lastSeen: time.Now()}
			s.mu.Unlock()
		}
	}
}

func (s *Service) announce() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	s.send()

	for {
		select {
		case <-ticker.C:
			s.send()
			s.expire()
		case <-s.stop:
			return
		}
	}
}

func (s *Service) send() {
	s.mu.RLock()
	c := s.conn
	udpAddress := s.UDPAddress
	tcpAddress := s.TCPAddress
	s.mu.RUnlock()
	if c == nil {
		return
	}

	b, err := json.Marshal(message{ID: s.ID, Address: tcpAddress})
	if err != nil {
		log.Printf("discovery: encode announcement: %v", err)
		return
	}

	_, port, err := net.SplitHostPort(udpAddress)
	if err != nil {
		log.Printf("discovery: invalid UDP address %q: %v", udpAddress, err)
		return
	}

	// Announce to the local IPv4 broadcast domain rather than to the
	// wildcard listen address (for example, :9002), which is not a
	// valid broadcast destination.
	target, err := net.ResolveUDPAddr("udp4", net.JoinHostPort("255.255.255.255", port))
	if err != nil {
		log.Printf("discovery: resolve broadcast address: %v", err)
		return
	}
	if _, err := c.WriteToUDP(b, target); err != nil {
		log.Printf("discovery: send announcement to %s: %v", target, err)
	}
}
func (s *Service) expire() {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, p := range s.peers {
		if now.Sub(p.lastSeen) > peerTTL {
			delete(s.peers, id)
		}
	}
}

func (s *Service) Peers() []Peer {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]Peer, 0, len(s.peers))
	for _, p := range s.peers {
		out = append(out, p.Peer)
	}

	return out
}

// AddPeer adds a peer learned out of band, which is useful for callers that
// already have a trusted endpoint and for deterministic integration tests.
func (s *Service) AddPeer(p Peer) {
	if p.ID == "" || p.Address == "" || p.ID == s.ID {
		return
	}
	s.mu.Lock()
	s.peers[p.ID] = peerEntry{Peer: p, lastSeen: time.Now()}
	s.mu.Unlock()
}

func (s *Service) Stop() error {
	s.mu.RLock()
	started := s.started
	s.mu.RUnlock()
	if !started {
		return nil
	}
	s.stopOnce.Do(func() {
		close(s.stop)
		s.mu.Lock()
		if s.conn != nil {
			_ = s.conn.Close()
		}
		s.mu.Unlock()
		<-s.done
	})
	return nil
}
