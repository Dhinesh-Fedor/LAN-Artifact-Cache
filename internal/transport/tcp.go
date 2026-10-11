// TCP request/response transfer for artifact fetches.
package transport

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strings"
	"time"
)

type Server struct {
	Addr  string
	Fetch func(string) (io.ReadCloser, error)
	ln    net.Listener
}

func (s *Server) Start() error {
	l, err := net.Listen("tcp", s.Addr)
	if err != nil {
		return err
	}
	s.ln = l
	s.Addr = l.Addr().String()
	go s.serve(l)
	return nil
}

func (s *Server) serve(l net.Listener) {
	for {
		c, err := l.Accept()
		if err != nil {
			return
		}
		go s.handle(c)
	}
}

func (s *Server) ListenAndServe() error {
	if err := s.Start(); err != nil {
		return err
	}
	select {}
}

func (s *Server) Close() error {
	if s.ln == nil {
		return nil
	}
	return s.ln.Close()
}

func (s *Server) handle(c net.Conn) {
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(10 * time.Second))

	line, err := bufio.NewReader(c).ReadString('\n')
	if err != nil {
		return
	}

	var req Request
	if err := json.Unmarshal([]byte(line), &req); err != nil {
		s.writeError(c, "malformed request")
		return
	}
	if strings.TrimSpace(req.Hash) == "" {
		s.writeError(c, "missing artifact hash")
		return
	}

	r, err := s.Fetch(req.Hash)
	if err != nil {
		s.writeError(c, err.Error())
		return
	}
	defer r.Close()

	// The protocol sends a JSON response header followed by exactly Size bytes.
	// A size is required so clients can reject truncated transfers.
	data, err := io.ReadAll(r)
	if err != nil {
		s.writeError(c, err.Error())
		return
	}
	header, err := EncodeResponse(Response{OK: true, Hash: req.Hash, Size: int64(len(data))})
	if err != nil {
		return
	}
	if _, err = fmt.Fprintf(c, "%s\n", header); err != nil {
		return
	}
	_, _ = io.Copy(c, bytes.NewReader(data))
}

func Fetch(addr, hash string) (io.ReadCloser, error) {
	return FetchWithTimeout(addr, hash, 5*time.Second)
}

func FetchWithTimeout(addr, hash string, timeout time.Duration) (io.ReadCloser, error) {
	c, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, err
	}
	_ = c.SetDeadline(time.Now().Add(timeout))

	b, err := EncodeRequest(Request{Hash: hash})
	if err != nil {
		c.Close()
		return nil, err
	}
	if _, err = c.Write(append(b, '\n')); err != nil {
		c.Close()
		return nil, err
	}

	br := bufio.NewReader(c)
	line, err := br.ReadString('\n')
	if err != nil {
		c.Close()
		return nil, err
	}
	var response Response
	if err := json.Unmarshal([]byte(line), &response); err != nil {
		c.Close()
		return nil, fmt.Errorf("malformed response: %w", err)
	}
	if !response.OK {
		c.Close()
		return nil, fmt.Errorf("peer: %s", response.Error)
	}
	if response.Hash != hash || response.Size < 0 {
		c.Close()
		return nil, fmt.Errorf("invalid response metadata")
	}
	return &limitedConn{Reader: io.LimitReader(br, response.Size), conn: c, remaining: response.Size}, nil
}

type limitedConn struct {
	io.Reader
	conn      net.Conn
	remaining int64
}

func (r *limitedConn) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	r.remaining -= int64(n)
	if r.remaining == 0 && err == nil {
		return n, io.EOF
	}
	return n, err
}

func (r *limitedConn) Close() error { return r.conn.Close() }

func (s *Server) writeError(c net.Conn, message string) {
	header, err := EncodeResponse(Response{Error: message})
	if err == nil {
		_, _ = fmt.Fprintf(c, "%s\n", header)
	}
}
