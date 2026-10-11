package transport

import (
	"io"
	"net"
	"testing"
	"time"
)

func TestFetchRejectsMalformedResponse(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		c, _ := ln.Accept()
		if c != nil {
			_, _ = c.Write([]byte("not-json\n"))
			c.Close()
		}
	}()
	if _, err := FetchWithTimeout(ln.Addr().String(), "abc", time.Second); err == nil {
		t.Fatal("malformed response unexpectedly succeeded")
	}
}

func TestFetchExposesTruncatedResponseForVerification(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		c, _ := ln.Accept()
		if c != nil {
			_, _ = c.Write([]byte(`{"ok":true,"hash":"abc","size":10}` + "\nshort"))
			c.Close()
		}
	}()
	r, err := FetchWithTimeout(ln.Addr().String(), "abc", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "short" {
		t.Fatalf("got %q", data)
	}
}
