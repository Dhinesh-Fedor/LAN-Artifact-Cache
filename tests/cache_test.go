package tests

import (
	"bytes"
	"testing"

	"github.com/Fedoraaa/lan-artifact-cache/internal/cache"
)

func TestCacheRoundTrip(t *testing.T) {
	s, e := cache.NewStorage(t.TempDir())
	if (e != nil) {
		t.Fatal(e)
	}

	c := cache.New(s)
	if e = c.Put("0123456789abcdef", bytes.NewBufferString("hello")); (e != nil) {
		t.Fatal(e)
	}

	if (!c.Has("0123456789abcdef")) {
		t.Fatal("missing artifact")
	}
}
