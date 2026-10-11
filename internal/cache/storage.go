// On-disk object store keyed by content hash.
package cache

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type Storage struct {
	Root string
}

func NewStorage(root string) (*Storage, error) {
	if err := os.MkdirAll(filepath.Join(root, "objects"), 0755); err != nil {
		return nil, err
	}

	return &Storage{Root: root}, nil
}

func (s *Storage) Path(hash string) string {
	if len(hash) < 2 {
		return ""
	}
	return filepath.Join(s.Root, "objects", hash[:2], hash[2:])
}

func (s *Storage) Has(hash string) bool {
	p := s.Path(hash)
	if p == "" {
		return false
	}
	_, err := os.Stat(p)
	return err == nil
}

func (s *Storage) Put(hash string, r io.Reader) error {
	if len(hash) < 2 {
		return fmt.Errorf("invalid hash")
	}

	p := s.Path(hash)
	if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
		return err
	}

	// Write to a temp file first, then rename so readers never
	// see a partially written object.
	f, err := os.CreateTemp(filepath.Dir(p), "."+filepath.Base(p)+".part-*")
	if err != nil {
		return err
	}
	tmp := f.Name()

	if _, err = io.Copy(f, r); err == nil {
		err = f.Close()
	} else {
		f.Close()
	}

	if err != nil {
		os.Remove(tmp)
		return err
	}

	return os.Rename(tmp, p)
}

func (s *Storage) Open(hash string) (*os.File, error) {
	p := s.Path(hash)
	if p == "" {
		return nil, fmt.Errorf("invalid hash")
	}
	return os.Open(p)
}

func (s *Storage) Remove(hash string) error {
	return os.Remove(s.Path(hash))
}
