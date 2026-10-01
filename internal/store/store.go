// Package store persists governance state to local disk for crash recovery.
package store

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
)

// Store is a file-backed atomic state store.
type Store struct {
	path string
}

// Open opens (or creates) the store at path.
func Open(path string) (*Store, error) {
	if dir := filepath.Dir(path); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	return &Store{path: path}, nil
}

// Save atomically persists data.
func (s *Store) Save(data []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(s.path), ".gov-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, s.path)
}

// Load returns the last persisted bytes, or nil if none exist.
func (s *Store) Load() ([]byte, error) {
	data, err := os.ReadFile(s.path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return data, nil
}

// Close releases underlying resources.
func (s *Store) Close() error { return nil }
