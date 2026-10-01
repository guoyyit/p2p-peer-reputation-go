package store

import (
	"path/filepath"
	"testing"
)

func TestSaveLoadRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "state.json")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	data, err := s.Load()
	if err != nil {
		t.Fatal(err)
	}
	if data != nil {
		t.Fatal("new store must load nil")
	}
	payload := []byte(`{"version":1}`)
	if err := s.Save(payload); err != nil {
		t.Fatal(err)
	}
	got, err := s.Load()
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(payload) {
		t.Fatalf("load = %q, want %q", got, payload)
	}
}

func TestSaveOverwritesAtomically(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Save([]byte("first")); err != nil {
		t.Fatal(err)
	}
	if err := s.Save([]byte("second-longer")); err != nil {
		t.Fatal(err)
	}
	got, _ := s.Load()
	if string(got) != "second-longer" {
		t.Fatalf("load = %q", got)
	}
}
