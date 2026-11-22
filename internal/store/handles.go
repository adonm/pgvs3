package store

import (
	"github.com/adonm/pgvs3/internal/ns"
)

type Cache struct {
	s    *Store
	name string
	sc   string
	err  error
}

func (s *Store) Cache(name string) Cache {
	sc, err := nsSQLIdent(ns.Cache, name)
	return Cache{s: s, name: name, sc: sc, err: err}
}
