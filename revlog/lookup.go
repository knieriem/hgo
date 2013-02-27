// Copyright 2013 The hgo Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package revlog

import (
	"errors"
)

type RevisionSpec interface {
	Lookup(*Index) (*Rec, error)
}

type FileRevSpec int

func (n FileRevSpec) Lookup(i *Index) (r *Rec, err error) {
	if n < 0 {
		n += FileRevSpec(len(i.index))
	}
	if n < 0 || int(n) >= len(i.index) {
		err = ErrRevisionNotFound
	} else {
		r = i.Record(int(n))
	}
	return
}

type LinkRevSpec int

func (want LinkRevSpec) Lookup(i *Index) (r *Rec, err error) {
	r = i.Tip()
	for j := range i.index {
		if int(i.index[j].Linkrev) > int(want) {
			if j == 0 {
				r = i.Null()
			} else {
				r = i.Record(j - 1)
			}
			return
		}
	}
	return
}

type NodeIdRevSpec string

func (hash NodeIdRevSpec) Lookup(rv *Index) (r *Rec, err error) {
	var i = -1
	var found bool

	wantid, err := NewId(string(hash))
	if err != nil {
		return
	}
	for j := range rv.index {
		nodeid := rv.NewNodeId(rv.index[j].NodeId[:])
		if len(wantid) <= len(nodeid) {
			if wantid.Eq(nodeid[:len(wantid)]) {
				if found {
					err = ErrRevisionAmbiguous
				}
				found = true
				i = j
			}
		}
	}
	if i == -1 {
		err = ErrRevNotFound
	} else {
		r = rv.Record(i)
	}
	return
}

type TipRevSpec struct{}

func (TipRevSpec) String() string {
	return "tip"
}

func (TipRevSpec) Lookup(i *Index) (r *Rec, err error) {
	if n := len(i.index); n == 0 {
		err = ErrRevisionNotFound
	} else {
		r = i.Record(n - 1)
	}
	return
}

type NullRevSpec struct{}

func (NullRevSpec) String() string {
	return "null"
}

func (NullRevSpec) Lookup(i *Index) (r *Rec, err error) {
	r = &i.null
	return
}

var ErrRevisionNotFound = errors.New("hg/revlog: revision not found")
var ErrRevisionAmbiguous = errors.New("hg/revlog: ambiguous revision spec")
