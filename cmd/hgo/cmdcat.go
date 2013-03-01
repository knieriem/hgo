// Copyright 2013 The hgo Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"errors"
	"io"

	"github.com/knieriem/hgo/changelog"
	"github.com/knieriem/hgo/revlog"
	"github.com/knieriem/hgo/store"
)

// Lookup the given revision of a file. The Manifest is consulted only
// if necessary, i.e. if it can''t be told from the filelog whether a file exists yet or not
func LookupFile(fileLog *revlog.Index, chgId int, manifestEntry func() (*store.ManifestEnt, error)) (r *revlog.Rec, err error) {
	r, err = revlog.LinkRevSpec(chgId).Lookup(fileLog)
	if err != nil {
		return
	}

	if int(r.Linkrev) == chgId {
		// The requested revision matches this record, which can be
		// used as a sign that the file is existent yet.
		return
	}

	if !r.IsLeaf() {
		// There are other records that have the current record as a parent.
		// This means, the file was existent, no need to check the manifest.
		return
	}

	// Check for the file's existence using the manifest.
	ent, err := manifestEntry()
	if err != nil {
		return
	}

	// compare hashes
	wantId, err := ent.Id()
	if err != nil {
		return
	}
	if !wantId.Eq(r.Id()) {
		err = errors.New("manifest node id does not match file id")
	}
	return
}

var cmdCat = &Command{
	UsageLine: "cat [-R dir] [-r rev] [file]",
	Short:     "write the current or given revision of a file to stdout",
	Long:      ``,
}

func init() {
	addStdFlags(cmdCat)
	addRevFlag(cmdCat)
	cmdCat.Run = runCat
}

func runCat(cmd *Command, w io.Writer, args []string) {
	openRepository(args)
	rs := getRevisionSpec()
	fileArg := getFileArg(args)
	st := repo.NewStore()

	fileLog, err := st.OpenRevlog(fileArg)
	if err != nil {
		fatalf("%s", err)
	}

	ra := repoAccess{
		fb: revlog.NewFileBuilder(),
		st: st,
	}
	localId, ok := rs.(revlog.FileRevSpec)
	if !ok {
		localId, err = ra.localChangesetId(rs)
		if err != nil {
			return
		}
	}

	r, err := LookupFile(fileLog, int(localId), func() (*store.ManifestEnt, error) {
		return ra.manifestEntry(int(localId), fileArg)
	})
	if err != nil {
		fatalf("%s", err)
	}

	fb := revlog.NewFileBuilder()
	err = fb.BuildWrite(w, r)
	if err != nil {
		fatalf("%s", err)
	}
}

type repoAccess struct {
	fb        *revlog.FileBuilder
	st        *store.Store
	changelog *revlog.Index
}

func (ra *repoAccess) manifestEntry(chgId int, fileName string) (me *store.ManifestEnt, err error) {
	r, err := ra.clRec(revlog.FileRevSpec(chgId))
	if err != nil {
		return
	}
	c, err := changelog.BuildEntry(r, ra.fb)
	if err != nil {
		return
	}
	m, err := getManifest(int(c.Linkrev), c.ManifestNode, ra.fb)
	if err != nil {
		return
	}
	me = m.Map()[fileName]
	if me == nil {
		err = errors.New("file does not exist in given revision")
	}
	return
}

func (ra *repoAccess) localChangesetId(rs revlog.RevisionSpec) (chgId revlog.FileRevSpec, err error) {
	r, err := ra.clRec(rs)
	if err == nil {
		chgId = revlog.FileRevSpec(r.FileRev())
	}
	return
}

func (ra *repoAccess) clRec(rs revlog.RevisionSpec) (r *revlog.Rec, err error) {
	if ra.changelog == nil {
		log, err1 := ra.st.OpenChangeLog()
		if err1 != nil {
			err = err1
			return
		}
		ra.changelog = log
	}
	r, err = rs.Lookup(ra.changelog)
	return
}
