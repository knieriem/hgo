// Copyright 2013 The hgo Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package revlog

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"github.com/knieriem/hgo/revlog/patch"
)

func (fb *FileBuilder) swap() {
	fb.w, fb.w1 = fb.w1, fb.w
}

type DataCache interface {
	Get(int) []byte
	Store(int, []byte)
}

type noCache struct{}

func (noCache) Get(int) (data []byte) { return }
func (noCache) Store(int, []byte)     {}

type FileBuilder struct {
	w, w1 *patch.Joiner

	dataCache DataCache
	data      dataHelper
	fileBuf   bytes.Buffer
}

func NewFileBuilder() (p *FileBuilder) {
	p = new(FileBuilder)
	p.w = patch.NewJoiner(600)
	p.w1 = patch.NewJoiner(600)
	p.data.tmp = bytes.NewBuffer(make([]byte, 0, 128))
	return
}

type dataHelper struct {
	file     DataReadCloser
	tmp      *bytes.Buffer
	keepOpen bool
}

func (dh *dataHelper) Open(fileName string) (file DataReadCloser, err error) {
	if dh.file != nil {
		file = dh.file
		return
	}
	file, err = os.Open(fileName)
	if err == nil {
		dh.file = file
	}
	return
}

func (dh *dataHelper) TmpBuffer() *bytes.Buffer {
	return dh.tmp
}

func (p *FileBuilder) SetDataCache(dc DataCache) {
	p.dataCache = dc
}
func (p *FileBuilder) Bytes() []byte {
	return p.fileBuf.Bytes()
}

func (p *FileBuilder) KeepDataOpen() {
	p.data.keepOpen = true
}
func (p *FileBuilder) CloseData() (err error) {
	if p.data.file != nil {
		err = p.data.file.Close()
		p.data.file = nil
	}
	return
}

func (p *FileBuilder) PreparePatch(r *Rec) (f *FilePatch, err error) {
	var prevPatch []patch.Hunk
	rsav := r
	dc := p.dataCache
	if dc == nil {
		dc = noCache{}
	}

	if !p.data.keepOpen {
		defer p.CloseData()
	}

	for {
		d := dc.Get(r.i)
		if d == nil {
			d, err = r.GetData(&p.data)
			if err != nil {
				err = fmt.Errorf("rev %d: get data: %v", r.i, err)
				return
			}
			dc.Store(r.i, d)
		}
		if r.IsBase() {
			skip := 0
			if r == rsav && r.IsStartOfBranch() && len(d) > 2 {
				if d[0] == '\001' && d[1] == '\n' {
					if i := bytes.Index(d[2:], []byte{'\001', '\n'}); i != -1 {
						skip = i + 4
					}
				}
			}
			f = new(FilePatch)
			f.baseData = d
			f.MetaLen = skip
			f.patch = prevPatch
			f.rev = rsav
			f.fb = p
			return
		}
		hunks, err1 := patch.Parse(d)
		if err1 != nil {
			err = err1
			return
		}
		if prevPatch == nil {
			prevPatch = hunks
		} else {
			prevPatch = p.w.JoinPatches(hunks, prevPatch)
			p.swap()
		}
		r = r.Prev()
	}
	panic("not reached")

}

func (p *FileBuilder) BuildWrite(w io.Writer, r *Rec) (err error) {
	fp, err := p.PreparePatch(r)
	if err == nil {
		err = fp.Apply(w)
	}
	return
}
func (p *FileBuilder) Build(r *Rec) (file []byte, err error) {
	fp, err := p.PreparePatch(r)
	if err == nil {
		err = fp.Apply(nil)
		if err == nil {
			file = p.Bytes()
		}
	}
	return
}

type FilePatch struct {
	fb       *FileBuilder
	rev      *Rec
	baseData []byte
	patch    []patch.Hunk

	MetaData map[string]string
	MetaLen  int
}

func (p *FilePatch) Apply(w io.Writer) (err error) {
	if w == nil {
		p.fb.fileBuf.Reset()
		w = &p.fb.fileBuf
	}

	r := p.rev

	h := r.Index.NewHash()
	for _, id := range sortedPair(r.Parent().Id(), r.Parent2().Id()) {
		h.Write([]byte(id))
	}

	orig := p.baseData
	n := 0
	if p.MetaLen > 0 {
		n, _ = h.Write(orig[:p.MetaLen])
	}
	n, err = patch.Apply(io.MultiWriter(h, w), orig, n, p.patch)
	if err != nil {
		return
	}

	if n != int(r.FileLength) {
		err = fmt.Errorf("revlog: length of computed file differs from the expected value: %d != %d", n, r.FileLength)
	} else {
		fileId := NodeId(h.Sum(nil))
		if !fileId.Eq(r.Id()) {
			err = fmt.Errorf("revlog: hash mismatch: internal error or corrupted data")
		}
	}
	return
}
