package local

import (
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/jackc/pgtype"
	"github.com/library-data-platform/ldpmarc/cmd/ldpmarc/util"
)

type Record struct {
	SRSID        string
	Line         int64
	MatchedID    string
	InstanceHRID string
	InstanceID   string
	Field        string
	Ind1         string
	Ind2         string
	Ord          int64
	SF           string
	Content      string
}

type Store struct {
	bins        map[string]*bin
	basepath    string
	doneWriting bool
}

type bin struct {
	encoder *gob.Encoder
	file    *os.File
	path    string
}

func NewStore(datadir string) (*Store, error) {
	var err error
	var bins = make(map[string]*bin)
	var allFields = util.GetAllFieldNames()
	var basepath = filepath.Join(datadir, "tmp/ldpmarc")
	_ = os.RemoveAll(basepath)
	if err = os.MkdirAll(basepath, 0700); err != nil {
		return nil, fmt.Errorf("unable to make directory: %v: %v", basepath, err)
	}
	var f string
	for _, f = range allFields {
		var path = filepath.Join(basepath, f)
		var file *os.File
		if file, err = os.Create(path); err != nil {
			if strings.HasSuffix(err.Error(), ": too many open files") {
				err = fmt.Errorf("%v: setting \"ulimit -n 1024\" may help", err)
			}
			return nil, fmt.Errorf("unable to create file: %v: %v", path, err)
		}
		bins[f] = &bin{
			encoder: gob.NewEncoder(file),
			file:    file,
			path:    path,
		}
	}
	return &Store{
		bins:     bins,
		basepath: basepath,
	}, nil
}

func (s *Store) Write(record *Record) (*string, error) {
	var ok bool
	var b *bin
	if b, ok = s.bins[record.Field]; !ok {
		var msg = fmt.Sprintf("unknown field: %s", record.Field)
		return &msg, nil
	}
	var err error
	if err = b.encoder.Encode(*record); err != nil {
		return nil, fmt.Errorf("encoding record: %v: %v", err, *record)
	}
	return nil, nil
}

func (s *Store) FinishWriting() error {
	var err error
	if s.doneWriting {
		return fmt.Errorf("write mode already completed")
	}
	s.doneWriting = true
	var f string
	var b *bin
	for f, b = range s.bins {
		if err = b.file.Close(); err != nil {
			return fmt.Errorf("closing file: %v: %v", b.path, err)
		}
		s.bins[f] = &bin{
			path: b.path,
		}
	}
	return nil
}

func (s *Store) Close() {
	var b *bin
	for _, b = range s.bins {
		if b.file != nil {
			_ = b.file.Close()
		}
		_ = os.Remove(b.path)
	}
	_ = os.RemoveAll(s.basepath)
}

type Source struct {
	err     error
	record  *Record
	decoder *gob.Decoder
	file    *os.File
	path    string
}

func (s *Store) ReadSource(field string) (*Source, error) {
	if !s.doneWriting {
		return nil, fmt.Errorf("source cannot be created in write mode")
	}
	var ok bool
	var b *bin
	if b, ok = s.bins[field]; !ok {
		return nil, fmt.Errorf("field not found: %v", field)
	}
	var err error
	var file *os.File
	if file, err = os.Open(b.path); err != nil {
		return nil, fmt.Errorf("unable to open file for reading: %v: %v", b.path, err)
	}
	return &Source{
		decoder: gob.NewDecoder(file),
		file:    file,
		path:    b.path,
	}, nil
}

func (s *Source) Close() {
	_ = s.file.Close()
	s.file = nil
	_ = os.Remove(s.path)
}

func (s *Source) Next() bool {
	var err error
	var record = new(Record)
	err = s.decoder.Decode(record)
	switch {
	case err == io.EOF:
		return false
	case err != nil:
		s.err = err
		return false
	default:
		s.record = record
		return true
	}
}

func (s *Source) Values() ([]any, error) {
	var err error
	switch {
	case s.err != nil:
		return nil, s.err
	case s.record == nil:
		s.err = fmt.Errorf("no record available: %s", s.path)
		return nil, s.err
	default:
		var r = s.record
		var srsID, matchedID, instanceID pgtype.UUID
		if srsID, err = encodeUUID(r.SRSID); err != nil {
			return nil, fmt.Errorf("encoding srs_id: %v", err)
		}
		if matchedID, err = encodeUUID(r.SRSID); err != nil {
			return nil, fmt.Errorf("encoding matched_id: %v", err)
		}
		if instanceID, err = encodeUUID(r.SRSID); err != nil {
			return nil, fmt.Errorf("encoding instance_id: %v", err)
		}
		var v = []any{
			srsID,
			r.Line,
			matchedID,
			r.InstanceHRID,
			instanceID,
			r.Field,
			r.Ind1,
			r.Ind2,
			r.Ord,
			r.SF,
			r.Content,
		}
		return v, nil
	}
}

func (s *Source) Err() error {
	return s.err
}

func encodeUUID(uuid string) (pgtype.UUID, error) {
	var err error
	var u pgtype.UUID
	if err = u.Set(uuid); err != nil {
		return pgtype.UUID{}, err
	}
	return u, nil
}
