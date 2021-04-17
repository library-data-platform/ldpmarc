package reader

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/library-data-platform/ldpmarc/cmd/ldpmarc/srs"
)

type Reader struct {
	pos       int
	records   []srs.Marc
	id        string
	rows      *sql.Rows
	readCount int64
	total     int64
	verbose   bool
}

func NewReader(txin *sql.Tx, tablein string, verbose bool) (*Reader, int64, error) {
	var err error
	// Read number of input records
	var total int64
	if total, err = selectCount(txin, tablein); err != nil {
		return nil, 0, err
	}
	// Set up Reader
	var r = &Reader{}
	r.total = total
	var q = "SELECT id, data FROM " + tablein + " ORDER BY id;"
	if r.rows, err = txin.QueryContext(context.TODO(), q); err != nil {
		return nil, 0, err
	}
	r.records = []srs.Marc{}
	r.verbose = verbose
	return r, total, nil
}

func (r *Reader) Close() {
	r.rows.Close()
}

func (r *Reader) Next(printerr func(string, ...interface{})) (bool, error) {
	var err error
	for {
		if r.pos < len(r.records) {
			return true, nil
		}
		if !r.rows.Next() {
			err = r.rows.Err()
			if err != nil {
				return false, err
			}
			printerr("processing: 100%%")
			return false, nil
		}
		var idN, dataN sql.NullString
		if err = r.rows.Scan(&idN, &dataN); err != nil {
			return false, err
		}
		err = r.rows.Err()
		if err != nil {
			return false, err
		}
		if !idN.Valid {
			printerr(skipValue(idN, dataN))
			continue
		}
		var id string = idN.String
		if r.verbose {
			printerr("verbose: read id=%s", id)
		}
		if strings.TrimSpace(id) == "" {
			printerr(skipValue(idN, dataN))
			continue
		}
		if !dataN.Valid {
			printerr(skipValue(idN, dataN))
			continue
		}
		var data string = dataN.String
		if strings.TrimSpace(data) == "" {
			printerr(skipValue(idN, dataN))
			continue
		}
		if r.records, err = srs.Transform(data); err != nil {
			printerr(skipError(idN, err))
			continue
		}
		r.pos = 0
		r.id = id
		r.readCount++
		if r.readCount%1000000 == 0 {
			var progress = int(float64(r.readCount) / float64(r.total) * 100)
			if progress > 0 {
				printerr("processing: %d%%", progress)
			}
		}
	}
}

func skipValue(idN, dataN sql.NullString) string {
	return fmt.Sprintf("skipping record: %s", idData(idN, dataN))
}

func skipError(idN sql.NullString, err error) string {
	return fmt.Sprintf("skipping record: %s: %s", nullString(idN), err)
}

func idData(idN, dataN sql.NullString) string {
	return fmt.Sprintf("id=%s data=%s", nullString(idN), nullString(dataN))
}

func nullString(s sql.NullString) string {
	if s.Valid {
		return s.String
	} else {
		return "(null)"
	}
}

func (r *Reader) Values() (string, *srs.Marc) {
	var m srs.Marc = r.records[r.pos]
	r.pos++
	return r.id, &srs.Marc{
		BibID:   m.BibID,
		Tag:     m.Tag,
		Ind1:    m.Ind1,
		Ind2:    m.Ind2,
		Ord:     m.Ord,
		SF:      m.SF,
		Content: m.Content,
	}
}

func (r *Reader) ReadCount() int64 {
	return r.readCount
}

func selectCount(txin *sql.Tx, tablein string) (int64, error) {
	var err error
	var count int64
	var q = "SELECT count(*) FROM " + tablein + ";"
	if err = txin.QueryRowContext(context.TODO(), q).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}
